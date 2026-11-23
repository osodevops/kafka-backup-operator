#!/usr/bin/env python3
"""Check GCS admission against installed CRDs on an explicitly selected cluster.

Uses server dry runs, except for a suspended backup in a temporary namespace
needed to check updates. Run after installing CRDs, before installing an operator.
"""

import argparse
import copy
import json
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--context", required=True, help="Explicit kubectl context")
    args = parser.parse_args()
    namespace = f"gcs-auth-test-{uuid.uuid4().hex[:8]}"
    passed = 0

    def kubectl(*arguments, resource=None):
        return subprocess.run(
            ["kubectl", "--context", args.context, *arguments],
            input=json.dumps(resource) if resource is not None else None,
            text=True,
            capture_output=True,
            check=False,
        )

    def check(result, accepted, label):
        nonlocal passed
        if (result.returncode == 0) != accepted:
            raise AssertionError(f"{label}: {result.stdout}{result.stderr}")
        if not accepted and "GCS requires either" not in result.stderr:
            raise AssertionError(f"{label}: unexpected rejection: {result.stderr}")
        passed += 1

    choices = [
        ("identity", {"useWorkloadIdentity": True}, True),
        ("legacy-secret", {"credentialsSecret": {"name": "key"}}, True),
        ("explicit-secret", {"useWorkloadIdentity": False, "credentialsSecret": {"name": "key"}}, True),
        ("both", {"useWorkloadIdentity": True, "credentialsSecret": {"name": "key"}}, False),
        ("neither", {}, False),
        ("false-without-secret", {"useWorkloadIdentity": False}, False),
        ("null-secret-identity", {"useWorkloadIdentity": True, "credentialsSecret": None}, True),
        ("null-secret-no-identity", {"credentialsSecret": None}, False),
        ("pvc", None, True),
    ]

    def storage(choice):
        if choice is None:
            return {"storageType": "pvc", "pvc": {"claimName": "data"}}
        return {"storageType": "gcs", "gcs": {"bucket": "backups", **choice}}

    def resource(destination, choice):
        kafka = {"bootstrapServers": ["kafka:9092"]}
        if destination == "backup":
            kind = "KafkaBackup"
            spec = {"kafkaCluster": kafka, "topics": ["orders"], "suspend": True,
                    "storage": storage(choice)}
        elif destination == "restore":
            kind = "KafkaRestore"
            spec = {"kafkaCluster": kafka, "backupRef": {"name": "external", "storage": storage(choice)}}
        else:
            kind = "KafkaBackupValidation"
            spec = {"backupRef": {"name": "external"}, "checks": {}, "suspend": True}
            if destination == "validation-source":
                spec["backupRef"]["storage"] = storage(choice)
            else:
                spec["evidence"] = {"storage": storage(choice)}
        return {"apiVersion": "kafka.oso.sh/v1alpha1", "kind": kind,
                "metadata": {"name": "gcs-auth-check", "namespace": namespace}, "spec": spec}

    created = kubectl("create", "namespace", namespace)
    if created.returncode:
        raise RuntimeError(created.stderr)
    try:
        for destination in ["backup", "restore", "validation-source", "evidence"]:
            for label, choice, accepted in choices:
                check(kubectl("create", "--dry-run=server", "-f", "-", resource=resource(destination, choice)),
                      accepted, f"create {destination}/{label}")
        # Persist one suspended legacy object, then exercise real update admission.
        legacy = kubectl("create", "-f", "-", "-o", "json",
                         resource=resource("backup", {"credentialsSecret": {"name": "key"}}))
        if legacy.returncode:
            raise RuntimeError(legacy.stderr)
        legacy = json.loads(legacy.stdout)
        assert legacy["spec"]["storage"]["gcs"]["useWorkloadIdentity"] is False
        for label, choice, accepted in choices:
            updated = copy.deepcopy(legacy)
            updated["spec"]["storage"] = storage(choice)
            check(kubectl("replace", "--dry-run=server", "-f", "-", resource=updated),
                  accepted, f"update legacy backup/{label}")
        print(f"PASS: {passed} create/update admission checks on {args.context}")
    finally:
        cleanup = kubectl("delete", "namespace", namespace, "--wait=false")
        if cleanup.returncode:
            raise RuntimeError(cleanup.stderr)


if __name__ == "__main__":
    main()
