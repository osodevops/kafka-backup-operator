#!/usr/bin/env python3
"""Exercise a released-to-candidate upgrade on a disposable kind cluster.

Requires an existing kind cluster, a loaded candidate image, and the previous
release image. Uses an explicit kubeconfig/context for every Kubernetes call.
Leaves its namespace and JSON evidence in place for diagnosis; delete the kind
cluster afterwards. Kafka data and all credentials are local test fixtures.
"""

import argparse
import copy
import json
import pathlib
import subprocess
import tarfile
import tempfile
import time


ROOT = pathlib.Path(__file__).resolve().parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubeconfig", required=True)
    parser.add_argument("--context", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--previous-tag", default="v1.9.1")
    parser.add_argument("--kafka-image", default="apache/kafka:4.1.1")
    parser.add_argument("--evidence", type=pathlib.Path, required=True)
    args = parser.parse_args()
    if not args.context.startswith("kind-"):
        parser.error("use a disposable kind context")
    args.evidence.mkdir(parents=True, exist_ok=True)
    namespace = f"kbo-upgrade-{int(time.time())}"
    kafka = {"bootstrapServers": [f"kafka.{namespace}.svc:9092"]}
    records = [f"key-{i:03d}\t{{\"id\":{i},\"value\":\"compatibility-λ\"}}" for i in range(90)]

    def run(command, data=None, accepted=True, timeout=240):
        result = subprocess.run(command, input=data, text=True, capture_output=True, timeout=timeout)
        if (result.returncode == 0) != accepted:
            raise AssertionError(f"{command}: {result.stdout}\n{result.stderr}")
        return result.stdout if accepted else result.stderr

    def k(*command, data=None, accepted=True):
        return run(["kubectl", "--kubeconfig", args.kubeconfig, "--context", args.context,
                    "-n", namespace, *command], data, accepted)

    def apply(resource):
        return k("apply", "-f", "-", data=json.dumps(resource))

    def get(kind, name):
        return json.loads(k("get", kind, name, "-o", "json"))

    def save(name, value):
        (args.evidence / f"{name}.json").write_text(json.dumps(value, indent=2) + "\n")

    def wait(label, predicate, budget=180):
        deadline = time.monotonic() + budget
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                print(f"PASS: {label}", flush=True)
                return value
            time.sleep(2)
        raise AssertionError(f"timed out: {label}")

    def complete(kind, name, phase="Completed"):
        def check():
            resource = get(kind, name)
            status = resource.get("status", {})
            if status.get("observedGeneration") != resource["metadata"]["generation"]:
                return None
            if status.get("phase") == "Failed" and phase != "Failed":
                raise AssertionError(f"{kind}/{name}: {json.dumps(status)}")
            return resource if status.get("phase") == phase else None
        result = wait(f"{kind}/{name} {phase}", check)
        save(f"{kind}-{name}", result)
        return result

    def cr(kind, name, spec):
        resource = {"apiVersion": "kafka.oso.sh/v1alpha1", "kind": kind,
                    "metadata": {"name": name, "namespace": namespace}, "spec": spec}
        apply(resource)
        return resource

    def cli(program, *arguments, data=None):
        return k("exec", "-i", "kafka", "--", f"/opt/kafka/bin/{program}.sh",
                 "--bootstrap-server", "kafka:9092", *arguments, data=data)

    def verify_topic(topic):
        output = cli("kafka-console-consumer", "--topic", topic, "--from-beginning",
                     "--max-messages", str(len(records)), "--timeout-ms", "20000",
                     "--property", "print.key=true", "--property", "key.separator=\t")
        assert sorted(output.splitlines()) == sorted(records), (topic, output)
        # Exact end offsets catch duplicate replay, even if a consumer stops at 90.
        offsets = cli("kafka-get-offsets", "--topic", topic, "--time", "-1")
        assert sum(int(line.rsplit(":", 1)[1]) for line in offsets.splitlines()) == len(records), offsets
        print(f"PASS: {topic} exact keys, values and count ({len(records)} records)", flush=True)

    def resume_backup(name, count, interval):
        start = len(records)
        added = [f"key-{i:03d}\t{{\"id\":{i},\"value\":\"compatibility-λ\"}}"
                 for i in range(start, start + count)]
        cli("kafka-console-producer", "--topic", "source", "--property", "parse.key=true",
            "--property", "key.separator=\t", data="\n".join(added) + "\n")
        records.extend(added)
        verify_topic("source")
        k("patch", "kafkabackup", name, "--type=merge", "-p",
          json.dumps({"spec": {"checkpoint": {"enabled": True, "intervalSecs": interval}}}))
        resumed = complete("kafkabackup", name)
        assert resumed["status"]["recordsProcessed"] == count, resumed["status"]
        assert resumed["status"]["backupId"] == name
        print(f"PASS: {name} resumes its SQLite checkpoint and adds only {count} records", flush=True)

    def group_offsets():
        output = cli("kafka-consumer-groups", "--describe", "--group", "compatibility")
        offsets = {}
        for line in output.splitlines():
            fields = line.split()
            if len(fields) >= 4 and fields[:2] == ["compatibility", "source"]:
                offsets[int(fields[2])] = int(fields[3])
        return offsets

    def leader_count():
        pods = json.loads(k("get", "pods", "-l", "app.kubernetes.io/name=kafka-backup-operator", "-o", "json"))
        leaders = 0
        for pod in pods["items"]:
            if pod["metadata"].get("deletionTimestamp"):
                continue
            if not any(c["type"] == "Ready" and c["status"] == "True"
                       for c in pod.get("status", {}).get("conditions", [])):
                continue
            name = pod["metadata"]["name"]
            metrics = k("get", "--raw", f"/api/v1/namespaces/{namespace}/pods/{name}:8080/proxy/metrics")
            leaders += sum(line.startswith("kafka_backup_operator_leader{") and line.endswith(" 1")
                           for line in metrics.splitlines())
        return leaders

    def install(chart, image):
        repository, tag = image.rsplit(":", 1)
        values = {"fullnameOverride": namespace,
                  "image": {"repository": repository, "tag": tag, "pullPolicy": "Never"},
                  "resources": {"limits": {"cpu": "2", "memory": "1Gi"}},
                  "extraVolumes": [{"name": "data", "persistentVolumeClaim": {"claimName": "archives"}}],
                  "extraVolumeMounts": [{"name": "data", "mountPath": "/data/archives"}]}
        values_file = args.evidence / "helm-values.json"
        values_file.write_text(json.dumps(values))
        run(["helm", "--kubeconfig", args.kubeconfig, "--kube-context", args.context,
             "upgrade", "--install", namespace, str(chart), "-n", namespace,
             "--skip-crds", "-f", str(values_file), "--wait", "--timeout", "3m"])
        def leads():
            holder = get("lease", f"{namespace}-leader")["spec"].get("holderIdentity")
            pods = json.loads(k("get", "pods", "-o", "json"))
            return any(p["metadata"]["name"] == holder and not p["metadata"].get("deletionTimestamp")
                       and p["spec"]["containers"][0]["image"] == image for p in pods["items"])
        wait(f"{image} holds the leader Lease", leads)
        print(f"PASS: install {image}", flush=True)

    def restore(name, backup, topic):
        cr("KafkaRestore", name, {"backupRef": {"name": backup}, "kafkaCluster": kafka,
                                 "topics": ["source"],
                                 "topicMapping": {"source": topic}, "createTopics": True,
                                 "defaultReplicationFactor": 1, "partitionMapping": {"0": 0}})
        result = complete("kafkarestore", name)
        assert result["status"]["recordsRestored"] == len(records), result
        verify_topic(topic)
        return result

    try:
        operators = json.loads(run(["kubectl", "--kubeconfig", args.kubeconfig,
                                    "--context", args.context, "get", "deployments", "-A",
                                    "-l", "app.kubernetes.io/name=kafka-backup-operator", "-o", "json"]))
        if operators["items"]:
            raise AssertionError("use a clean disposable cluster: existing operators watch all namespaces")
        run(["kubectl", "--kubeconfig", args.kubeconfig, "--context", args.context,
             "create", "namespace", namespace])
        apply({"apiVersion": "v1", "kind": "Service", "metadata": {"name": "kafka"},
               "spec": {"selector": {"app": "kafka"}, "ports": [{"port": 9092}]}})
        env = {"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
               "KAFKA_LISTENERS": "PLAINTEXT://:9092,CONTROLLER://:9093",
               "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://kafka.{namespace}.svc:9092",
               "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
               "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
               "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
               "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
               "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
               "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
               "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS": "0"}
        apply({"apiVersion": "v1", "kind": "Pod", "metadata": {"name": "kafka", "labels": {"app": "kafka"}},
               "spec": {"containers": [{"name": "kafka", "image": args.kafka_image,
                                         "imagePullPolicy": "Never",
                                         "env": [{"name": n, "value": v} for n, v in env.items()],
                                         "readinessProbe": {"tcpSocket": {"port": 9092},
                                                            "periodSeconds": 2}}]}})
        apply({"apiVersion": "v1", "kind": "PersistentVolumeClaim", "metadata": {"name": "archives"},
               "spec": {"accessModes": ["ReadWriteOnce"], "resources": {"requests": {"storage": "1Gi"}}}})
        k("wait", "pod/kafka", "--for=condition=Ready", "--timeout=180s")
        cli("kafka-topics", "--create", "--topic", "source", "--partitions", "3", "--replication-factor", "1")
        cli("kafka-topics", "--create", "--topic", "empty", "--partitions", "1", "--replication-factor", "1")
        cli("kafka-console-producer", "--topic", "source", "--property", "parse.key=true",
            "--property", "key.separator=\t", data="\n".join(records) + "\n")
        verify_topic("source")
        with tempfile.TemporaryDirectory(prefix="kbo-previous-") as tmp:
            archive = subprocess.check_output(["git", "-C", str(ROOT), "archive", args.previous_tag,
                                               "deploy/helm/kafka-backup-operator", "deploy/crds/all.yaml"])
            archive_file = pathlib.Path(tmp) / "previous.tar"
            archive_file.write_bytes(archive)
            with tarfile.open(archive_file) as previous:
                # git archive contains only the two explicitly selected repository paths.
                previous.extractall(tmp, filter="data")
            old_chart = pathlib.Path(tmp) / "deploy/helm/kafka-backup-operator"
            # Establish the previous schema with Helm ownership, including when
            # the CI smoke install has already created the candidate CRDs.
            k("apply", "--server-side", "--field-manager=helm", "--force-conflicts",
              "-f", str(pathlib.Path(tmp) / "deploy/crds/all.yaml"))
            previous_image = f"ghcr.io/osodevops/kafka-backup-operator:{args.previous_tag.removeprefix('v')}"
            install(old_chart, previous_image)
            backup_spec = {"kafkaCluster": kafka, "topics": ["source", "empty"],
                           "storage": {"storageType": "pvc", "pvc": {"claimName": "archives"}},
                           "stopAtCurrentOffsets": True, "checkpoint": {"enabled": True, "intervalSecs": 5}}
            cr("KafkaBackup", "legacy", backup_spec)
            assert complete("kafkabackup", "legacy")["status"]["recordsProcessed"] == len(records)
            restore("legacy-restore", "legacy", "baseline-restored")
            validation_spec = {"backupRef": {"name": "legacy"}, "kafkaCluster": kafka,
                               "checks": {"messageCount": {"enabled": True}, "offsetRange": {"enabled": True}},
                               "evidence": {"formats": ["json"]}}
            cr("KafkaBackupValidation", "legacy-validation", validation_spec)
            validation = complete("kafkabackupvalidation", "legacy-validation")
            assert validation["status"]["validationResult"] == "Pass", validation["status"]
            assert any(r.get("data", {}).get("topic_counts") for r in validation["status"]["checkResults"]), validation
            cli("kafka-consumer-groups", "--group", "compatibility", "--topic", "source",
                "--reset-offsets", "--to-offset", "10", "--execute")
            assert group_offsets() == {0: 10, 1: 10, 2: 10}
            snapshot = {"snapshot_id": "compatibility", "created_at": "2026-11-20T00:00:00Z",
                        "bootstrap_servers": kafka["bootstrapServers"],
                        "group_offsets": {"compatibility": {"group_id": "compatibility", "partition_count": 3,
                                         "offsets": {"source": {str(p): {"offset": 10, "metadata": None,
                                                                        "timestamp": None} for p in range(3)}}}}}
            holder = get("lease", f"{namespace}-leader")["spec"]["holderIdentity"]
            k("exec", "-i", holder, "--", "sh", "-c", "cat > /data/archives/offset-snapshot.json",
              data=json.dumps(snapshot))
            reset_spec = {"kafkaCluster": kafka, "consumerGroups": ["compatibility"],
                          "topics": ["source"], "resetStrategy": "to-earliest", "snapshotBeforeReset": False}
            rollback_spec = {"kafkaCluster": kafka, "snapshotRef": {"name": "compatibility",
                             "path": "/data/archives/offset-snapshot.json"}, "verifyAfterRollback": True}
            cr("KafkaOffsetReset", "legacy-reset", reset_spec)
            complete("kafkaoffsetreset", "legacy-reset")
            assert group_offsets() == {0: 0, 1: 0, 2: 0}
            cr("KafkaOffsetRollback", "legacy-rollback", rollback_spec)
            complete("kafkaoffsetrollback", "legacy-rollback")
            assert group_offsets() == {0: 10, 1: 10, 2: 10}
            stored = [("kafkabackup", "legacy"), ("kafkarestore", "legacy-restore"),
                      ("kafkabackupvalidation", "legacy-validation"), ("kafkaoffsetreset", "legacy-reset"),
                      ("kafkaoffsetrollback", "legacy-rollback")]
            before = {kind: get(kind, name) for kind, name in stored}
            save("before-upgrade", before)
            old_lease = get("lease", f"{namespace}-leader")
            k("apply", "--server-side", "--field-manager=kafka-backup-operator-crds",
              "--force-conflicts", "-f", str(ROOT / "deploy/crds/all.yaml"))
            install(ROOT / "deploy/helm/kafka-backup-operator", args.image)
            for kind, name in stored:
                after = get(kind, name)
                assert after["metadata"]["uid"] == before[kind]["metadata"]["uid"], kind
                assert after["spec"] == before[kind]["spec"], kind
                assert after["status"] == before[kind]["status"], (kind, after["status"])
            save("after-upgrade", {kind: get(kind, name) for kind, name in stored})
            print("PASS: all five existing CR kinds retain UID, spec and status without replay", flush=True)
            new_lease = get("lease", f"{namespace}-leader")
            assert new_lease["spec"]["holderIdentity"] != old_lease["spec"]["holderIdentity"]
            assert new_lease["spec"]["leaseTransitions"] > old_lease["spec"]["leaseTransitions"]
            restore("upgrade-restore", "legacy", "upgrade-restored")
            resume_backup("legacy", 15, 6)
            restore("resumed-restore", "legacy", "resumed-restored")
            cr("KafkaOffsetReset", "candidate-reset", reset_spec)
            complete("kafkaoffsetreset", "candidate-reset")
            assert group_offsets() == {0: 0, 1: 0, 2: 0}
            cr("KafkaOffsetRollback", "candidate-rollback", rollback_spec)
            complete("kafkaoffsetrollback", "candidate-rollback")
            assert group_offsets() == {0: 10, 1: 10, 2: 10}
            cr("KafkaBackup", "candidate", backup_spec)
            complete("kafkabackup", "candidate")
            scheduled = copy.deepcopy(backup_spec)
            scheduled["schedule"] = "*/10 * * * * * *"
            cr("KafkaBackup", "scheduled", scheduled)
            first = wait("first scheduled backup", lambda: get("kafkabackup", "scheduled").get("status", {}).get("lastScheduleTime"))
            wait("second scheduled tick", lambda: get("kafkabackup", "scheduled").get("status", {}).get("lastScheduleTime") not in (None, first))
            k("patch", "kafkabackup", "scheduled", "--type=merge", "-p", '{"spec":{"suspend":true}}')
            wait("scheduled backup drains", lambda: get("kafkabackup", "scheduled").get("status", {}).get("phase") != "Running")
            assert get("kafkabackup", "scheduled")["status"]["backupId"] == "scheduled"
            restore("scheduled-restore", "scheduled", "scheduled-restored")
            validation_spec["schedule"] = "*/10 * * * * * *"
            cr("KafkaBackupValidation", "scheduled-validation", validation_spec)
            first = complete("kafkabackupvalidation", "scheduled-validation")["status"]["startTime"]
            wait("scheduled validation recurs", lambda: get("kafkabackupvalidation", "scheduled-validation").get("status", {}).get("startTime") not in (None, first))
            k("patch", "kafkabackupvalidation", "scheduled-validation", "--type=merge", "-p", '{"spec":{"suspend":true}}')
            wait("scheduled validation drains", lambda: get("kafkabackupvalidation", "scheduled-validation").get("status", {}).get("phase") != "Running")
            k("scale", f"deployment/{namespace}", "--replicas=2")
            k("rollout", "status", f"deployment/{namespace}", "--timeout=120s")
            assert leader_count() == 1, "two ready replicas must have exactly one active controller"
            lease = get("lease", f"{namespace}-leader")
            holder = lease["spec"]["holderIdentity"]
            k("delete", "pod", holder, "--wait=false")
            wait("leader handover with migrated Lease timestamps", lambda: get("lease", f"{namespace}-leader")["spec"].get("holderIdentity") not in (None, "", holder))
            wait("one active controller after handover", lambda: leader_count() == 1)
            save("lease-after-handover", get("lease", f"{namespace}-leader"))
            k("scale", f"deployment/{namespace}", "--replicas=1")
            k("rollout", "status", f"deployment/{namespace}", "--timeout=120s")
            # Keep new CRDs on rollback, as Helm does, and read a candidate archive.
            install(old_chart, previous_image)
            resume_backup("candidate", 5, 7)
            restore("rollback-restore", "candidate", "rollback-restored")
        save("result", {"passed": True, "namespace": namespace, "context": args.context,
                        "previous": args.previous_tag, "candidate": args.image, "records": len(records)})
        print(f"PASS: complete upgrade/rollback compatibility run; evidence {args.evidence}", flush=True)
    finally:
        for command, name in [(('get', 'pods', '-o', 'json'), 'pods.json'),
                              (('get', 'events', '-o', 'json'), 'events.json'),
                              (('logs', '-l', 'app.kubernetes.io/name=kafka-backup-operator', '--all-containers=true', '--tail=300'), 'operator.log')]:
            try:
                (args.evidence / name).write_text(k(*command))
            except (AssertionError, subprocess.TimeoutExpired):
                pass


if __name__ == "__main__":
    main()
