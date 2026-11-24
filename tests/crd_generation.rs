use std::process::Command;

use kafka_backup_operator::crd::KafkaRestoreSpec;
use serde_json::{json, Value};

fn generate_bundle() -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_crdgen"))
        .output()
        .expect("run crdgen");
    assert!(
        output.status.success(),
        "crdgen failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("crdgen must emit UTF-8")
}

fn check_schema(schema: &Value, path: &str, violations: &mut Vec<String>) {
    for keyword in [
        "definitions",
        "dependencies",
        "deprecated",
        "discriminator",
        "id",
        "patternProperties",
        "readOnly",
        "writeOnly",
        "xml",
        "$ref",
    ] {
        if schema.get(keyword).is_some() {
            violations.push(format!("{path}: {keyword} must not be set"));
        }
    }
    if schema["uniqueItems"] == true {
        violations.push(format!("{path}: uniqueItems must not be true"));
    }
    if schema["additionalProperties"] == false {
        violations.push(format!("{path}: additionalProperties must not be false"));
    }
    if schema.get("additionalProperties").is_some() && schema.get("properties").is_some() {
        violations.push(format!(
            "{path}: additionalProperties and properties are mutually exclusive"
        ));
    }

    // Visit subschemas, not arbitrary data like defaults or enum members.
    if let Some(properties) = schema["properties"].as_object() {
        for (name, child) in properties {
            check_schema(child, &format!("{path}.properties.{name}"), violations);
        }
    }
    for keyword in [
        "additionalProperties",
        "additionalItems",
        "items",
        "allOf",
        "anyOf",
        "oneOf",
        "not",
    ] {
        let child = &schema[keyword];
        if let Some(children) = child.as_array() {
            for (index, child) in children.iter().enumerate() {
                check_schema(child, &format!("{path}.{keyword}[{index}]"), violations);
            }
        } else if child.is_object() {
            check_schema(child, &format!("{path}.{keyword}"), violations);
        }
    }
}

#[test]
fn generated_crd_schemas_avoid_unsupported_kubernetes_keywords() {
    let bundle = generate_bundle();
    let crds: Vec<Value> = serde_saphyr::from_multiple(&bundle).expect("parse generated bundle");
    assert!(!crds.is_empty(), "crdgen must emit CRDs");
    let mut violations = Vec::new();

    for crd in &crds {
        let kind = crd["spec"]["names"]["kind"].as_str().expect("CRD kind");
        let versions = crd["spec"]["versions"].as_array().expect("CRD versions");
        assert!(!versions.is_empty(), "{kind} has no versions");
        for version in versions {
            let name = version["name"].as_str().expect("CRD version name");
            let schema = &version["schema"]["openAPIV3Schema"];
            assert!(schema.is_object(), "{kind}/{name} has no OpenAPI schema");
            check_schema(schema, &format!("{kind}/{name}"), &mut violations);
        }
    }

    assert!(
        violations.is_empty(),
        "invalid CRD schema keywords:\n{}",
        violations.join("\n")
    );

    let restore = crds
        .iter()
        .find(|crd| crd["spec"]["names"]["kind"] == "KafkaRestore")
        .expect("crdgen must emit KafkaRestore");
    for version in restore["spec"]["versions"].as_array().unwrap() {
        let schema = version
            .pointer("/schema/openAPIV3Schema/properties/spec/properties/partitionMapping")
            .expect("KafkaRestore partitionMapping schema");
        assert_eq!(schema["type"], "object");
        assert_eq!(schema["default"], json!({}));
        assert_eq!(schema["additionalProperties"]["type"], "integer");
        assert_eq!(schema["additionalProperties"]["format"], "int32");
    }
}

#[test]
fn checked_in_crd_bundles_match_crdgen() {
    let bundle = generate_bundle();
    assert!(
        bundle == include_str!("../deploy/crds/all.yaml"),
        "regenerate the standalone CRD bundle with crdgen"
    );
    assert!(
        bundle == include_str!("../deploy/helm/kafka-backup-operator/crds/all.yaml"),
        "update the Helm CRD bundle with the generated CRDs"
    );
}

#[test]
fn restore_partition_mapping_still_deserializes_integer_keys() {
    let mut spec = json!({
        "backupRef": {"name": "backup"},
        "kafkaCluster": {"bootstrapServers": ["kafka:9092"]},
        "partitionMapping": {"0": 2}
    });
    let restore: KafkaRestoreSpec = serde_json::from_value(spec.clone()).unwrap();
    assert_eq!(restore.partition_mapping.get(&0), Some(&2));

    spec["partitionMapping"] = json!({"not-a-partition": 2});
    assert!(serde_json::from_value::<KafkaRestoreSpec>(spec).is_err());
}
