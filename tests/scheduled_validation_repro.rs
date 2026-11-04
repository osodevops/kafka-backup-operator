//! Scheduled KafkaBackupValidations never ran again after their first run.
//!
//! Once `status.observedGeneration` matched the spec, the controller handed a
//! `Completed` or `Failed` validation to `check_schedule`, which only
//! requeued until the next cron tick and never executed. A validation with
//! `schedule: "0 0 2 * * *"` ran once at creation and then reported the same
//! `lastValidationTime` forever (reproduced in minikube: 5 schedule wakeups,
//! 1 run).
//!
//! These tests pin the scheduling decision: a run is due when a cron tick has
//! passed since the last run started (`status.startTime`), and a tick this
//! process already fired is not fired again from a stale reflector cache.

use std::time::Duration;

use chrono::{DateTime, TimeZone, Utc};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::Time;
use kafka_backup_operator::crd::KafkaBackupValidation;
use kafka_backup_operator::reconcilers::validation::{
    next_validation_time, record_scheduled_run, requeue_until_next_run, scheduled_validation_due,
};
use kube::runtime::controller::Action;
use serde_json::json;

fn at(h: u32, m: u32, s: u32, ms: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 11, 4, h, m, s).unwrap() + chrono::Duration::milliseconds(ms.into())
}

/// A validation scheduled every 15 seconds (ticks at :00, :15, :30, :45).
fn scheduled(name: &str, phase: &str, last_start: Option<DateTime<Utc>>) -> KafkaBackupValidation {
    let spec = json!({
        "backupRef": {"name": "orders-backup"},
        "checks": {"messageCount": {"enabled": true}},
        "schedule": "*/15 * * * * *",
    });
    let mut validation = KafkaBackupValidation::new(name, serde_json::from_value(spec).unwrap());
    validation.metadata.namespace = Some("kbo".to_string());
    validation.metadata.generation = Some(1);
    validation.metadata.creation_timestamp = Some(Time(at(7, 20, 45, 0)));
    validation.status = Some(
        serde_json::from_value(json!({
            "phase": phase,
            "observedGeneration": 1,
            "startTime": last_start,
        }))
        .unwrap(),
    );
    validation
}

#[test]
fn completed_validation_runs_again_once_the_next_tick_passes() {
    let v = scheduled("rerun-completed", "Completed", Some(at(7, 20, 45, 395)));

    assert!(!scheduled_validation_due(&v, at(7, 20, 50, 0)));
    assert!(!scheduled_validation_due(&v, at(7, 20, 59, 999)));
    assert!(scheduled_validation_due(&v, at(7, 21, 0, 0)));
    assert!(
        scheduled_validation_due(&v, at(7, 21, 7, 0)),
        "a missed tick still runs once"
    );
}

#[test]
fn failed_scheduled_validation_runs_again_at_the_next_tick() {
    let v = scheduled("rerun-failed", "Failed", Some(at(7, 20, 45, 395)));

    assert!(!scheduled_validation_due(&v, at(7, 20, 59, 0)));
    assert!(scheduled_validation_due(&v, at(7, 21, 0, 0)));
}

/// The reflector cache can lag our own status patches by tens of
/// milliseconds. A fast validation can finish and be reconciled again while
/// the cache still shows the previous run's `startTime`; the tick this
/// process just fired must not fire twice.
#[test]
fn a_tick_already_fired_in_process_is_not_fired_again_from_a_stale_cache() {
    let stale = scheduled("rerun-stale-cache", "Completed", Some(at(7, 20, 45, 395)));
    assert!(scheduled_validation_due(&stale, at(7, 21, 0, 10)));

    record_scheduled_run(&stale, at(7, 21, 0, 10));

    assert!(!scheduled_validation_due(&stale, at(7, 21, 0, 40)));
    assert!(!scheduled_validation_due(&stale, at(7, 21, 14, 999)));
    assert!(scheduled_validation_due(&stale, at(7, 21, 15, 0)));
}

#[test]
fn the_guard_is_per_resource() {
    let fired = scheduled("guard-fired", "Completed", Some(at(7, 20, 44, 0)));
    let other = scheduled("guard-other", "Completed", Some(at(7, 20, 44, 0)));

    record_scheduled_run(&fired, at(7, 21, 0, 0));

    assert!(!scheduled_validation_due(&fired, at(7, 21, 0, 30)));
    assert!(scheduled_validation_due(&other, at(7, 21, 0, 30)));
}

#[test]
fn unscheduled_or_suspended_validations_do_not_rerun() {
    let mut one_shot = scheduled("one-shot", "Completed", Some(at(7, 20, 44, 0)));
    one_shot.spec.schedule = None;
    assert!(!scheduled_validation_due(&one_shot, at(8, 0, 0, 0)));

    let mut suspended = scheduled("suspended", "Completed", Some(at(7, 20, 44, 0)));
    suspended.spec.suspend = true;
    assert!(!scheduled_validation_due(&suspended, at(8, 0, 0, 0)));

    let mut invalid = scheduled("invalid-cron", "Completed", Some(at(7, 20, 44, 0)));
    invalid.spec.schedule = Some("not a cron".to_string());
    assert!(!scheduled_validation_due(&invalid, at(8, 0, 0, 0)));
}

/// Without a recorded start (status lost or never written), the creation
/// time anchors the schedule instead of firing immediately.
#[test]
fn creation_time_anchors_a_validation_without_a_recorded_start() {
    let v = scheduled("no-start-time", "Completed", None);

    assert!(!scheduled_validation_due(&v, at(7, 20, 50, 0)));
    assert!(scheduled_validation_due(&v, at(7, 21, 0, 0)));
}

#[test]
fn next_run_is_the_next_tick_after_now() {
    let v = scheduled("next-run", "Completed", Some(at(7, 20, 44, 0)));

    assert_eq!(
        next_validation_time(&v, at(7, 20, 50, 0)),
        Some(at(7, 21, 0, 0))
    );

    let mut one_shot = v.clone();
    one_shot.spec.schedule = None;
    assert_eq!(next_validation_time(&one_shot, at(7, 20, 50, 0)), None);
}

/// After a run, the controller requeues for the next tick rather than
/// re-entering the schedule check with the pre-run object (which would read
/// the old `startTime` and run again immediately).
#[test]
fn after_a_run_the_controller_waits_for_the_next_tick() {
    let v = scheduled("requeue", "Completed", Some(at(7, 20, 44, 0)));
    assert_eq!(
        requeue_until_next_run(&v, at(7, 20, 50, 0)),
        Action::requeue(Duration::from_secs(10))
    );

    let mut daily = v.clone();
    daily.spec.schedule = Some("0 0 2 * * *".to_string());
    assert_eq!(
        requeue_until_next_run(&daily, at(7, 20, 50, 0)),
        Action::requeue(Duration::from_secs(300)),
        "long waits are capped so a lost requeue is recovered"
    );

    let mut one_shot = v.clone();
    one_shot.spec.schedule = None;
    assert_eq!(
        requeue_until_next_run(&one_shot, at(7, 20, 50, 0)),
        Action::await_change()
    );
}

/// Changing the spec runs a validation at once, but setting `suspend: true`
/// is a spec change too: it ran one last validation instead of stopping.
/// A suspended validation records the new generation without running.
#[test]
fn a_suspended_validation_does_not_run_on_spec_change() {
    use kafka_backup_operator::reconcilers::validation::runs_on_spec_change;

    let mut v = scheduled("suspend-on-change", "Completed", Some(at(7, 20, 45, 395)));
    v.metadata.generation = Some(2);
    assert!(runs_on_spec_change(&v));

    v.spec.suspend = true;
    assert!(!runs_on_spec_change(&v));

    v.spec.schedule = None;
    assert!(
        !runs_on_spec_change(&v),
        "one-shot validations created suspended wait too"
    );
}
