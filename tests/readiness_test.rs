//! `/readyz` and the leader gauge follow the published leader-election state.

use http_body_util::BodyExt;
use hyper::StatusCode;
use kafka_backup_operator::leader::LeaderState;
use kafka_backup_operator::metrics;

async fn readyz() -> (StatusCode, String) {
    let response = metrics::ready_response();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8(body.to_vec()).unwrap())
}

#[tokio::test]
async fn readyz_follows_the_leader_state_and_the_gauge_marks_the_leader() {
    // Election disabled / nothing published yet: ready, as before.
    assert_eq!(readyz().await, (StatusCode::OK, "leader".to_string()));

    metrics::set_leader_state("kbo-operator-abc", LeaderState::Unknown);
    let (status, body) = readyz().await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, "leader election pending");
    assert_eq!(
        metrics::LEADER
            .with_label_values(&["kbo-operator-abc"])
            .get(),
        0.0
    );

    metrics::set_leader_state("kbo-operator-abc", LeaderState::Follower);
    assert_eq!(readyz().await, (StatusCode::OK, "standby".to_string()));
    assert_eq!(
        metrics::LEADER
            .with_label_values(&["kbo-operator-abc"])
            .get(),
        0.0
    );

    metrics::set_leader_state("kbo-operator-abc", LeaderState::Leader);
    assert_eq!(readyz().await, (StatusCode::OK, "leader".to_string()));
    assert_eq!(
        metrics::LEADER
            .with_label_values(&["kbo-operator-abc"])
            .get(),
        1.0
    );
    assert_eq!(metrics::leader_state(), LeaderState::Leader);
}
