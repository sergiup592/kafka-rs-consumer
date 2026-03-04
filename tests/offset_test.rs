use kafka_rs_consumer::{config::CommitStrategy, offset::OffsetTracker};

#[test]
fn tracks_latest_offsets_per_partition() {
    let tracker = OffsetTracker::new(CommitStrategy::Manual, 10);

    tracker.track_success("orders", 0, 4);
    tracker.track_success("orders", 0, 6);
    tracker.track_success("orders", 0, 3);
    tracker.track_success("orders", 1, 2);

    let mut pending = tracker.snapshot_pending_offsets();
    pending.sort_by(|a, b| a.1.cmp(&b.1));

    assert_eq!(pending.len(), 2);
    assert_eq!(pending[0], ("orders".to_string(), 0, 7));
    assert_eq!(pending[1], ("orders".to_string(), 1, 3));
}

#[test]
fn commit_batching_threshold_is_enforced() {
    let tracker = OffsetTracker::new(CommitStrategy::Manual, 3);

    tracker.track_success("events", 0, 1);
    tracker.track_success("events", 0, 2);
    assert!(!tracker.should_commit());

    tracker.track_success("events", 0, 3);
    assert!(tracker.should_commit());
}

#[test]
fn auto_commit_strategy_does_not_track_offsets() {
    let tracker = OffsetTracker::new(CommitStrategy::Auto, 1);

    tracker.track_success("events", 0, 1);
    tracker.track_success("events", 1, 1);

    assert_eq!(tracker.pending_count(), 0);
    assert!(!tracker.should_commit());
}
