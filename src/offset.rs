use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use rdkafka::{
    consumer::{CommitMode, Consumer, ConsumerContext},
    Offset, TopicPartitionList,
};

use crate::{config::CommitStrategy, error::KafkaError};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct TopicPartitionKey {
    topic: String,
    partition: i32,
}

impl TopicPartitionKey {
    fn new(topic: impl Into<String>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }
}

#[derive(Debug, Default)]
struct TrackerState {
    pending: HashMap<TopicPartitionKey, i64>,
    since_last_commit: usize,
}

#[derive(Clone)]
pub struct OffsetTracker {
    strategy: CommitStrategy,
    batch_size: usize,
    state: Arc<Mutex<TrackerState>>,
}

impl OffsetTracker {
    pub fn new(strategy: CommitStrategy, batch_size: usize) -> Self {
        Self {
            strategy,
            batch_size: batch_size.max(1),
            state: Arc::new(Mutex::new(TrackerState::default())),
        }
    }

    pub fn strategy(&self) -> CommitStrategy {
        self.strategy.clone()
    }

    pub fn track_success(&self, topic: &str, partition: i32, offset: i64) {
        if self.strategy != CommitStrategy::Manual {
            return;
        }

        let key = TopicPartitionKey::new(topic.to_string(), partition);
        let next_offset = offset + 1;

        let mut state = self.state.lock().expect("offset tracker lock poisoned");
        state
            .pending
            .entry(key)
            .and_modify(|existing| {
                if next_offset > *existing {
                    *existing = next_offset;
                }
            })
            .or_insert(next_offset);
        state.since_last_commit += 1;
    }

    pub fn should_commit(&self) -> bool {
        if self.strategy != CommitStrategy::Manual {
            return false;
        }

        let state = self.state.lock().expect("offset tracker lock poisoned");
        state.since_last_commit >= self.batch_size
    }

    pub fn pending_count(&self) -> usize {
        let state = self.state.lock().expect("offset tracker lock poisoned");
        state.pending.len()
    }

    pub fn snapshot_pending_offsets(&self) -> Vec<(String, i32, i64)> {
        let state = self.state.lock().expect("offset tracker lock poisoned");
        state
            .pending
            .iter()
            .map(|(tp, offset)| (tp.topic.clone(), tp.partition, *offset))
            .collect()
    }

    pub fn commit_pending<C, Ctx>(
        &self,
        consumer: &C,
        mode: CommitMode,
    ) -> Result<usize, KafkaError>
    where
        C: Consumer<Ctx>,
        Ctx: ConsumerContext,
    {
        if self.strategy != CommitStrategy::Manual {
            return Ok(0);
        }

        let mut state = self.state.lock().expect("offset tracker lock poisoned");
        if state.pending.is_empty() {
            return Ok(0);
        }

        let mut tpl = TopicPartitionList::new();
        for (tp, offset) in &state.pending {
            tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*offset))
                .map_err(KafkaError::Commit)?;
        }

        consumer.commit(&tpl, mode).map_err(KafkaError::Commit)?;

        let committed = state.pending.len();
        state.pending.clear();
        state.since_last_commit = 0;

        Ok(committed)
    }

    pub fn commit_revoked<C, Ctx>(
        &self,
        consumer: &C,
        revoked: &TopicPartitionList,
        mode: CommitMode,
    ) -> Result<usize, KafkaError>
    where
        C: Consumer<Ctx>,
        Ctx: ConsumerContext,
    {
        if self.strategy != CommitStrategy::Manual {
            return Ok(0);
        }

        let mut state = self.state.lock().expect("offset tracker lock poisoned");
        if state.pending.is_empty() {
            return Ok(0);
        }

        let mut keys = Vec::new();
        let mut tpl = TopicPartitionList::new();

        for elem in revoked.elements() {
            let key = TopicPartitionKey::new(elem.topic(), elem.partition());
            if let Some(offset) = state.pending.get(&key).copied() {
                tpl.add_partition_offset(elem.topic(), elem.partition(), Offset::Offset(offset))
                    .map_err(KafkaError::Commit)?;
                keys.push(key);
            }
        }

        if keys.is_empty() {
            return Ok(0);
        }

        consumer.commit(&tpl, mode).map_err(KafkaError::Commit)?;

        for key in &keys {
            state.pending.remove(key);
        }
        state.since_last_commit = state
            .since_last_commit
            .saturating_sub(keys.len())
            .min(state.pending.len());

        Ok(keys.len())
    }
}

#[cfg(test)]
impl OffsetTracker {
    fn tracked_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        let state = self.state.lock().expect("offset tracker lock poisoned");
        state
            .pending
            .get(&TopicPartitionKey::new(topic.to_string(), partition))
            .copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_highest_next_offset_per_partition() {
        let tracker = OffsetTracker::new(CommitStrategy::Manual, 10);

        tracker.track_success("topic-a", 0, 4);
        tracker.track_success("topic-a", 0, 2);
        tracker.track_success("topic-a", 0, 9);

        assert_eq!(tracker.tracked_offset("topic-a", 0), Some(10));
    }

    #[test]
    fn batch_threshold_is_respected() {
        let tracker = OffsetTracker::new(CommitStrategy::Manual, 2);

        tracker.track_success("topic-a", 0, 1);
        assert!(!tracker.should_commit());

        tracker.track_success("topic-a", 0, 2);
        assert!(tracker.should_commit());
    }

    #[test]
    fn auto_strategy_skips_tracking() {
        let tracker = OffsetTracker::new(CommitStrategy::Auto, 1);
        tracker.track_success("topic-a", 0, 1);

        assert_eq!(tracker.pending_count(), 0);
        assert!(!tracker.should_commit());
    }
}
