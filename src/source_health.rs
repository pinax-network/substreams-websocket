//! Per-instance upstream read activity for embedding applications.

use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

use tokio::time::Instant;

use crate::StreamEvent;

/// A cloneable view of one embedded server's source readers. This contains no
/// endpoint, credential, cursor, or stream identity. The embedding application
/// chooses its own inactivity budget and readiness policy.
#[derive(Clone)]
pub struct SourceHealth {
    state: Arc<Mutex<State>>,
}

struct State {
    activity: Vec<Option<Instant>>,
    draining: bool,
}

/// An aggregate snapshot of upstream protocol activity, not chain freshness or
/// the availability of every configured channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceHealthSnapshot {
    pub configured_sources: usize,
    /// Readers with successful protocol activity within the supplied inactivity
    /// budget in their current attempt. Always zero while draining. A connected
    /// reader that has not received a response does not count.
    pub active_sources: usize,
    pub draining: bool,
}

impl SourceHealth {
    pub(crate) fn new(configured_sources: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                activity: vec![None; configured_sources],
                draining: false,
            })),
        }
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Evaluate activity using a monotonic clock. A response exactly at the
    /// inactivity boundary still counts; an ended attempt never does, even if
    /// its last response was recent. Empty blocks and progress responses count
    /// independently of decoded rows and WebSocket subscribers.
    pub fn snapshot(&self, max_inactivity: Duration) -> SourceHealthSnapshot {
        let state = self.lock();
        let now = Instant::now();
        SourceHealthSnapshot {
            configured_sources: state.activity.len(),
            active_sources: if state.draining {
                0
            } else {
                state
                    .activity
                    .iter()
                    .flatten()
                    .filter(|last| now.duration_since(**last) <= max_inactivity)
                    .count()
            },
            draining: state.draining,
        }
    }

    pub(crate) fn start_read(&self, index: usize) -> ReadActivity {
        self.lock().activity[index] = None;
        ReadActivity {
            health: self.clone(),
            index,
            ended: false,
        }
    }

    pub(crate) fn drain(&self) {
        let mut state = self.lock();
        state.draining = true;
        state.activity.fill(None);
    }
}

/// Scoped to one read attempt, so task cancellation, errors and EOF clear
/// activity before a reconnect can start. A fatal response cannot be followed
/// by a response that makes the same attempt healthy again.
pub(crate) struct ReadActivity {
    health: SourceHealth,
    index: usize,
    ended: bool,
}

impl ReadActivity {
    pub(crate) fn observe(&mut self, event: &StreamEvent) {
        match event {
            StreamEvent::Fatal { .. } => self.finish(),
            StreamEvent::Unknown => {}
            _ if !self.ended => {
                let mut state = self.health.lock();
                if !state.draining {
                    state.activity[self.index] = Some(Instant::now());
                }
            }
            _ => {}
        }
    }

    fn finish(&mut self) {
        self.ended = true;
        self.health.lock().activity[self.index] = None;
    }
}

impl Drop for ReadActivity {
    fn drop(&mut self) {
        self.finish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const BUDGET: Duration = Duration::from_secs(120);

    fn progress() -> StreamEvent {
        StreamEvent::Progress {
            running_jobs: 0,
            processed_blocks: 0,
        }
    }

    #[tokio::test(start_paused = true)]
    async fn protocol_activity_is_independent_of_rows_and_expires_monotonically() {
        let health = SourceHealth::new(3);
        let mut first = health.start_read(0);
        let mut second = health.start_read(1);
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        first.observe(&StreamEvent::Unknown);
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        first.observe(&StreamEvent::Session {
            trace_id: String::new(),
            resolved_start_block: 0,
            chain_head: 0,
        });
        second.observe(&progress());
        assert_eq!(health.snapshot(BUDGET).configured_sources, 3);
        assert_eq!(health.snapshot(BUDGET).active_sources, 2);
        tokio::time::advance(BUDGET).await;
        assert_eq!(health.snapshot(BUDGET).active_sources, 2);
        tokio::time::advance(Duration::from_nanos(1)).await;
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        first.observe(&StreamEvent::Unknown);
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        first.observe(&StreamEvent::Block {
            number: 1,
            id: "block".into(),
            timestamp: String::new(),
            timestamp_seconds: 0,
            output_type_url: crate::SUPPORTED_OUTPUT_TYPE_URL.into(),
            payload: Vec::new(),
            cursor: String::new(),
        });
        assert_eq!(health.snapshot(BUDGET).active_sources, 1);
        assert_eq!(health.snapshot(Duration::ZERO).active_sources, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn fatal_and_ended_attempts_clear_immediately_and_retries_start_inactive() {
        let health = SourceHealth::new(1);
        let mut read = health.start_read(0);
        read.observe(&progress());
        read.observe(&StreamEvent::Fatal {
            message: String::new(),
        });
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        read.observe(&progress());
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        drop(read);
        let mut retry = health.start_read(0);
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
        retry.observe(&progress());
        assert_eq!(health.snapshot(BUDGET).active_sources, 1);
        drop(retry);
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
    }

    #[tokio::test]
    async fn task_cancellation_clears_an_active_read() {
        let health = SourceHealth::new(1);
        let reader_health = health.clone();
        let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let mut read = reader_health.start_read(0);
            read.observe(&progress());
            ready_tx.send(()).unwrap();
            std::future::pending::<()>().await;
            drop(read);
        });
        ready_rx.await.unwrap();
        assert_eq!(health.snapshot(BUDGET).active_sources, 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(health.snapshot(BUDGET).active_sources, 0);
    }

    #[test]
    fn drain_is_immediate_permanent_and_isolated_between_instances() {
        let health = SourceHealth::new(1);
        let other = SourceHealth::new(1);
        let handle = health.clone();
        let mut read = health.start_read(0);
        let mut other_read = other.start_read(0);
        read.observe(&progress());
        other_read.observe(&progress());
        health.drain();
        read.observe(&progress());
        assert_eq!(
            handle.snapshot(BUDGET),
            SourceHealthSnapshot {
                configured_sources: 1,
                active_sources: 0,
                draining: true,
            }
        );
        assert_eq!(other.snapshot(BUDGET).active_sources, 1);
        assert!(!other.snapshot(BUDGET).draining);
    }
}
