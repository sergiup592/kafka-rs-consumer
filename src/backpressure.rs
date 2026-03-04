use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use tokio::sync::{AcquireError, OwnedSemaphorePermit, Semaphore};

#[derive(Clone)]
pub struct BackpressureController {
    semaphore: Arc<Semaphore>,
    in_flight: Arc<AtomicUsize>,
    max_in_flight: usize,
}

impl BackpressureController {
    pub fn new(max_in_flight: usize) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_in_flight)),
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight,
        }
    }

    pub async fn acquire(&self) -> Result<BackpressurePermit, AcquireError> {
        let permit = self.semaphore.clone().acquire_owned().await?;
        self.in_flight.fetch_add(1, Ordering::Relaxed);
        Ok(BackpressurePermit {
            permit,
            in_flight: Arc::clone(&self.in_flight),
        })
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }
}

pub struct BackpressurePermit {
    permit: OwnedSemaphorePermit,
    in_flight: Arc<AtomicUsize>,
}

impl Drop for BackpressurePermit {
    fn drop(&mut self) {
        let _ = &self.permit;
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}
