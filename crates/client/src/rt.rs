//! Task and timer primitives shared by every target.
//!
//! Native builds delegate straight to tokio. wasm32 has no tokio runtime or clock, so tasks run
//! on the browser event loop (`spawn_local`) and timers use `setTimeout`. Callers use the same
//! API on both, which keeps the client logic itself free of target-specific branches.

use std::{future::Future, time::Duration};

tokio::task_local! {
    pub static CURRENT_CLIENT: String;
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::*;
#[cfg(target_arch = "wasm32")]
pub use wasm::*;

/// Returned when [`timeout`] gives up before the future completes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed;

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::*;

    pub use tokio::task::{JoinError, JoinHandle, yield_now};

    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(future)
    }

    pub async fn sleep(duration: Duration) {
        tokio::time::sleep(duration).await
    }

    pub async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, Elapsed> {
        tokio::time::timeout(duration, future).await.map_err(|_| Elapsed)
    }

    pub type Interval = tokio::time::Interval;

    /// Ticks every `period`, starting immediately; late ticks delay the following ones.
    pub fn interval(period: Duration) -> Interval {
        let mut interval = tokio::time::interval(period);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        interval
    }
}

#[cfg(target_arch = "wasm32")]
mod wasm {
    use super::*;
    use firefly_core::utils::SendWrapper;
    use futures::future::{AbortHandle, Abortable, Either};
    use std::{
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::sync::oneshot;

    #[derive(Debug)]
    pub struct JoinError;

    impl std::fmt::Display for JoinError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("task was cancelled")
        }
    }

    impl std::error::Error for JoinError {}

    pub struct JoinHandle<T> {
        abort: AbortHandle,
        result: oneshot::Receiver<T>,
    }

    impl<T> JoinHandle<T> {
        pub fn abort(&self) {
            self.abort.abort();
        }
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;

        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Pin::new(&mut self.result).poll(cx).map_err(|_| JoinError)
        }
    }

    /// The browser is single threaded, so unlike tokio the future does not need to be `Send`.
    pub fn spawn<F>(future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let (abort, registration) = AbortHandle::new_pair();
        let (tx, result) = oneshot::channel();
        wasm_bindgen_futures::spawn_local(async move {
            if let Ok(output) = Abortable::new(future, registration).await {
                let _ = tx.send(output);
            }
        });
        JoinHandle { abort, result }
    }

    /// Longest delay `setTimeout` honours; longer ones overflow and fire immediately.
    const MAX_TIMER_MILLIS: u128 = i32::MAX as u128;

    /// `Send` so shared async code awaiting it stays `Send` (wasm runs a single thread).
    /// Durations beyond the browser's timer limit (~24.8 days) are clamped to it.
    pub fn sleep(duration: Duration) -> impl Future<Output = ()> + Send {
        let millis = duration.as_millis().min(MAX_TIMER_MILLIS) as u32;
        SendWrapper(gloo_timers::future::TimeoutFuture::new(millis))
    }

    pub async fn timeout<F: Future>(duration: Duration, future: F) -> Result<F::Output, Elapsed> {
        let future = std::pin::pin!(future);
        let timer = std::pin::pin!(sleep(duration));
        match futures::future::select(future, timer).await {
            Either::Left((output, _)) => Ok(output),
            Either::Right(_) => Err(Elapsed),
        }
    }

    pub async fn yield_now() {
        sleep(Duration::ZERO).await
    }

    /// Ticks every `period`, starting immediately; late ticks delay the following ones.
    pub struct Interval {
        period: Duration,
        first: bool,
    }

    pub fn interval(period: Duration) -> Interval {
        Interval { period, first: true }
    }

    impl Interval {
        pub async fn tick(&mut self) {
            if std::mem::take(&mut self.first) {
                return;
            }
            sleep(self.period).await;
        }
    }
}
