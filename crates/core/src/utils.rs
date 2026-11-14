use std::time::{Duration, SystemTime, UNIX_EPOCH};

lazy_static::lazy_static! {
    pub static ref HTTP_CLIENT: reqwest::Client = {
        #[cfg(not(target_arch = "wasm32"))]
        {
            reqwest::Client::builder()
                .pool_max_idle_per_host(0)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new())
        }
        #[cfg(target_arch = "wasm32")]
        {
            reqwest::Client::new()
        }
    };
}

lazy_static::lazy_static! {
    /// Only honoured with the `test-hooks` feature so release builds can never skip credential
    /// expiry checks via the environment.
    pub static ref EMULATOR_MODE: bool =
        cfg!(feature = "test-hooks") && std::env::var("EMULATOR_MODE").unwrap_or_default() == "true";
}

const BEARER_WORD: &str = "Bearer ";

pub fn get_datetime_from_millis(milliseconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(milliseconds)
}

pub fn get_system_time_from_secs(seconds: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_secs(seconds)
}

#[cfg(target_arch = "wasm32")]
#[derive(Clone, Copy, Debug)]
pub struct SendWrapper<F>(pub F);

#[cfg(target_arch = "wasm32")]
unsafe impl<F> Send for SendWrapper<F> {}

#[cfg(target_arch = "wasm32")]
unsafe impl<F> Sync for SendWrapper<F> {}

#[cfg(target_arch = "wasm32")]
impl<F: std::future::Future> std::future::Future for SendWrapper<F> {
    type Output = F::Output;
    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output> {
        unsafe { self.map_unchecked_mut(|s| &mut s.0).poll(cx) }
    }
}

pub fn current_timestamp_duration_since_epoch() -> Duration {
    #[cfg(target_arch = "wasm32")]
    {
        Duration::from_millis(js_sys::Date::now() as u64)
    }
    #[cfg(not(target_arch = "wasm32"))]
    {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time Went Backwards 🚨🚨🚨 !!!")
    }
}

pub fn now_system_time() -> SystemTime {
    UNIX_EPOCH + current_timestamp_duration_since_epoch()
}

pub fn get_current_timestamp_in_millis() -> u64 {
    current_timestamp_duration_since_epoch().as_millis() as u64
}

pub fn get_current_timestamp_in_secs() -> u64 {
    current_timestamp_duration_since_epoch().as_secs()
}

pub fn get_current_timestamp_in_microsecs() -> u128 {
    current_timestamp_duration_since_epoch().as_micros()
}

pub fn remove_bearer_word<'a>(s: &'a str) -> Option<&'a str> {
    if s.starts_with(BEARER_WORD) {
        Some(&s[BEARER_WORD.len()..])
    } else {
        None
    }
}

const CHARACTERS: &[u8] =
    b"0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ\\^_`abcdefghijklmnopqrstuvwxyz{|}~";
// const CHARACTERS: &[u8] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

pub fn number_to_string(mut n: u64) -> String {
    if n == 0 {
        String::new()
    } else {
        let mut cs = Vec::<u8>::with_capacity(12);
        while n > 0 {
            let idx = ((n - 1) as usize) % CHARACTERS.len();
            let c = CHARACTERS[idx];
            cs.push(c);

            n = (n - 1) / CHARACTERS.len() as u64;
        }

        cs.reverse();
        unsafe { String::from_utf8_unchecked(cs) }
    }
}

pub fn number_to_string_write(mut n: u64, s: &mut String) -> usize {
    if n == 0 {
        0
    } else {
        unsafe {
            let mut cs = [0u8; 12];
            let mut cis = 0;
            while n > 0 {
                let idx = ((n - 1) as usize) % CHARACTERS.len();
                let c = CHARACTERS[idx];
                cs[cis] = c;
                *cs.get_unchecked_mut(cis) = c;
                cis += 1;

                n = (n - 1) / CHARACTERS.len() as u64;
            }
            let cs_len = cs.len();

            for i in (0..cis).rev() {
                let c = *cs.get_unchecked(i);
                s.push(char::from_u32_unchecked(c as u32));
            }
            cs_len
        }
    }
}

pub fn string_to_number(s: &str) -> Option<u64> {
    if s.len() > 11 {
        return None;
    }
    let mut n: u64 = 0;

    for (idx, c) in s.as_bytes().iter().rev().enumerate() {
        let pow = CHARACTERS.len().pow(idx as u32);

        let (c_idx, _) = CHARACTERS.iter().enumerate().find(|(_idx, x)| **x == *c)?;

        n = n + (pow * (1 + c_idx)) as u64;
    }

    Some(n)
}

/// How much later than the server a receiver may judge a credential's expiry. The server checks
/// a commit against its own clock when accepting it, and the message id the receiver is handed
/// comes from the database clock a moment later; a receiver must never refuse what the server
/// accepted over that gap.
pub const RECEIVE_EXPIRY_GRACE_SECS: u64 = 10 * 60;

tokio::task_local! {
    /// Server time (unix seconds) of the commit being processed, set by
    /// [`with_commit_time`] while a received commit is applied.
    static COMMIT_TIME_SECS: u64;
}

/// Runs `fut` (the processing of one received message) with `commit_time_secs` as the time
/// credential expiry is judged against, instead of this device's clock. Every member then
/// decides a commit's validity at the same instant (the server's), so a member that processes
/// it days later or whose clock is wrong reaches the same result as everyone else.
pub async fn with_commit_time<F: std::future::Future>(commit_time_secs: Option<u64>, fut: F) -> F::Output {
    match commit_time_secs {
        Some(secs) => COMMIT_TIME_SECS.scope(secs, fut).await,
        None => fut.await,
    }
}

/// The server time of the received commit being processed, if known.
pub fn commit_time_secs() -> Option<u64> {
    COMMIT_TIME_SECS.try_with(|t| *t).ok()
}

/// Server message ids are the database clock in microseconds since the epoch. Returns that time
/// in seconds when `id` looks like one (2020..2100), `None` otherwise.
pub fn server_id_time_secs(id: u64) -> Option<u64> {
    const MIN_SECS: u64 = 1_577_836_800; // 2020-01-01
    const MAX_SECS: u64 = 4_102_444_800; // 2100-01-01
    let secs = id / 1_000_000;
    (MIN_SECS..MAX_SECS).contains(&secs).then_some(secs)
}

/// The instant a credential must still be valid at to be accepted: the commit's server time
/// (less [`RECEIVE_EXPIRY_GRACE_SECS`]) while a received commit is processed, else now.
pub fn credential_check_time_secs() -> u64 {
    match commit_time_secs() {
        Some(t) => t.saturating_sub(RECEIVE_EXPIRY_GRACE_SECS),
        None => get_current_timestamp_in_secs(),
    }
}

/// The instant a credential counts as expired by, for the rules that let any member remove an
/// expired device: the commit's server time (plus [`RECEIVE_EXPIRY_GRACE_SECS`]) while a
/// received commit is processed, else now. Lenient on receive in the same direction as
/// [`credential_check_time_secs`]: a receiver accepts whatever the server accepted.
pub fn expiry_reference_time_secs() -> u64 {
    match commit_time_secs() {
        Some(t) => t.saturating_add(RECEIVE_EXPIRY_GRACE_SECS),
        None => get_current_timestamp_in_secs(),
    }
}
