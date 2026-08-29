use chrono::Local;
use log::{LevelFilter, Log, Metadata, Record};
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

tokio::task_local! {
    pub static CURRENT_CLIENT: String;
}

pub struct TeeLogger {
    android: android_logger::AndroidLogger,
    file: Mutex<File>,
    level: LevelFilter,
}

impl TeeLogger {
    pub fn new(file_path: impl AsRef<Path>, level: LevelFilter) -> anyhow::Result<Self> {
        let path = PathBuf::from(file_path.as_ref());
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?;

        let android = android_logger::AndroidLogger::new(
            android_logger::Config::default()
                .with_tag("libfirefly")
                .with_max_level(level)
                .format(|f, record| {
                    write!(
                        f,
                        "libfirefly:{}:{} {}",
                        record.file().unwrap_or_default(),
                        record.line().unwrap_or_default(),
                        record.args()
                    )
                }),
        );

        Ok(Self {
            android,
            file: Mutex::new(file),
            level,
        })
    }
}

impl Log for TeeLogger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= self.level
    }

    fn log(&self, record: &Record) {
        if !self.enabled(record.metadata()) {
            return;
        }

        // logcat
        self.android.log(record);

        let client_tag = CURRENT_CLIENT
            .try_with(|id| format!("[{}] ", id))
            .unwrap_or_default();

        let log_line = format!(
            "{} [{}] {}libfirefly:{}:{} {}\n",
            Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            record.level(),
            client_tag,
            record.file().unwrap_or_default(),
            record.line().unwrap_or_default(),
            record.args()
        );

        if let Ok(mut f) = self.file.lock() {
            let _ = f.write_all(log_line.as_bytes());
        }
    }

    fn flush(&self) {
        let _ = self.file.lock().map(|mut f| f.flush());
    }
}
