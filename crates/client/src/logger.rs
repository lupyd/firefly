use chrono::Local;
use log::{LevelFilter, Log, Metadata, Record};
use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

tokio::task_local! {
    pub static CURRENT_CLIENT: String;
}

pub struct TeeLogger {
    android: android_logger::AndroidLogger,
    files: Mutex<HashMap<String, File>>,
    default_file: Mutex<File>,
    log_dir: PathBuf,
    level: LevelFilter,
}

impl TeeLogger {
    pub fn new(file_path: impl AsRef<Path>, level: LevelFilter) -> anyhow::Result<Self> {
        let path = PathBuf::from(file_path.as_ref());
        let log_dir = path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();

        std::fs::create_dir_all(&log_dir)?;

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
            files: Mutex::new(HashMap::new()),
            default_file: Mutex::new(file),
            log_dir,
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

        let log_line = format!(
            "{} [{}] libfirefly:{}:{} {}\n",
            Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            record.level(),
            record.file().unwrap_or_default(),
            record.line().unwrap_or_default(),
            record.args()
        );

        let client_id = CURRENT_CLIENT.try_with(|id| id.clone()).ok();

        if let Some(id) = client_id {
            let mut files = self.files.lock().unwrap();
            if !files.contains_key(&id) {
                let path = self.log_dir.join(format!("{}-client.log", id));
                if let Ok(file) = std::fs::OpenOptions::new()
                    .create(true)
                    .write(true)
                    .append(true)
                    .open(path)
                {
                    files.insert(id.clone(), file);
                }
            }
            if let Some(f) = files.get_mut(&id) {
                let _ = f.write_all(log_line.as_bytes());
            }
        } else {
            // file
            if let Ok(mut f) = self.default_file.lock() {
                let _ = f.write_all(log_line.as_bytes());
            }
        }
        self.flush();
    }

    fn flush(&self) {
        let _ = self.default_file.lock().map(|mut f| f.flush());
        let mut files = self.files.lock().unwrap();
        for f in files.values_mut() {
            let _ = f.flush();
        }
    }
}
