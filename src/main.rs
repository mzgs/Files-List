use crossbeam_channel::bounded;
use ignore::{WalkBuilder, WalkState};
use std::env;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::thread;

struct Config {
    path: PathBuf,
    threads: usize,
}

enum ArgParse {
    Ok(Config),
    Help,
    Err(String),
}

fn default_threads() -> usize {
    thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

fn usage(bin: &str) -> String {
    format!(
        "Usage: {bin} [PATH] [--path PATH] [--threads N]\n\n\
         Lists files under PATH with their byte sizes.\n\
         Output format: <path>\\t<size_bytes>"
    )
}

fn parse_args() -> ArgParse {
    let mut args = env::args_os();
    let bin = args
        .next()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "files-list".to_string());

    let mut path: Option<PathBuf> = None;
    let mut threads: Option<usize> = None;

    while let Some(arg) = args.next() {
        let s = arg.to_string_lossy();

        if s == "-h" || s == "--help" {
            eprintln!("{}", usage(&bin));
            return ArgParse::Help;
        }

        if s == "-t" || s == "--threads" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --threads".to_string());
            };
            let parsed = value
                .to_string_lossy()
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0);
            if let Some(n) = parsed {
                threads = Some(n);
                continue;
            }
            return ArgParse::Err("invalid --threads value, expected integer > 0".to_string());
        }

        if let Some(value) = s.strip_prefix("--threads=") {
            let parsed = value.parse::<usize>().ok().filter(|n| *n > 0);
            if let Some(n) = parsed {
                threads = Some(n);
                continue;
            }
            return ArgParse::Err("invalid --threads value, expected integer > 0".to_string());
        }

        if s == "-p" || s == "--path" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --path".to_string());
            };
            if path.is_none() {
                path = Some(PathBuf::from(value));
                continue;
            }
            return ArgParse::Err("path provided multiple times".to_string());
        }

        if let Some(value) = s.strip_prefix("--path=") {
            if path.is_none() {
                path = Some(PathBuf::from(value));
                continue;
            }
            return ArgParse::Err("path provided multiple times".to_string());
        }

        if s.starts_with('-') {
            return ArgParse::Err(format!("unknown option: {s}"));
        }

        if path.is_none() {
            path = Some(PathBuf::from(arg));
        } else {
            return ArgParse::Err("only one PATH argument is supported".to_string());
        }
    }

    ArgParse::Ok(Config {
        path: path.unwrap_or_else(|| PathBuf::from(".")),
        threads: threads.unwrap_or_else(default_threads),
    })
}

fn write_single_file(path: &Path, size: u64) -> io::Result<()> {
    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());
    writeln!(out, "{}\t{}", path.display(), size)?;
    out.flush()
}

fn writer(rx: crossbeam_channel::Receiver<(PathBuf, u64)>) -> io::Result<()> {
    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());
    for (path, size) in rx {
        writeln!(out, "{}\t{}", path.display(), size)?;
    }
    out.flush()
}

fn run(config: Config) -> Result<(), String> {
    let meta = fs::metadata(&config.path)
        .map_err(|e| format!("cannot access '{}': {e}", config.path.display()))?;

    if meta.is_file() {
        return write_single_file(&config.path, meta.len())
            .map_err(|e| format!("failed to write output: {e}"));
    }

    if !meta.is_dir() {
        return Err(format!(
            "path '{}' is neither a file nor directory",
            config.path.display()
        ));
    }

    let (tx, rx) = bounded::<(PathBuf, u64)>(16_384);
    let writer_handle = thread::spawn(move || writer(rx));

    let mut builder = WalkBuilder::new(&config.path);
    builder.standard_filters(false);
    builder.threads(config.threads);

    let walker = builder.build_parallel();
    walker.run(move || {
        let tx = tx.clone();
        Box::new(move |entry| {
            let dent = match entry {
                Ok(dent) => dent,
                Err(_) => return WalkState::Continue,
            };

            let is_file = dent.file_type().map(|ft| ft.is_file()).unwrap_or(false);
            if !is_file {
                return WalkState::Continue;
            }

            let size = match dent.metadata() {
                Ok(meta) => meta.len(),
                Err(_) => return WalkState::Continue,
            };

            if tx.send((dent.into_path(), size)).is_err() {
                return WalkState::Quit;
            }

            WalkState::Continue
        })
    });

    match writer_handle.join() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(format!("failed to write output: {e}")),
        Err(_) => Err("writer thread panicked".to_string()),
    }
}

fn main() -> ExitCode {
    match parse_args() {
        ArgParse::Ok(config) => match run(config) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                eprintln!("error: {err}");
                ExitCode::FAILURE
            }
        },
        ArgParse::Help => ExitCode::SUCCESS,
        ArgParse::Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}
