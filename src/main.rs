use crossbeam_channel::{bounded, Receiver, Sender};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::{WalkBuilder, WalkState};
use regex::Regex;
use std::cmp::Ordering;
use std::env;
use std::fs;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::thread;

#[derive(Clone, Copy)]
enum SortKey {
    Size,
    Path,
}

#[derive(Clone)]
struct Filters {
    min_size: Option<u64>,
    max_size: Option<u64>,
    exclude_set: Option<GlobSet>,
    regex_filter: Option<Regex>,
}

impl Filters {
    fn matches(&self, path: &Path, size: u64) -> bool {
        let path_text = path.to_string_lossy();
        let normalized_path_text = path_text.strip_prefix("./").unwrap_or(&path_text);

        if let Some(min_size) = self.min_size {
            if size < min_size {
                return false;
            }
        }

        if let Some(max_size) = self.max_size {
            if size > max_size {
                return false;
            }
        }

        if let Some(exclude_set) = &self.exclude_set {
            if exclude_set.is_match(path) || exclude_set.is_match(normalized_path_text) {
                return false;
            }
        }

        if let Some(regex_filter) = &self.regex_filter {
            if !regex_filter.is_match(&path_text) && !regex_filter.is_match(normalized_path_text) {
                return false;
            }
        }

        true
    }
}

struct Config {
    path: PathBuf,
    threads: usize,
    human: bool,
    sort: Option<SortKey>,
    desc: bool,
    show_hidden: bool,
    top: Option<usize>,
    fast: bool,
    filters: Filters,
}

struct FileEntry {
    path: PathBuf,
    size: u64,
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
        "Usage: {bin} [PATH] [OPTIONS]\n\n\
         Options:\n\
         --path PATH            Path to scan (same as positional PATH)\n\
         --threads auto|N       Worker count (default: auto)\n\
         --human                Print human-readable sizes\n\
         --min-size SIZE        Include only files >= SIZE\n\
         --max-size SIZE        Include only files <= SIZE\n\
         --sort size|path       Sort output\n\
         --desc                 Descending sort\n\
         --no-hidden            Exclude hidden files\n\
         --top N                Keep first N results (defaults to size desc if no --sort)\n\
         --exclude GLOB         Exclude paths by glob (repeatable)\n\
         --regex-filter REGEX   Keep paths that match REGEX\n\
         --fast                 Stream output for max throughput (disables --sort/--top)\n\
         -h, --help             Show help\n\n\
         SIZE suffixes: B, KB, MB, GB, TB, PB (also KiB/MiB/GiB/TiB/PiB)\n\
         Output format: <path>\\t<size>"
    )
}

fn parse_threads(value: &str) -> Result<usize, String> {
    if value.eq_ignore_ascii_case("auto") {
        return Ok(default_threads());
    }

    value
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| "invalid --threads value, expected auto or integer > 0".to_string())
}

fn parse_sort(value: &str) -> Result<SortKey, String> {
    match value {
        "size" => Ok(SortKey::Size),
        "path" => Ok(SortKey::Path),
        _ => Err("invalid --sort value, expected size|path".to_string()),
    }
}

fn parse_size(value: &str) -> Result<u64, String> {
    let text = value.trim();
    if text.is_empty() {
        return Err("size cannot be empty".to_string());
    }

    let split = text
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(text.len());
    if split == 0 {
        return Err(format!("invalid size '{text}'"));
    }

    let number = text[..split]
        .parse::<u64>()
        .map_err(|_| format!("invalid size number '{}'", &text[..split]))?;

    let unit = text[split..].trim().to_ascii_lowercase();
    let multiplier: u64 = match unit.as_str() {
        "" | "b" => 1,
        "k" | "kb" | "ki" | "kib" => 1024,
        "m" | "mb" | "mi" | "mib" => 1024_u64.pow(2),
        "g" | "gb" | "gi" | "gib" => 1024_u64.pow(3),
        "t" | "tb" | "ti" | "tib" => 1024_u64.pow(4),
        "p" | "pb" | "pi" | "pib" => 1024_u64.pow(5),
        _ => return Err(format!("invalid size suffix in '{text}'")),
    };

    number
        .checked_mul(multiplier)
        .ok_or_else(|| format!("size overflow for '{text}'"))
}

fn parse_positive_usize(value: &str, flag: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|n| *n > 0)
        .ok_or_else(|| format!("invalid {flag} value, expected integer > 0"))
}

fn parse_args() -> ArgParse {
    let mut args = env::args_os();
    let bin = args
        .next()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "files-list".to_string());

    let mut path: Option<PathBuf> = None;
    let mut threads: Option<usize> = None;
    let mut human = false;
    let mut min_size: Option<u64> = None;
    let mut max_size: Option<u64> = None;
    let mut sort: Option<SortKey> = None;
    let mut desc = false;
    let mut desc_set = false;
    let mut show_hidden = true;
    let mut top: Option<usize> = None;
    let mut excludes: Vec<String> = Vec::new();
    let mut fast = false;
    let mut regex_filter: Option<String> = None;

    while let Some(arg) = args.next() {
        let s = arg.to_string_lossy();

        if s == "-h" || s == "--help" {
            eprintln!("{}", usage(&bin));
            return ArgParse::Help;
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

        if s == "-t" || s == "--threads" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --threads".to_string());
            };
            let parsed = parse_threads(&value.to_string_lossy());
            match parsed {
                Ok(n) => {
                    threads = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if let Some(value) = s.strip_prefix("--threads=") {
            match parse_threads(value) {
                Ok(n) => {
                    threads = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if s == "--human" {
            human = true;
            continue;
        }

        if s == "--min-size" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --min-size".to_string());
            };
            match parse_size(&value.to_string_lossy()) {
                Ok(n) => {
                    min_size = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if let Some(value) = s.strip_prefix("--min-size=") {
            match parse_size(value) {
                Ok(n) => {
                    min_size = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if s == "--max-size" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --max-size".to_string());
            };
            match parse_size(&value.to_string_lossy()) {
                Ok(n) => {
                    max_size = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if let Some(value) = s.strip_prefix("--max-size=") {
            match parse_size(value) {
                Ok(n) => {
                    max_size = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if s == "--sort" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --sort".to_string());
            };
            match parse_sort(&value.to_string_lossy()) {
                Ok(key) => {
                    sort = Some(key);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if let Some(value) = s.strip_prefix("--sort=") {
            match parse_sort(value) {
                Ok(key) => {
                    sort = Some(key);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if s == "--desc" {
            desc = true;
            desc_set = true;
            continue;
        }

        if s == "--no-hidden" {
            show_hidden = false;
            continue;
        }

        if s == "--top" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --top".to_string());
            };
            match parse_positive_usize(&value.to_string_lossy(), "--top") {
                Ok(n) => {
                    top = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if let Some(value) = s.strip_prefix("--top=") {
            match parse_positive_usize(value, "--top") {
                Ok(n) => {
                    top = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if s == "--exclude" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --exclude".to_string());
            };
            let value = value.to_string_lossy();
            let mut found = false;
            for pattern in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                excludes.push(pattern.to_string());
                found = true;
            }
            if !found {
                return ArgParse::Err("empty --exclude pattern".to_string());
            }
            continue;
        }

        if let Some(value) = s.strip_prefix("--exclude=") {
            let mut found = false;
            for pattern in value.split(',').map(str::trim).filter(|p| !p.is_empty()) {
                excludes.push(pattern.to_string());
                found = true;
            }
            if !found {
                return ArgParse::Err("empty --exclude pattern".to_string());
            }
            continue;
        }

        if s == "--regex-filter" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --regex-filter".to_string());
            };
            regex_filter = Some(value.to_string_lossy().to_string());
            continue;
        }

        if let Some(value) = s.strip_prefix("--regex-filter=") {
            regex_filter = Some(value.to_string());
            continue;
        }

        if s == "--fast" {
            fast = true;
            continue;
        }

        if s.starts_with('-') {
            return ArgParse::Err(format!("unknown option: {s}"));
        }

        if path.is_none() {
            path = Some(PathBuf::from(arg));
            continue;
        }

        return ArgParse::Err("only one PATH argument is supported".to_string());
    }

    if let (Some(min), Some(max)) = (min_size, max_size) {
        if min > max {
            return ArgParse::Err("--min-size cannot be larger than --max-size".to_string());
        }
    }

    if top.is_some() && sort.is_none() {
        sort = Some(SortKey::Size);
        if !desc_set {
            desc = true;
        }
    }

    if desc && sort.is_none() {
        return ArgParse::Err("--desc requires --sort or --top".to_string());
    }

    if fast && (sort.is_some() || top.is_some()) {
        return ArgParse::Err("--fast cannot be used with --sort or --top".to_string());
    }

    let exclude_set = if excludes.is_empty() {
        None
    } else {
        let mut builder = GlobSetBuilder::new();
        for pattern in excludes {
            let glob = match Glob::new(&pattern) {
                Ok(glob) => glob,
                Err(err) => {
                    return ArgParse::Err(format!("invalid --exclude pattern '{pattern}': {err}"));
                }
            };
            builder.add(glob);
        }
        match builder.build() {
            Ok(set) => Some(set),
            Err(err) => return ArgParse::Err(format!("failed to build exclude set: {err}")),
        }
    };

    let regex_filter = match regex_filter {
        Some(pattern) => match Regex::new(&pattern) {
            Ok(regex) => Some(regex),
            Err(err) => return ArgParse::Err(format!("invalid --regex-filter: {err}")),
        },
        None => None,
    };

    ArgParse::Ok(Config {
        path: path.unwrap_or_else(|| PathBuf::from(".")),
        threads: threads.unwrap_or_else(default_threads),
        human,
        sort,
        desc,
        show_hidden,
        top,
        fast,
        filters: Filters {
            min_size,
            max_size,
            exclude_set,
            regex_filter,
        },
    })
}

fn human_size(size: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    if size < 1024 {
        return format!("{size} B");
    }

    let mut value = size as f64;
    let mut unit = 0usize;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    format!("{value:.2} {}", UNITS[unit])
}

fn write_entry(
    out: &mut BufWriter<io::StdoutLock<'_>>,
    entry: &FileEntry,
    human: bool,
) -> io::Result<()> {
    if human {
        writeln!(out, "{}\t{}", entry.path.display(), human_size(entry.size))
    } else {
        writeln!(out, "{}\t{}", entry.path.display(), entry.size)
    }
}

fn write_single_file(path: &Path, size: u64, human: bool) -> io::Result<()> {
    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());
    let entry = FileEntry {
        path: path.to_path_buf(),
        size,
    };
    write_entry(&mut out, &entry, human)?;
    out.flush()
}

fn write_stream(rx: Receiver<FileEntry>, human: bool, fast: bool) -> io::Result<()> {
    let stdout = io::stdout();
    let buffer_size = if fast { 1 << 22 } else { 1 << 20 };
    let mut out = BufWriter::with_capacity(buffer_size, stdout.lock());
    for entry in rx {
        write_entry(&mut out, &entry, human)?;
    }
    out.flush()
}

fn write_all(entries: &[FileEntry], human: bool) -> io::Result<()> {
    let stdout = io::stdout();
    let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());
    for entry in entries {
        write_entry(&mut out, entry, human)?;
    }
    out.flush()
}

fn walk_send(
    root: &Path,
    threads: usize,
    show_hidden: bool,
    filters: Arc<Filters>,
    tx: Sender<FileEntry>,
) {
    let mut builder = WalkBuilder::new(root);
    builder.standard_filters(false);
    builder.hidden(!show_hidden);
    builder.threads(threads);

    let walker = builder.build_parallel();
    walker.run(move || {
        let tx = tx.clone();
        let filters = Arc::clone(&filters);
        Box::new(move |entry| {
            let dent = match entry {
                Ok(dent) => dent,
                Err(_) => return WalkState::Continue,
            };

            if !dent.file_type().map(|ft| ft.is_file()).unwrap_or(false) {
                return WalkState::Continue;
            }

            let size = match dent.metadata() {
                Ok(meta) => meta.len(),
                Err(_) => return WalkState::Continue,
            };

            let path = dent.into_path();
            if !filters.matches(&path, size) {
                return WalkState::Continue;
            }

            if tx.send(FileEntry { path, size }).is_err() {
                return WalkState::Quit;
            }

            WalkState::Continue
        })
    });
}

fn compare_entries(a: &FileEntry, b: &FileEntry, sort_key: SortKey, desc: bool) -> Ordering {
    let ord = match sort_key {
        SortKey::Size => a.size.cmp(&b.size).then_with(|| a.path.cmp(&b.path)),
        SortKey::Path => a.path.cmp(&b.path).then_with(|| a.size.cmp(&b.size)),
    };
    if desc {
        ord.reverse()
    } else {
        ord
    }
}

fn run_streaming(config: &Config) -> Result<(), String> {
    let channel_size = if config.fast { 65_536 } else { 16_384 };
    let (tx, rx) = bounded::<FileEntry>(channel_size);
    let human = config.human;
    let fast = config.fast;
    let writer_handle = thread::spawn(move || write_stream(rx, human, fast));

    walk_send(
        &config.path,
        config.threads,
        config.show_hidden,
        Arc::new(config.filters.clone()),
        tx,
    );

    match writer_handle.join() {
        Ok(Ok(())) => Ok(()),
        Ok(Err(err)) => Err(format!("failed to write output: {err}")),
        Err(_) => Err("writer thread panicked".to_string()),
    }
}

fn run_sorted(config: &Config) -> Result<(), String> {
    let (tx, rx) = bounded::<FileEntry>(16_384);
    let collect_handle = thread::spawn(move || rx.into_iter().collect::<Vec<FileEntry>>());

    walk_send(
        &config.path,
        config.threads,
        config.show_hidden,
        Arc::new(config.filters.clone()),
        tx,
    );

    let mut entries = match collect_handle.join() {
        Ok(entries) => entries,
        Err(_) => return Err("collector thread panicked".to_string()),
    };

    if let Some(sort_key) = config.sort {
        entries.sort_unstable_by(|a, b| compare_entries(a, b, sort_key, config.desc));
    }

    if let Some(top_n) = config.top {
        if entries.len() > top_n {
            entries.truncate(top_n);
        }
    }

    write_all(&entries, config.human).map_err(|err| format!("failed to write output: {err}"))
}

fn run(config: Config) -> Result<(), String> {
    let meta = fs::metadata(&config.path)
        .map_err(|err| format!("cannot access '{}': {err}", config.path.display()))?;

    if meta.is_file() {
        let size = meta.len();
        if config.filters.matches(&config.path, size) {
            write_single_file(&config.path, size, config.human)
                .map_err(|err| format!("failed to write output: {err}"))?;
        }
        return Ok(());
    }

    if !meta.is_dir() {
        return Err(format!(
            "path '{}' is neither a file nor directory",
            config.path.display()
        ));
    }

    if config.sort.is_some() || config.top.is_some() {
        run_sorted(&config)
    } else {
        run_streaming(&config)
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
