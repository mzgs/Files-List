use crossbeam_channel::{bounded, Receiver, Sender};
use globset::{Glob, GlobSet, GlobSetBuilder};
use ignore::{WalkBuilder, WalkState};
use regex::Regex;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::env;
use std::fs::{self, File};
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
    fn is_excluded_path(&self, path: &Path) -> bool {
        let path_text = path.to_string_lossy();
        let normalized_path_text = path_text.strip_prefix("./").unwrap_or(&path_text);

        if let Some(exclude_set) = &self.exclude_set {
            exclude_set.is_match(path) || exclude_set.is_match(normalized_path_text)
        } else {
            false
        }
    }

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

        if self.is_excluded_path(path) {
            return false;
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
    depth: Option<usize>,
    human: bool,
    output: OutputTarget,
    sort: Option<SortKey>,
    desc: bool,
    show_hidden: bool,
    top: Option<usize>,
    fast: bool,
    include_folders: bool,
    folders_only: bool,
    filters: Filters,
}

#[derive(Clone)]
enum OutputTarget {
    Stdout,
    Csv(PathBuf),
}

struct FileEntry {
    path: PathBuf,
    size: u64,
    is_dir: bool,
}

enum ScannedEntry {
    File(FileEntry),
    Dir(PathBuf),
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
         --depth N              Max depth (1 = direct children; with --folders-only, limits folder output depth)\n\
         --human                Print human-readable sizes\n\
         --export-csv FILE      Export result to CSV file (no stdout output)\n\
         --min-size SIZE        Include only files >= SIZE\n\
         --max-size SIZE        Include only files <= SIZE\n\
         --sort size|path       Sort output\n\
         --desc                 Descending sort\n\
         --no-hidden            Exclude hidden files\n\
         --top N                Keep first N results (defaults to size desc if no --sort)\n\
         --folders              Include folders with recursive total sizes\n\
         --folders-only         Show only folders with recursive total sizes\n\
         --exclude GLOB         Exclude paths by glob (repeatable)\n\
         --exclude-cloud        Exclude iCloud/Google Drive/Dropbox paths on macOS\n\
         --regex-filter REGEX   Keep paths that match REGEX\n\
         --fast                 Stream output for max throughput (disables --sort/--top)\n\
         -h, --help             Show help\n\n\
         SIZE suffixes: B, KB, MB, GB, TB, PB (also KiB/MiB/GiB/TiB/PiB)\n\
         Stdout output format: <path>\\t<size>"
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

fn cloud_exclude_patterns() -> Vec<String> {
    let mut patterns = vec![
        "/Volumes/GoogleDrive*".to_string(),
        "/Volumes/GoogleDrive*/**".to_string(),
        "/Volumes/Dropbox*".to_string(),
        "/Volumes/Dropbox*/**".to_string(),
        "/Volumes/iCloud*".to_string(),
        "/Volumes/iCloud*/**".to_string(),
        "**/Library/CloudStorage/GoogleDrive*".to_string(),
        "**/Library/CloudStorage/GoogleDrive*/**".to_string(),
        "**/Library/CloudStorage/Dropbox*".to_string(),
        "**/Library/CloudStorage/Dropbox*/**".to_string(),
        "**/Library/CloudStorage/iCloud*".to_string(),
        "**/Library/CloudStorage/iCloud*/**".to_string(),
        "**/Library/Mobile Documents/com~apple~CloudDocs".to_string(),
        "**/Library/Mobile Documents/com~apple~CloudDocs/**".to_string(),
    ];

    if let Ok(home) = env::var("HOME") {
        patterns.push(format!("{home}/Library/CloudStorage/GoogleDrive*"));
        patterns.push(format!("{home}/Library/CloudStorage/GoogleDrive*/**"));
        patterns.push(format!("{home}/Library/CloudStorage/Dropbox*"));
        patterns.push(format!("{home}/Library/CloudStorage/Dropbox*/**"));
        patterns.push(format!("{home}/Library/CloudStorage/iCloud*"));
        patterns.push(format!("{home}/Library/CloudStorage/iCloud*/**"));
        patterns.push(format!(
            "{home}/Library/Mobile Documents/com~apple~CloudDocs"
        ));
        patterns.push(format!(
            "{home}/Library/Mobile Documents/com~apple~CloudDocs/**"
        ));
        patterns.push(format!("{home}/Dropbox"));
        patterns.push(format!("{home}/Dropbox/**"));
        patterns.push(format!("{home}/Google Drive"));
        patterns.push(format!("{home}/Google Drive/**"));
    }

    patterns
}

fn parse_args() -> ArgParse {
    let mut args = env::args_os();
    let bin = args
        .next()
        .and_then(|s| s.into_string().ok())
        .unwrap_or_else(|| "files-list".to_string());

    let mut path: Option<PathBuf> = None;
    let mut threads: Option<usize> = None;
    let mut depth: Option<usize> = None;
    let mut human = false;
    let mut export_csv: Option<PathBuf> = None;
    let mut min_size: Option<u64> = None;
    let mut max_size: Option<u64> = None;
    let mut sort: Option<SortKey> = None;
    let mut desc = false;
    let mut desc_set = false;
    let mut show_hidden = true;
    let mut top: Option<usize> = None;
    let mut include_folders = false;
    let mut folders_only = false;
    let mut excludes: Vec<String> = Vec::new();
    let mut exclude_cloud = false;
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

        if s == "--depth" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --depth".to_string());
            };
            match parse_positive_usize(&value.to_string_lossy(), "--depth") {
                Ok(n) => {
                    depth = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if let Some(value) = s.strip_prefix("--depth=") {
            match parse_positive_usize(value, "--depth") {
                Ok(n) => {
                    depth = Some(n);
                    continue;
                }
                Err(err) => return ArgParse::Err(err),
            }
        }

        if s == "--human" {
            human = true;
            continue;
        }

        if s == "--export-csv" {
            let Some(value) = args.next() else {
                return ArgParse::Err("missing value for --export-csv".to_string());
            };
            let value = value.to_string_lossy();
            if value.trim().is_empty() {
                return ArgParse::Err("empty value for --export-csv".to_string());
            }
            if export_csv.is_some() {
                return ArgParse::Err("--export-csv provided multiple times".to_string());
            }
            export_csv = Some(PathBuf::from(value.as_ref()));
            continue;
        }

        if let Some(value) = s.strip_prefix("--export-csv=") {
            if value.trim().is_empty() {
                return ArgParse::Err("empty value for --export-csv".to_string());
            }
            if export_csv.is_some() {
                return ArgParse::Err("--export-csv provided multiple times".to_string());
            }
            export_csv = Some(PathBuf::from(value));
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

        if s == "--folders" {
            include_folders = true;
            continue;
        }

        if s == "--folders-only" {
            folders_only = true;
            include_folders = true;
            continue;
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

        if s == "--exclude-cloud" {
            exclude_cloud = true;
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

    if fast && include_folders {
        return ArgParse::Err("--fast cannot be used with --folders or --folders-only".to_string());
    }

    if exclude_cloud {
        excludes.extend(cloud_exclude_patterns());
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
        depth,
        human,
        output: export_csv
            .map(OutputTarget::Csv)
            .unwrap_or(OutputTarget::Stdout),
        sort,
        desc,
        show_hidden,
        top,
        fast,
        include_folders,
        folders_only,
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

fn size_text(size: u64, human: bool) -> String {
    if human {
        human_size(size)
    } else {
        size.to_string()
    }
}

fn entry_path_text(entry: &FileEntry) -> String {
    let mut text = entry.path.to_string_lossy().into_owned();
    if entry.is_dir && !text.ends_with(std::path::MAIN_SEPARATOR) {
        text.push(std::path::MAIN_SEPARATOR);
    }
    text
}

fn write_text_entry<W: Write>(out: &mut W, entry: &FileEntry, human: bool) -> io::Result<()> {
    writeln!(
        out,
        "{}\t{}",
        entry_path_text(entry),
        size_text(entry.size, human)
    )
}

fn write_csv_field<W: Write>(out: &mut W, value: &str) -> io::Result<()> {
    let needs_quotes = value
        .bytes()
        .any(|b| matches!(b, b',' | b'"' | b'\n' | b'\r'));
    if !needs_quotes {
        return out.write_all(value.as_bytes());
    }

    out.write_all(b"\"")?;
    for ch in value.chars() {
        if ch == '"' {
            out.write_all(b"\"\"")?;
        } else {
            write!(out, "{ch}")?;
        }
    }
    out.write_all(b"\"")
}

fn write_csv_entry<W: Write>(out: &mut W, entry: &FileEntry, human: bool) -> io::Result<()> {
    let path_text = entry_path_text(entry);
    let size = size_text(entry.size, human);

    write_csv_field(out, &path_text)?;
    out.write_all(b",")?;
    write_csv_field(out, &size)?;
    out.write_all(b"\n")
}

fn write_csv_header<W: Write>(out: &mut W) -> io::Result<()> {
    out.write_all(b"path,size\n")
}

fn write_stream(
    rx: Receiver<FileEntry>,
    human: bool,
    fast: bool,
    output: OutputTarget,
) -> io::Result<()> {
    let buffer_size = if fast { 1 << 22 } else { 1 << 20 };

    match output {
        OutputTarget::Stdout => {
            let stdout = io::stdout();
            let mut out = BufWriter::with_capacity(buffer_size, stdout.lock());
            for entry in rx {
                write_text_entry(&mut out, &entry, human)?;
            }
            out.flush()
        }
        OutputTarget::Csv(path) => {
            let file = File::create(&path)?;
            let mut out = BufWriter::with_capacity(buffer_size, file);
            write_csv_header(&mut out)?;
            for entry in rx {
                write_csv_entry(&mut out, &entry, human)?;
            }
            out.flush()
        }
    }
}

fn write_all(entries: &[FileEntry], human: bool, output: &OutputTarget) -> io::Result<()> {
    match output {
        OutputTarget::Stdout => {
            let stdout = io::stdout();
            let mut out = BufWriter::with_capacity(1 << 20, stdout.lock());
            for entry in entries {
                write_text_entry(&mut out, entry, human)?;
            }
            out.flush()
        }
        OutputTarget::Csv(path) => {
            let file = File::create(path)?;
            let mut out = BufWriter::with_capacity(1 << 20, file);
            write_csv_header(&mut out)?;
            for entry in entries {
                write_csv_entry(&mut out, entry, human)?;
            }
            out.flush()
        }
    }
}

fn walk_send(
    root: &Path,
    threads: usize,
    depth: Option<usize>,
    show_hidden: bool,
    filters: Arc<Filters>,
    tx: Sender<FileEntry>,
) {
    let mut builder = WalkBuilder::new(root);
    builder.standard_filters(false);
    builder.hidden(!show_hidden);
    builder.threads(threads);
    builder.max_depth(depth);

    let walker = builder.build_parallel();
    walker.run(move || {
        let tx = tx.clone();
        let filters = Arc::clone(&filters);
        Box::new(move |entry| {
            let dent = match entry {
                Ok(dent) => dent,
                Err(_) => return WalkState::Continue,
            };

            let is_dir = dent.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
            if filters.is_excluded_path(dent.path()) {
                return if is_dir {
                    WalkState::Skip
                } else {
                    WalkState::Continue
                };
            }

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

            if tx
                .send(FileEntry {
                    path,
                    size,
                    is_dir: false,
                })
                .is_err()
            {
                return WalkState::Quit;
            }

            WalkState::Continue
        })
    });
}

fn walk_send_with_dirs(
    root: &Path,
    threads: usize,
    depth: Option<usize>,
    show_hidden: bool,
    filters: Arc<Filters>,
    tx: Sender<ScannedEntry>,
) {
    let mut builder = WalkBuilder::new(root);
    builder.standard_filters(false);
    builder.hidden(!show_hidden);
    builder.threads(threads);
    builder.max_depth(depth);

    let walker = builder.build_parallel();
    walker.run(move || {
        let tx = tx.clone();
        let filters = Arc::clone(&filters);
        Box::new(move |entry| {
            let dent = match entry {
                Ok(dent) => dent,
                Err(_) => return WalkState::Continue,
            };

            let is_dir = dent.file_type().map(|ft| ft.is_dir()).unwrap_or(false);
            if filters.is_excluded_path(dent.path()) {
                return if is_dir {
                    WalkState::Skip
                } else {
                    WalkState::Continue
                };
            }

            if is_dir {
                if tx
                    .send(ScannedEntry::Dir(dent.path().to_path_buf()))
                    .is_err()
                {
                    return WalkState::Quit;
                }
                return WalkState::Continue;
            }

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

            if tx
                .send(ScannedEntry::File(FileEntry {
                    path,
                    size,
                    is_dir: false,
                }))
                .is_err()
            {
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

fn path_depth_from_root(root: &Path, path: &Path) -> Option<usize> {
    path.strip_prefix(root).ok().map(|p| p.components().count())
}

fn build_folder_totals(
    root: &Path,
    files: &[FileEntry],
    dirs: Vec<PathBuf>,
    folder_depth_limit: Option<usize>,
    filters: &Filters,
) -> Vec<FileEntry> {
    let mut dir_set: HashSet<PathBuf> = dirs.into_iter().collect();
    dir_set.insert(root.to_path_buf());

    let mut totals: HashMap<PathBuf, u64> = dir_set.iter().cloned().map(|dir| (dir, 0)).collect();

    for entry in files {
        let Some(parent) = entry.path.parent() else {
            continue;
        };
        if !parent.starts_with(root) {
            continue;
        }
        if !totals.contains_key(parent) {
            continue;
        }

        let parent = parent.to_path_buf();
        totals
            .entry(parent.clone())
            .and_modify(|total| *total = total.saturating_add(entry.size))
            .or_insert(entry.size);
    }

    let mut ordered_dirs: Vec<PathBuf> = totals.keys().cloned().collect();
    ordered_dirs.sort_unstable_by(|a, b| {
        b.components()
            .count()
            .cmp(&a.components().count())
            .then_with(|| a.cmp(b))
    });

    for dir in ordered_dirs {
        let subtotal = totals.get(&dir).copied().unwrap_or(0);

        let Some(parent) = dir.parent() else {
            continue;
        };
        if !parent.starts_with(root) {
            continue;
        }
        if !totals.contains_key(parent) {
            continue;
        }

        let parent = parent.to_path_buf();
        totals
            .entry(parent.clone())
            .and_modify(|total| *total = total.saturating_add(subtotal))
            .or_insert(subtotal);
    }

    totals
        .into_iter()
        .filter_map(|(path, size)| {
            if let Some(limit) = folder_depth_limit {
                let depth = path_depth_from_root(root, &path)?;
                if depth > limit {
                    return None;
                }
            }
            if filters.matches(&path, size) {
                Some(FileEntry {
                    path,
                    size,
                    is_dir: true,
                })
            } else {
                None
            }
        })
        .collect()
}

fn run_streaming(config: &Config) -> Result<(), String> {
    let channel_size = if config.fast { 65_536 } else { 16_384 };
    let (tx, rx) = bounded::<FileEntry>(channel_size);
    let human = config.human;
    let fast = config.fast;
    let output = config.output.clone();
    let writer_handle = thread::spawn(move || write_stream(rx, human, fast, output));

    walk_send(
        &config.path,
        config.threads,
        config.depth,
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
    let mut entries: Vec<FileEntry>;
    let mut folder_dirs: Vec<PathBuf> = Vec::new();

    if config.include_folders {
        let (tx, rx) = bounded::<ScannedEntry>(16_384);
        let collect_handle = thread::spawn(move || {
            let mut files = Vec::new();
            let mut dirs = Vec::new();
            for entry in rx {
                match entry {
                    ScannedEntry::File(file) => files.push(file),
                    ScannedEntry::Dir(dir) => dirs.push(dir),
                }
            }
            (files, dirs)
        });

        walk_send_with_dirs(
            &config.path,
            config.threads,
            if config.folders_only {
                None
            } else {
                config.depth
            },
            config.show_hidden,
            Arc::new(config.filters.clone()),
            tx,
        );

        (entries, folder_dirs) = match collect_handle.join() {
            Ok(collected) => collected,
            Err(_) => return Err("collector thread panicked".to_string()),
        };
    } else {
        let (tx, rx) = bounded::<FileEntry>(16_384);
        let collect_handle = thread::spawn(move || rx.into_iter().collect::<Vec<FileEntry>>());

        walk_send(
            &config.path,
            config.threads,
            config.depth,
            config.show_hidden,
            Arc::new(config.filters.clone()),
            tx,
        );

        entries = match collect_handle.join() {
            Ok(entries) => entries,
            Err(_) => return Err("collector thread panicked".to_string()),
        };
    }

    if config.include_folders {
        let folder_entries = build_folder_totals(
            &config.path,
            &entries,
            folder_dirs,
            if config.folders_only {
                config.depth
            } else {
                None
            },
            &config.filters,
        );
        if config.folders_only {
            entries = folder_entries;
        } else {
            entries.extend(folder_entries);
        }
    }

    if let Some(sort_key) = config.sort {
        entries.sort_unstable_by(|a, b| compare_entries(a, b, sort_key, config.desc));
    }

    if let Some(top_n) = config.top {
        if entries.len() > top_n {
            entries.truncate(top_n);
        }
    }

    write_all(&entries, config.human, &config.output)
        .map_err(|err| format!("failed to write output: {err}"))
}

fn run(config: Config) -> Result<(), String> {
    let meta = fs::metadata(&config.path)
        .map_err(|err| format!("cannot access '{}': {err}", config.path.display()))?;

    if meta.is_file() {
        if config.include_folders {
            return Err("--folders and --folders-only require PATH to be a directory".to_string());
        }

        let size = meta.len();
        let mut entries = Vec::new();
        if config.filters.matches(&config.path, size) {
            entries.push(FileEntry {
                path: config.path.clone(),
                size,
                is_dir: false,
            });
        }
        write_all(&entries, config.human, &config.output)
            .map_err(|err| format!("failed to write output: {err}"))?;
        return Ok(());
    }

    if !meta.is_dir() {
        return Err(format!(
            "path '{}' is neither a file nor directory",
            config.path.display()
        ));
    }

    if config.sort.is_some() || config.top.is_some() || config.include_folders {
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
