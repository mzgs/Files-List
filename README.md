# Files-List

Fast Rust CLI to scan a path and print file paths with file sizes.

The scanner is parallel by default and optimized for large directory trees.

## Features

- Parallel filesystem traversal (`--threads auto|N`)
- File size filters (`--min-size`, `--max-size`)
- Path filters (`--exclude` glob, `--regex-filter`)
- Hidden file control (`--no-hidden`)
- CSV export (`--export-csv`)
- Sorting and top-N (`--sort`, `--desc`, `--top`)
- Human-readable size output (`--human`)
- Max-throughput streaming mode (`--fast`)

## Build

```bash
cargo build --release
```

Binary path:

```bash
./target/release/files-list
```

## Quick Start

Scan current directory:

```bash
./target/release/files-list --path .
```

Or with positional path:

```bash
./target/release/files-list .
```

## Output Format

Default stdout output is tab-separated:

```text
<path>\t<size>
```

Examples:

```text
./src/main.rs	20137
./README.md	4120
```

With `--human`, size becomes human-readable:

```text
./src/main.rs	19.66 KiB
./README.md	4.02 KiB
```

With `--export-csv`, output is written to CSV and nothing is printed to terminal.
CSV format:

```text
path,size
./src/main.rs,20137
./README.md,4120
```

## CLI Reference

Usage:

```bash
files-list [PATH] [OPTIONS]
```

Options:

| Option | Description | Default |
|---|---|---|
| `--path PATH` | Path to scan (same as positional `PATH`) | `.` |
| `--threads auto\|N` | Worker thread count | `auto` |
| `--human` | Print human-readable sizes | off |
| `--export-csv FILE` | Write results to CSV file (no stdout output) | none |
| `--min-size SIZE` | Include only files `>= SIZE` | none |
| `--max-size SIZE` | Include only files `<= SIZE` | none |
| `--sort size\|path` | Sort output by key | none |
| `--desc` | Descending sort order | ascending |
| `--no-hidden` | Exclude hidden files | hidden files are included by default |
| `--top N` | Keep only first `N` rows after sorting | none |
| `--exclude GLOB` | Exclude paths by glob (repeatable) | none |
| `--regex-filter REGEX` | Keep only matching paths | none |
| `--fast` | Stream output for max throughput | off |
| `-h`, `--help` | Show help | n/a |

## Size Syntax

Accepted units for `--min-size` and `--max-size`:

- `B`
- `KB`, `MB`, `GB`, `TB`, `PB`
- `KiB`, `MiB`, `GiB`, `TiB`, `PiB`

Examples:

```bash
--min-size 100
--min-size 100B
--min-size 64KB
--max-size 2GiB
```

## Examples

Show human-readable sizes:

```bash
./target/release/files-list --path . --human
```

Export to CSV (no terminal output):

```bash
./target/release/files-list --path . --export-csv files.csv
```

Filter by size range:

```bash
./target/release/files-list --path . --min-size 10MB --max-size 2GB
```

Sort by size descending and keep top 20:

```bash
./target/release/files-list --path . --sort size --desc --top 20
```

Sort by path:

```bash
./target/release/files-list --path . --sort path
```

Exclude hidden files:

```bash
./target/release/files-list --path . --no-hidden
```

Exclude multiple path patterns:

```bash
./target/release/files-list --path . \
  --exclude 'target/**' \
  --exclude '.git/**' \
  --exclude '**/*.log'
```

Use regex path filter:

```bash
./target/release/files-list --path . --regex-filter 'src|Cargo'
```

Control parallelism:

```bash
./target/release/files-list --path . --threads auto
./target/release/files-list --path . --threads 16
```

Fast streaming mode:

```bash
./target/release/files-list --path /data --fast
```

## Behavior Notes

- Hidden files are included by default.
- `--export-csv FILE` writes to CSV and suppresses stdout output.
- `--top N` without `--sort` automatically uses `--sort size --desc`.
- `--desc` requires `--sort` or `--top`.
- `--fast` cannot be used with `--sort` or `--top`.
- If `--min-size` is larger than `--max-size`, command fails.

## Exit Codes

- `0` on success
- `1` on argument/runtime error
