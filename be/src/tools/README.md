# Doris BE Meta Tool

`meta_tool` inspects and maintains Doris BE metadata. This document focuses on the read-only
segment inspection operations used for diagnosing and validating segment files.

## Build

Build the BE meta tool from the Doris repository root:

```bash
sh build.sh --be --meta-tool -j 32
```

The installed binary is normally available at:

```text
be/output/lib/meta_tool
```

If the loader reports `libjvm.so: cannot open shared object file`, add the JDK server library before
running the examples:

```bash
export LD_LIBRARY_PATH="${JAVA_HOME}/lib/server:${LD_LIBRARY_PATH:-}"
```

## Inspect a segment footer

```bash
be/output/lib/meta_tool \
  --operation=show_segment_footer \
  --file=/path/to/segment.dat
```

This parses and prints `SegmentFooterPB`. It does not decode column values.

## Check physical page CRCs

Doris segment pages use this physical trailer layout:

```text
page body | PageFooterPB | footer_size (4-byte little endian) | crc32c (4-byte little endian)
```

The stored CRC covers every byte except the final four-byte CRC itself. The page CRC operations
also validate that `footer_size` stays inside the supplied page range and that the footer parses as
a `PageFooterPB` with its required logical fields.

There are three related but different page checks:

| Check | Page locations come from | Intended use |
|---|---|---|
| `show_segment_data --scan_segment_pages=true` | Segment footer and ordinal indexes | Validate metadata-referenced data and dictionary pages while reading the segment normally. |
| `--operation=check_page_crc` | Caller-supplied `offset:size` values | Test known or corrected physical `PagePointer` candidates without trusting segment metadata. |
| `--operation=scan_page_crc` | A caller-supplied start and end offset | Derive consecutive page sizes from CRC trailers when the first page boundary is already known. |

Raw CRC operation flags:

| Flag | Default | Operation | Description |
|---|---:|---|---|
| `--file` | empty | both | Local segment path; required. |
| `--page_ranges` | empty | `check_page_crc` | Comma-separated decimal `OFFSET:SIZE` values. |
| `--page_ranges_file` | empty | `check_page_crc` | File containing decimal `OFFSET SIZE` pairs. |
| `--page_scan_start` | `0` | `scan_page_crc` | Inclusive known first-page offset; the flag must be explicitly supplied even when the value is zero. |
| `--page_scan_end` | `0` | `scan_page_crc` | Exclusive expected end offset; the flag must be explicitly supplied. |
| `--page_output` | `all` | both | Per-page output level: `all`, `errors`, or `summary`. |

### Check known page ranges

Use `--page_ranges` for a small number of decimal `OFFSET:SIZE` pairs:

```bash
be/output/lib/meta_tool \
  --operation=check_page_crc \
  --file=/path/to/segment.dat \
  --page_ranges=5242880:18349,32162899:2387
```

Whitespace around comma-separated ranges is accepted, but hexadecimal input is not. Ranges are
checked in their input order and duplicates are preserved. Every range is checked even if an
earlier range fails; the final exit status is non-zero when any range is invalid, unreadable, has a
bad CRC, or contains an invalid footer.

For a large list, use `--page_ranges_file` instead of `--page_ranges`:

```bash
be/output/lib/meta_tool \
  --operation=check_page_crc \
  --file=/path/to/segment.dat \
  --page_ranges_file=/path/to/page_ranges.txt
```

The ranges file contains one decimal `OFFSET SIZE` pair per line. Blank lines, leading/trailing
whitespace, full-line comments, and trailing comments are accepted:

```text
# column 1 data page 0
5242880 18349

32162899 2387  # column 1 ordinal root
```

Exactly one of `--page_ranges` and `--page_ranges_file` is required.

### Scan consecutive pages from a known boundary

Use `scan_page_crc` when the first candidate page offset and the expected end of a continuous page
range are known, but individual page sizes are not:

```bash
be/output/lib/meta_tool \
  --operation=scan_page_crc \
  --file=/path/to/segment.dat \
  --page_scan_start=5242880 \
  --page_scan_end=32203820
```

The range is `[page_scan_start, page_scan_end)`. Starting at `page_scan_start`, the scanner extends
CRC32C one byte at a time until the following four bytes match the running CRC, then validates the
candidate page footer. A successful page becomes the start of the next search. Success requires at
least one page and requires the final page to end exactly at `page_scan_end`.

`scan_page_crc` does **not** search for an arbitrary first page offset. If `page_scan_start` points
to padding, a corrupt extent, or the middle of a page, it reports `no_page_boundary`. Candidate
physical starts such as an S3 multipart boundary must be proposed from independent evidence before
using this operation.

### Page output control

Both raw CRC operations accept `--page_output`:

| Value | Output |
|---|---|
| `all` | Print every checked or discovered page, followed by the summary. This is the default. |
| `errors` | Print only failed page records and `no_page_boundary`, followed by the summary. |
| `summary` | Suppress individual page records and print only headers and the final summary. |

A successful page record looks like:

```text
page=0 offset=5242880 size=18349 range_valid=true readable=true actual=929431102 expect=929431102 checksum_ok=true footer_size=15 footer_ok=true page_type=DATA_PAGE
```

The final result is designed for shell automation:

```text
=== Page CRC Summary ===
Mode: ranges
Pages Checked: 2
Valid: 2
Bad CRC: 0
Bad Footer: 0
Unreadable: 0
Invalid Range: 0
Status: OK
```

The implementation uses bounded one-MiB reads for CRC calculation and scanning. It does not load
the complete segment or complete scan range into memory.

## Dump or check segment data

The default command prints the first 10 logical rows from every top-level scalar column:

```bash
be/output/lib/meta_tool \
  --operation=show_segment_data \
  --file=/path/to/segment.dat
```

### Row-reading flags

| Flag | Default | Description |
|---|---:|---|
| `--rows` | `10` | Maximum logical rows read per column. Use `-1` for all remaining rows and `0` to read no values. |
| `--row_start` | `0` | First logical row ordinal to read. |
| `--batch_rows` | `4096` | Maximum rows decoded by each `next_batch` call. This bounds working memory and does not change the selected row range. |
| `--check_only` | `false` | Decode selected rows without printing every value. A compact result is printed for each column. |
| `--verify_checksum` | `true` | Verify page checksums through the normal `ColumnReader`/`PageIO` path. |
| `--scan_segment_pages` | `false` | Independently scan data pages referenced by ordinal indexes and dictionary pages, and print CRC/layout summaries. |

`--rows` applies to each selected top-level column. Doris segments are columnar, so dumping all
rows from a 43-column segment prints approximately 43 values per logical row, not 1 output line per
row.

If `--rows` exceeds the number of rows remaining after `--row_start`, the range is clamped at the
segment row count. `--row_start` greater than the segment row count and `--batch_rows=0` are invalid.

### Examples

Print the first 1,000 rows from every scalar column:

```bash
be/output/lib/meta_tool \
  --operation=show_segment_data \
  --file=/path/to/segment.dat \
  --rows=1000
```

Print logical rows `[100000, 101000)`:

```bash
be/output/lib/meta_tool \
  --operation=show_segment_data \
  --file=/path/to/segment.dat \
  --row_start=100000 \
  --rows=1000
```

Dump every row. Redirecting stdout is recommended for large segments:

```bash
be/output/lib/meta_tool \
  --operation=show_segment_data \
  --file=/path/to/segment.dat \
  --rows=-1 \
  --verify_checksum=true \
  > segment.dump.txt
```

Decode every row without printing values:

```bash
be/output/lib/meta_tool \
  --operation=show_segment_data \
  --file=/path/to/segment.dat \
  --rows=-1 \
  --batch_rows=4096 \
  --check_only=true \
  --verify_checksum=true
```

For a stronger forensic check, combine full decoding with the independent page scan:

```bash
be/output/lib/meta_tool \
  --operation=show_segment_data \
  --file=/path/to/segment.dat \
  --rows=-1 \
  --batch_rows=4096 \
  --check_only=true \
  --verify_checksum=true \
  --scan_segment_pages=true
```

Successful full decoding ends with a summary similar to:

```text
=== Data Read Summary ===
Columns Checked: 43
Rows Per Column: 1211072
Row Range: [0,1211072)
Check Only: true
Verify Checksum: true
Status: OK
```

## Validation scope and limitations

Full `check_only` decoding verifies that every selected logical row can be read through the column
iterator, including page checksum verification when `--verify_checksum=true`. It exercises data,
dictionary, encoding, compression, nullable-column, and ordinal-index read paths needed for the
selected columns.

`--scan_segment_pages=true` independently checks every data page exposed by each ordinal index and
each dictionary page. It does not currently enumerate every auxiliary zone-map, Bloom-filter, or
bitmap-index page, so success must not be described as exhaustive validation of all auxiliary
indexes.

`check_page_crc` and `scan_page_crc` operate on raw physical ranges and do not prove that those
pages are referenced by the segment footer or rowset metadata. A single CRC32C match is not by
itself proof that an arbitrary offset is a page boundary; footer parsing, exact range completion,
multiple consecutive pages, and agreement with independent metadata materially strengthen the
evidence. Neither operation modifies or repairs its input file.

Top-level complex columns are not yet supported by `check_only`; the command returns a non-zero
status instead of silently reporting success. Displayed string-like values are escaped and truncated
after 50 bytes, so `show_segment_data` is a readability/debug dump rather than a round-trippable
logical export.

## Exit status

| Code | Meaning |
|---:|---|
| `0` | All requested rows or pages completed successfully; CRC scanning ended exactly at its requested end. |
| `1` | The file/ranges file could not be read, or a footer, checksum, page, iterator, decoding, boundary, or early-EOF error occurred. |
| `2` | Required flags, row/page options, range syntax, output mode, or scan bounds are invalid. |
