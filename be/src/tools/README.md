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

Top-level complex columns are not yet supported by `check_only`; the command returns a non-zero
status instead of silently reporting success. Displayed string-like values are escaped and truncated
after 50 bytes, so `show_segment_data` is a readability/debug dump rather than a round-trippable
logical export.

## Exit status

| Code | Meaning |
|---:|---|
| `0` | All requested rows and enabled page checks completed successfully. |
| `1` | The file could not be opened or a footer, checksum, page, iterator, decoding, or early-EOF error occurred. |
| `2` | Command-line row options are invalid or the required `--file` flag is missing. |
