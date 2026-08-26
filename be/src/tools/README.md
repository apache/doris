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

### When to use each operation

Start with the normal metadata-driven path whenever the segment footer and indexes are expected to
be trustworthy. The raw CRC operations are forensic tools for the cases where normal reading has
already failed or where a candidate physical layout must be tested independently of the offsets
stored in the segment.

| Situation | Recommended operation | Reason |
|---|---|---|
| Validate that a healthy or repaired segment can really serve all logical rows | `show_segment_data --rows=-1 --check_only=true --verify_checksum=true` | Exercises normal footer, index, iterator, decompression, decoding, and checksum paths. |
| Inventory metadata-referenced data and dictionary pages | Add `--scan_segment_pages=true` to `show_segment_data` | Uses the same PagePointers that Doris will use in production and reports page-level layout/CRC results. |
| Check one or more page locations whose exact physical offsets and sizes are already known | `check_page_crc` | Directly tests each supplied `offset:size` range without following the possibly wrong PagePointer stored in metadata. |
| Test whether adding a suspected constant delta to metadata offsets recovers pages | `check_page_crc` | The corrected offset and the original metadata size form an exact candidate range. Sampling pages near the start, middle, and end tests whether one displacement explains the suffix. |
| Validate a candidate first-page boundary when individual page sizes are unknown | `scan_page_crc` | Derives each page end from CRC/footer evidence and requires a gap-free chain to the supplied end. |
| Search an entire file for an unknown first-page offset | Neither raw operation directly supports this | `scan_page_crc` verifies a caller-provided start; it does not enumerate possible starts. First derive candidates from metadata, an S3 part boundary, byte comparison, logs, or another independent source. |
| Determine whether bytes are semantically correct merely because their CRC matches | Neither raw operation is sufficient | CRC proves physical consistency of a selected page range, not business-value correctness. Follow with normal full decoding. |
| Modify or repair the segment | Neither operation | Both operations are read-only. Repair must be performed separately and then validated through the normal read path. |

The distinction matters because Doris page reads are address based, not self-synchronizing. The
reader obtains an absolute `PagePointer(offset, size)` from the footer or an index and reads exactly
that byte range. It does not search forward for another valid page after a checksum mismatch. If an
unaccounted extent is inserted in the middle of the file, the footer at the physical EOF can still
parse successfully while every PagePointer after the insertion addresses the wrong bytes:

```text
metadata/logical layout
... previous page | next page at logical offset L | following pages ...
                  ^ PagePointer offset L

damaged physical layout
... previous page | unaccounted bytes | next page at physical offset P | following pages ...
                  ^ reader still uses L           ^ candidate P = L + delta
```

This is the main use case for the two raw operations. `scan_page_crc` can test whether `P` begins a
continuous page chain, while `check_page_crc` can test representative metadata ranges after adding
the same `delta` to their offsets.

### Practical investigation workflow: inserted extent at an S3 boundary

The following workflow comes from a real `Bad page: checksum mismatch` investigation. The segment
footer was readable, pages before logical offset `5,203,052` were readable, and pages after that
offset failed. Metadata independently placed both the end of the key/primary-index group and the
first value-column page at logical offset `5,203,052`.

The configured S3 write-buffer boundary was 5 MiB:

```text
5 MiB = 5,242,880
candidate delta = 5,242,880 - 5,203,052 = 39,828
```

The 5 MiB value supplied a candidate physical start; the CRC scanner did not discover that start.
Use the following sequence when investigating a similar layout.

1. Preserve the original file and record its length and checksum. Run `show_segment_footer` and a
   normal `show_segment_data` check first. Record the first failing column/page and whether all
   metadata-referenced pages after some logical boundary fail.

2. Establish the logical boundary `L` from metadata. Useful independent signals include the end of
   the last readable PagePointer, the start of the first failing data page, vertical-compaction
   key/value group boundaries, and the expected logical EOF derived from the footer.

3. Propose one or more physical starts `P` from evidence outside the CRC scanner. Examples include
   S3 multipart boundaries, file-cache block boundaries, direct object-range comparison, known
   alignment, or a constant difference between the actual and expected file sizes.

4. Check whether `[L,P)` itself forms a page chain. For the example, scanning the suspected extra
   extent is expected to fail:

   ```bash
   be/output/lib/meta_tool \
     --operation=scan_page_crc \
     --file=/path/to/bad-segment.dat \
     --page_scan_start=5203052 \
     --page_scan_end=5242880 \
     --page_output=errors
   ```

   The observed result was `no_page_boundary offset=5203052 remaining=39828` and exit code 1. This
   does not by itself prove that the bytes are corrupt; it says only that the suspected extent is
   not a consecutive Doris page chain beginning at `L`.

5. Scan forward from candidate `P` to an independently expected end. In the example, the following
   range was expected to contain all 91 pages of column 1:

   ```bash
   be/output/lib/meta_tool \
     --operation=scan_page_crc \
     --file=/path/to/bad-segment.dat \
     --page_scan_start=5242880 \
     --page_scan_end=5919643 \
     --page_output=all
   ```

   It produced 91 consecutive CRC- and footer-valid pages, with no gaps or overlaps, and ended
   exactly at `5,919,643`. The page count and end position also matched the metadata after adding
   `39,828`. A long exact chain is much stronger boundary evidence than one isolated CRC match.

6. Use `check_page_crc` to test the same delta at widely separated locations. Keep the sizes from
   metadata and add `39,828` only to offsets:

   ```bash
   be/output/lib/meta_tool \
     --operation=check_page_crc \
     --file=/path/to/bad-segment.dat \
     --page_ranges=5242880:6485,5919754:47,264635689:1384,274697492:2932 \
     --page_output=all
   ```

   These candidates cover the first displaced data page, another small data page, a middle/late
   ordinal root, and a page near the file tail. If all pass while their uncorrected metadata offsets
   fail, one inserted extent and a constant suffix displacement is a better explanation than many
   unrelated page corruptions.

7. If creating a non-destructive forensic copy by removing the suspected extent, preserve the
   original and verify byte identity on both sides of the edit. Then validate the copy through the
   normal path, not only with raw CRCs:

   ```bash
   be/output/lib/meta_tool \
     --operation=show_segment_data \
     --file=/path/to/repaired-copy.dat \
     --rows=-1 \
     --batch_rows=4096 \
     --check_only=true \
     --verify_checksum=true \
     --scan_segment_pages=true
   ```

   In the incident, deleting only `[5,203,052,5,242,880)` made the suffix byte-for-byte identical
   to the expected logical layout; 9,060 data pages and six dictionary pages passed, and all 11
   columns were readable. That establishes the repaired copy as forensic evidence, but does not by
   itself authorize replacing a production object or its rowset metadata.

### Interpreting raw CRC results

| Observation | What it supports | What it does not prove |
|---|---|---|
| One explicit range has matching CRC and a valid footer | The supplied offset and size delimit an internally consistent Doris page. | The page is referenced by this segment, contains the intended values, or is at the correct logical position. |
| Corrected range passes while the metadata range of the same size fails | The candidate physical displacement is plausible for that page. | A single page is not enough to prove one constant displacement for the whole suffix. |
| Many corrected ranges near the start, middle, and tail pass with the same delta | A single inserted/omitted physical extent is a strong explanation. | Whether the discrepancy originated in object upload, object storage, cache, warm-up, range download, or local copying. |
| `scan_page_crc` finds a long continuous chain and ends exactly at an independently expected end | The proposed start is strongly supported as a real page boundary. | How the start was created or that it would have been found without the supplied candidate. |
| `no_page_boundary` occurs at the first offset | No valid page chain was found beginning exactly there within the supplied range. | The range contains no valid page at some later, unknown start. |
| A chain stops after several valid pages | The valid prefix is physically consistent; the next page is corrupt, the requested end is wrong, or non-page bytes follow. | Which of those causes applies without more metadata or byte-layout evidence. |
| Raw page checks pass but normal full decoding fails | Physical page framing is intact for the checked ranges. | Compression, encoding, index linkage, schema, logical ordering, or value semantics may still be wrong. |

For an unknown start, avoid running `scan_page_crc` across a multi-gigabyte range and treating its
failure as discovery. The implementation performs a byte-by-byte running CRC search from exactly
the supplied start, so the work is proportional to the distance to the next accepted boundary.
Generate a small set of defensible candidate starts first, then test each one.

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
