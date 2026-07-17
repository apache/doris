# FileScannerV2 Code Review Guide

This guide contains the detailed checklists referenced by
`be/src/format_v2/AGENTS.md`. Read the common checklist for every FileReader review, then apply the
format-specific checklist when reviewing Parquet or ORC.

## Common FileReader: Indexes and Predicate Filtering

- Inventory the reader's actual pruning capabilities before evaluating a change: metadata or
  statistics, dictionary information, Bloom filters, page/stripe/row indexes, partition/Split
  ranges, and format-specific encodings. Record the granularity, supported predicate/type set,
  exactness, I/O cost, and conservative fallback for each capability.
- A FileReader consumes only predicates already localized by `TableColumnMapper` in
  `FileScanRequest`. It may translate those predicates into format-native indexes or SDK filters,
  but it must not reinterpret table-schema identity, defaults, partitions, or table-format
  semantics.
- Every index may discard a candidate only when it proves that the candidate cannot match. Missing,
  malformed, stale, truncated, unsupported, writer-incompatible, or unsafe metadata must retain the
  candidate or return the format's explicit correctness-preserving error.
- Check logical-to-physical identity at every index boundary: file-local root and nested column IDs,
  physical leaf IDs, row-group/stripe/page ordinals, byte ranges, file-global row offsets, and
  selected row ranges. Index results for one column or unit must never be applied to another.
- Verify metadata semantics for NULL/all-NULL, empty units, NaN, signedness, truncated bounds,
  decimal precision/scale, date/timestamp/timezone, string/binary ordering, CHAR padding, and
  external-writer differences before trusting min/max or membership information.
- Preserve a cheap-to-expensive pruning order. Do not read or parse a finer index for a file,
  row group, stripe, or page already eliminated by a cheaper layer. Measure index read/parse/build
  cost as well as the I/O, decompression, decoding, and materialization it avoids.
- Trace each predicate through index pruning, exact format-native filtering, Doris residual VExpr,
  delete predicates, and final materialization. A predicate not exactly covered by an earlier layer
  must remain in the residual path.
- Preserve SQL three-valued logic and error behavior across AND/OR/NOT, comparisons, IN/NOT IN,
  IS NULL, null-safe equality, casts, functions, stateful expressions, and exception-sensitive
  operations. Splitting or reordering predicates requires proof of equivalence.
- Predicate columns and lazily read non-predicate columns must refer to the same original rows after
  all skips and filters. Skipping must advance every physical reader consistently, including nested
  definition/repetition state, offsets, row positions, and subsequent batches.
- Keep row-level deletes, equality deletes, position deletes, table filters, and query predicates in
  their specified order. An index optimization must not bypass a delete or use post-filter row
  numbering where file-global numbering is required.
- Readers without a native index or lazy-read capability must declare that boundary and preserve
  correctness through residual evaluation. Do not add an imitation index in a generic layer merely
  to make formats appear uniform.
- Require differential tests that compare exact results and errors with each index/filter
  optimization enabled and disabled. Cover missing/invalid indexes, all/none/partially filtered
  units, multiple files/Splits/batches, NULL and type boundaries, nested data, deletes, and
  external-writer fixtures.

## Common FileReader: Data and Condition Caches

- Distinguish the cache layers and their value semantics: remote `FileCache` stores file bytes,
  format metadata/page caches store format-specific serialized ranges or parsed metadata,
  `ConditionCache` stores predicate survivor granules, and table-format caches may store deletion
  vectors or decoded objects. Never reuse an entry as a different representation.
- A cache key must include every input that can change the value: filesystem and canonical path,
  stable object/file version, size or mtime where reliable, byte/Split range, format/encoding
  context, and predicate digest for filter results. Disable the cache when a stable identity cannot
  be established; never trade stale rows for a hit.
- Validate hit, miss, partial coverage, overlapping/subrange reads, eviction, concurrent access,
  cancellation, and error paths. A partial cache hit must read or conservatively retain uncovered
  data rather than treating it as absent.
- `ConditionCache` can skip only file-global granules explicitly known to contain no surviving row.
  Disable or expand the key when Runtime Filters, delete files/vectors, table snapshots, or other
  changing semantics are not represented. Publish a miss result only after the physical reader
  reaches EOF successfully so unvisited granules cannot become false negatives.
- Cache admission, prefetch, and range merging must follow pruning and lazy materialization. Do not
  prefetch output columns or pruned units merely to improve hit rate, and account for read
  amplification, request count, memory ownership, and cache pollution.
- Preserve resource accounting and source attribution across local, peer, and remote hits. Require
  counters for hit/miss/write/eviction, bytes by source, wait/download time, requests, and avoided
  reads so performance claims are diagnosable.
- Require warm/cold, enabled/disabled, overwrite/version-change, partial-range, concurrent, and
  cancellation tests. Cached and uncached execution must return identical rows and errors.

## Common FileReader: Virtual Columns

- Keep file-coordinate virtual columns distinct from table-format virtual columns. FileReaders may
  synthesize reserved file-local `ROW_POSITION` and `GLOBAL_ROWID`; `TableReader` and
  `TableColumnMapper` own table semantics such as Iceberg `_row_id`,
  `_last_updated_sequence_number`, and Doris Iceberg row locators.
- `ROW_POSITION` is the absolute zero-based physical row in the file, not an output, batch,
  selected-row, row-group, stripe, or Split-local ordinal. It must advance across pruned units,
  skipped pages/granules, rejected batches, lazy filters, and deletes without renumbering survivors.
- `GLOBAL_ROWID` must be stable and unique for its documented context. Review context version,
  backend/file identity, serialization, physical row position, cross-file collisions, and retries;
  filtering and batching must not change the generated ID for the same source row.
- Generate virtual values only when requested as output or needed by a predicate/delete. Support
  virtual-only scans with no physical projected column, predicate-only virtual columns, selected-row
  materialization, and EOF without forcing unrelated file I/O.
- Preserve declared type, nullability, nested shape, and `LocalColumnId`/`LocalIndex` mapping. Do not
  let reserved negative IDs collide with invalid IDs, physical columns, table IDs, or block
  positions.
- Require tests across multiple files, Splits, row groups/stripes/pages, batches, all rows filtered,
  no rows filtered, index/cache skips, lazy materialization, deletes, and virtual-only projection.
  Compare virtual values with the same scan when pruning, caching, and lazy reads are disabled.

## Common FileReader: Performance and Observability

- Keep index construction, predicate translation, cache lookup, and virtual-column setup out of
  per-row and repeated batch paths unless the work is inherently row-local. Avoid repeated schema
  traversal, expression cloning, metadata parsing, allocation, and conversion.
- Keep Profile counters in the visible `FileScannerV2 -> TableReader -> FileReader -> IO` hierarchy.
  Format-specific readers, such as `ParquetReader`, belong below `FileReader`; lifecycle, metadata,
  index, predicate, decode, materialization, and physical I/O paths must all have timers at the layer
  that owns the work. Flush recursively aggregated child-reader statistics at every batch boundary,
  including empty-selection and error exits, so a slow in-progress scan is diagnosable before close.
- Require format readers to populate the common `ReaderStatistics` accurately where applicable:
  filtered/read row groups, Bloom and min/max pruning, filtered group/page/lazy rows, read rows and
  bytes, metadata/footer/cache timing, page-index work, predicate time, dictionary rewrite, and
  Bloom read time.
- Reject dead or ambiguous counters. In particular, `FilteredBytes` counts compressed bytes of
  projected physical chunks avoided by pruning, not every child in the Row Group; footer read,
  footer parse, lazy page-index materialization, and page-index predicate evaluation need distinct
  timers. Raw I/O counters stay under `IO` even when a format reader initiates the request.
- Evaluate performance with representative format versions, writers, data ordering, predicate
  selectivity, nested width, remote storage, batch sizes, and warm/cold caches. Report both the
  optimization overhead and the avoided work; a low pruning ratio alone is not a defect.

## Parquet Native Decode Boundary

- V2 must instantiate only readers and decoders under `be/src/format_v2/parquet/`; calls into the
  v1 `ParquetColumnReader` or edits under `be/src/format/parquet/` are review blockers.
- Trace the hot path as `ColumnReader -> Decoder span/cursor API -> DataTypeSerDe -> Doris Column`.
  Decoder must not accept a Doris column or target type, and the path must not create Arrow arrays,
  builders, `DecodedColumnView`, or another decoded leaf batch.
- Verify physical/logical metadata is immutable per leaf reader and complete for signed integers,
  decimal precision/scale, date/time/timestamp units and UTC adjustment, INT96, UUID, FLOAT16, and
  fixed-width binary. Unsupported combinations return explicit errors before plausible output.
- Treat legacy Parquet `TIMESTAMP_MILLIS` and `TIMESTAMP_MICROS` converted types as UTC-adjusted.
  Do not give them the local/unspecified semantics of an unannotated INT64 timestamp; data decode,
  statistics conversion, and min/max pruning must use the same timezone rule.
- Route plain, dictionary, and decoded timestamp inputs through one checked conversion contract.
  Validate INT96 nanos-of-day before widened Julian-day arithmetic, reject unit scaling overflow,
  and enforce Doris year 0001-9999 before materialization. Conversion failures must follow the same
  strict/non-strict and dictionary-ID propagation rules as other direct types.
- Verify schema-change routing separately from physical decode. Integer, FLOAT-to-DOUBLE, decimal,
  and string-family changes should use the direct target-SerDe path. Other supported logical casts
  may use one persistent generic `ColumnTypeConverter` source column; its value/null-map sizes must
  reset per batch while normal-size capacity is retained, oversized capacity must be released after
  the top-level parent consumes the batch, and it must never become a decoder-facing ABI.
- Dictionary review must separate dictionary-entry IDs from logical rows and non-null payload
  ordinals. Materialize the typed dictionary once per generation through the same SerDe, validate
  every index before access, and invalidate cached dictionary state at Row Group/file/type changes.
  Dictionary-entry predicate evaluation and later row-value flattening must reuse that same typed
  generation rather than serializing, parsing, or converting the dictionary twice.
- Check direct materialization for PLAIN, RLE/dictionary, DELTA_BINARY_PACKED,
  DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, and BYTE_STREAM_SPLIT. Filtering must advance encoded
  values without allocating output; null runs must append defaults without advancing payload.
- For a filtered page fragment without definition-level NULLs, require one SerDe entry and one
  batch-level selected-decode dispatch. Selection ranges belong to persistent reader scratch;
  per-range virtual SerDe/decoder calls in the hot path are a review blocker. Fixed PLAIN should
  bulk-gather spans, BYTE_ARRAY PLAIN should scan lengths once, dictionary decode should validate
  every ID before gathering selected IDs, and stateful encodings should batch-decode/reconstruct and
  compact. A NULL-interleaving fallback is acceptable only when it preserves logical output order
  without a decoded intermediate column and is counted by
  `HybridSelectionNullFallbackBatches`.
- Review complex types as a level/shape problem around scalar leaf materialization. Parent offsets,
  null maps, sibling alignment, page-spanning rows, and child payload counts must remain correct
  without materializing an intermediate complex column.
- For MAP, the non-null key leaf owns the outer entry shape. Validate the materialized key/value
  entry counts, but do not compare their raw repetition vectors: a nested value legitimately has
  deeper levels. For STRUCT, compare each sibling only at the current parent boundary and ignore
  repetition owned by a deeper child collection.
- Require a bounded high-water policy for persistent definition/repetition, null, selection,
  conversion, dictionary-index, and decoder-owned scratch. Distinguish active bytes from retained
  capacity: never release an oversized buffer while the current batch still needs it, and require
  three ordinary/idle batches before releasing capacity above the high-water limit. Test both
  outlier release and steady-state reuse so the policy does not create allocation thrash.
- Review all decoder read and skip paths as equally exposed corruption boundaries. Check requested
  counts against remaining/declared values before pointer arithmetic or narrowing; use checked
  addition/multiplication for byte extents; bound BYTE_ARRAY dictionary entry counts and IDs before
  allocation/indexing; and require DELTA_BYTE_ARRAY prefixes to fit the previous reconstructed
  value. BOOLEAN RLE and DELTA skip paths must consume bounded chunks and fail on short streams.
- Before Snappy decompression, inspect the encoded uncompressed length and validate destination
  capacity. Page V1, Page V2, and dictionary pages must produce exactly the declared decoded size;
  an UNCOMPRESSED dictionary page must also declare equal compressed and uncompressed sizes.
- For a STRUCT whose projected children are all missing after schema evolution, require a
  levels-only physical reference leaf. It must advance and validate encoded payload cursors while
  deriving the synthetic child count, without constructing a discarded string/complex column.
- `CountColumnReader` must use the native levels-only reader and must not decode payload or call
  Arrow `ReadRecords`. Require profiles that distinguish page I/O, decompression, level decode,
  value decode, SerDe materialization, hybrid selection batches/ranges/NULL fallback,
  filtered-value skips, and page fragmentation.
- Validate every signed Column Chunk offset/length before converting it to `size_t`. The dictionary
  offset is usable only when it is non-negative and precedes the data offset; the complete range
  must fit the file. Apply the PARQUET-816 tail padding only to affected parquet-mr versions, cap it
  at 100 bytes, and keep it inside the file. Scalar and levels-only COUNT readers share this helper.
- Treat OffsetIndex as one optional, all-or-nothing navigation structure. Require first row zero,
  the first physical location to equal the owning ColumnMetaData `data_page_offset`, strictly
  increasing row ordinals and physical offsets, positive sizes, non-overlapping page ranges, and
  containment in the owning Column Chunk. Discard a malformed index before selecting the indexed
  reader.
- Page iteration skips `INDEX_PAGE` and unknown auxiliary pages before initializing a data decoder.
  Dictionary pages retain their special first-page handling; a later dictionary page is corrupt.
- Derive writer workarounds once from `created_by` and pass them through scalar, nested, page-cache,
  and COUNT paths. Pre-Arrow-3 parquet-cpp Data Page V2 payloads remain compressed despite the
  historical `is_compressed=false` flag.
- Preserve nullable conversion semantics in direct native materialization. Numeric, DATE,
  DATETIME, TIME, and DECIMAL failures insert a default nested value and mark the corresponding NULL
  only in non-strict mode; strict or non-nullable reads return the error. Dictionary failures follow
  the selected dictionary IDs to output rows.
- For cold small-file tests, separate footer I/O/Thrift parse from Arrow metadata adaptation. V2 may
  retain the already-read serialized footer to avoid serializing the same Thrift object again; v1
  opens must not retain those bytes by default.
- Identical fixed-width POD values append with one bulk copy. FIXED_LEN_BYTE_ARRAY strings copy the
  dense byte span once and synthesize offsets; validate this execution contract without flaky
  wall-clock assertions.

## Parquet Multi-Level Filtering

- Use [FileScannerV2 Parquet Scan Design](file-scanner-v2-parquet-scan-design.md) as the detailed
  architecture reference. Trace each affected predicate through localization, Row Group planning,
  Page ranges, row-level residual evaluation, and final selected-column materialization.
- At Row Group level, check Split ownership and file-global row offsets, then verify Statistics,
  Dictionary, and Bloom pruning independently. Dictionary pruning requires complete compatible
  encoding. Bloom may prove absence only; a hit is never a matching row.
- Preserve the cost order from cheap to expensive. Footer Statistics should reduce candidates before
  Dictionary/Bloom I/O, and ColumnIndex/OffsetIndex should be read only for surviving Row Groups.
- At Page level, require compatible ColumnIndex and OffsetIndex semantics. Check page-to-row mapping,
  first/last row boundaries, empty or all-null pages, multi-column range intersection, and conversion
  from logical `selected_ranges` to each leaf reader's physical `page_skip_plan`.
- Page skipping must keep every column reader aligned. Skipping values or pages must advance value,
  definition, and repetition state consistently, especially for nested/repeated columns whose Page
  boundaries do not align across leaves.
- At Row/Batch level, keep SelectionVector positions aligned with original Row Group rows across
  dictionary-ID filters, incremental predicates, residual expressions, deletes, and output
  materialization. Physical row positions must not be renumbered after pruning.
- Verify lazy materialization avoids reading and decoding non-predicate columns for rejected rows
  while advancing all readers correctly. Predicate columns should be read/prefetched first; output
  prefetch should wait for survivors when filtering is active.
- For safe staged single-column predicates, review the observed cost/rejection ordering and its
  cold-start behavior. Reordering is allowed only after every candidate has a sample; prefetch may
  stop at a low-probability reach prefix, while output-column prefetch may start early only after a
  learned high survival ratio. Cache the batch SelectionVector's dense bitmap by generation so a
  wide lazy projection does not rebuild the same O(batch) filter for every column.
- Register Parquet Page Cache ranges only for surviving projected Column Chunks, require a stable
  file-version key, and assess FileCache, MergeRange, prefetch, requests, and read amplification
  together.
- Require counters for Statistics/Dictionary/Bloom pruning, Page Index selected ranges and skipped
  rows/pages, raw and filtered rows, dictionary-row filtering, lazy-read savings, cache sources, and
  remote I/O.
- Differential tests must cover absent/invalid statistics, missing or partial Page Index, mixed
  dictionary/plain encoding, Bloom false positives, NULL/NaN/type conversion, cross-Page batches,
  nested/repeated columns, multiple Row Groups/Splits, and all/none filtered.

## ORC SARG and Index Filtering

- Trace every pushed predicate from localized `FileScanRequest` through
  `build_orc_search_argument()`, ORC `SearchArgumentBuilder`, Stripe selection, SDK RowReader index
  pruning, lazy callback filtering, and residual Doris VExpr.
- SARG conversion must be equivalent to the original Doris predicate for every value, including
  NULL. Preserve AND/OR/NOT grouping, literal-on-left comparison direction, comparison/IN/NULL
  semantics, and wrappers for Runtime Filter, direct-IN, and TopN predicates.
- Verify ORC predicate-domain and literal conversion for integer, floating-point, boolean, string,
  binary, varchar, date, decimal, timestamp, and timestamp-instant, including overflow, non-finite
  values, signed boundaries, precision/scale, CHAR/VARCHAR, timezone, and NULL.
- Treat schema-evolution casts as SARGable only when truth is preserved in the ORC domain. Review
  numeric exactness, decimal widening, date-to-datetime boundary normalization, timestamp precision,
  and string/binary casts. Lossy or timezone-changing casts must remain residual.
- For nested predicates, verify struct field name/ordinal traversal and the final ORC type ID.
  Unsupported array/map/repeated/missing paths must not target another primitive child.
- Intersect the Split byte window with Stripe ownership before SARG selection, then let ORC RowReader
  use row indexes and Bloom filters inside surviving Stripes. SARG must not reintroduce an
  out-of-Split Stripe.
- Validate non-adjacent Stripe ranges, all-pruned/no-Stripe cases, file-global row positions,
  deletes, and Condition Cache granules after every skipped Stripe or row group.
- Keep SDK filtering and Doris lazy materialization aligned: include the correct filter columns,
  preserve selected-row indexes, and decode non-predicate columns only for survivors without
  desynchronizing nested vectors or later batches.
- Review SARG cost for large IN lists, deep trees, many Runtime Filters, repeated literal conversion,
  Stripe-statistics reads, and SDK index initialization. Build once per reader/Split setup and keep
  expensive work out of batch loops.
- Require counters for evaluated/selected groups or Stripes, filtered rows/bytes, groups read,
  lazy-filtered rows, I/O, decompression, and decoding. Explain pruning benefit and SARG/index cost.
- Differential tests must cover NULL truth tables, literal-on-left, nested AND/OR/NOT, IN/NOT IN
  with NULL, casts, all literal domains, nested structs, unsupported arrays/maps, non-adjacent
  Stripes, Split boundaries, row-index strides, Bloom present/absent, and all/none filtered.
