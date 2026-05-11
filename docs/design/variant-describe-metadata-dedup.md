# Variant DESCRIBE Metadata Deduplication

## Background

When `describe_extend_variant_column` is enabled, FE requests remote tablet schemas through
`FetchRemoteTabletSchemaUtil`. BE handles the request in `PInternalService::fetch_remote_tablet_schema`
and currently calls `VariantCompactionUtil::calculate_variant_extended_schema` on the worker BE.

The current implementation scans all snapshot rowsets and opens every segment to read Variant
subcolumn metadata. This is expensive when a tablet has many segments whose Variant subcolumn
schema is identical or highly repeated.

The goal is to reduce repeated metadata reads for DESCRIBE without storing the complete Variant
subcolumn schema in rowset metadata. This is important for cloud mode because rowset metadata can
already be large, especially when the table has many columns.

## Design

Each newly written rowset records two compact pieces of Variant schema metadata:

- a rowset-level `variant_schema_hash`, used to skip rowsets with identical Variant DESCRIBE schema;
- rowset-level `variant_schema_representatives`, the first final segment id for each unique
  segment-level Variant schema in that rowset.

The rowset metadata does not store complete Variant subcolumn `ColumnPB` data or the full canonical
schema key. It also does not store a per-representative schema hash; the rowset hash already
summarizes the full set of unique segment schema keys, and each representative only needs a segment
id. The persisted data is bounded by the number of unique segment schema layouts, not by the number
of table columns or Variant subcolumns.

The canonical key used during writing must include every property that affects DESCRIBE output:

- a format version;
- Variant root column stable identity, preferably parent unique id;
- canonical Variant subpath;
- logical type, nested structure, and nullability.

Implementation builds this key from a canonicalized `ColumnMetaPB` serialization for each supported
Variant subcolumn. Physical segment fields such as page pointers, row counts, byte sizes, and
segment-local unique ids are deliberately excluded so identical logical schemas still deduplicate.

The canonical key is used only while building the rowset. The persisted hash is a compact
fingerprint and must not be used to reconstruct schema content.

The fast path intentionally supports only ordinary dynamic Variant subcolumns. Segment footers may
contain an empty sparse container marker even for ordinary dynamic rows; that marker is ignored.
Complex layouts such as doc mode, actual sparse column materialization with non-empty sparse
statistics, typed paths when they produce sparse statistics, nested group markers, and root-only
scalar Variant segments omit this optimization metadata and use the existing full segment scan. This
keeps the optimized metadata small and avoids returning an incomplete DESCRIBE schema for partially
represented layouts.

## Write Path

`SegmentWriter` and `VerticalSegmentWriter` build a segment-level Variant canonical key after the
segment footer and external column metadata layout are finalized. The key must be generated from a
stable binary or structured encoding, not from protobuf debug text.

`SegmentStatistics` carries the segment-level canonical key and hash in BE memory. `BaseBetaRowsetWriter`
uses these fields when building rowset metadata:

- if the tablet has no Variant columns, no new fields are written;
- if a segment contains doc-mode, actual sparse statistics, typed-path-to-sparse materialized as
  sparse statistics, nested-group, or root-only scalar Variant layout, no new fields are written;
- if any non-empty segment lacks a canonical key, no new fields are written and DESCRIBE falls back
  to the full scan path;
- otherwise, final segment ids are traversed in order, full canonical keys are deduplicated exactly,
  and the first segment id for each unique key is persisted as a representative;
- the rowset-level hash is computed from the sorted set of unique segment canonical keys, so
  the hash is independent of which segment schema appears first.

Load stream must not send the full canonical key in `SegmentStatisticsPB`, because that would make
RPC metadata grow with Variant subcolumns. If the receiver cannot rebuild the key from the written
segment before rowset metadata is built, the rowset must omit the new fields and use the fallback
scan path.

Segcompaction must use final segment ids. Compacted output segments must produce their own canonical
keys. Plain segment rename paths must keep any in-memory segment statistics aligned with the final
segment id. If this cannot be guaranteed, the new fields must be omitted.

## Metadata Merge And Compatibility

Unsafe metadata merges must clear the new fields rather than trying to preserve a partial summary.
This includes `RowsetMeta::merge_rowset_meta`: because rowset metadata only persists hashes and
representative segment ids, a merged rowset cannot safely rebuild a precise rowset-level canonical
key without opening segment metadata or storing full keys. Clearing the new fields preserves
correctness by forcing DESCRIBE to fall back to the full scan path.

Copy-only metadata paths can copy the fields unchanged when the segment layout is unchanged. Any
path that combines multiple rowsets must use an all-or-nothing rule: if all inputs have complete
metadata and the final segment id offsets are known, the representatives may be offset and carried;
otherwise the new fields must be cleared.

Old rowsets and rowsets written by old BEs do not have the new fields. They remain correct because
DESCRIBE falls back to scanning all segments.

The new fields are independent of cloud schema dictionary handling. This change does not modify
`write_schema_kv` behavior and does not solve historical rowset metadata growth caused by existing
tablet schema or sparse column metadata. It only guarantees that the new optimization metadata does
not grow with the number of columns or Variant subcolumns.

## DESCRIBE Read Path

`VariantCompactionUtil::calculate_variant_extended_schema` keeps its public behavior but internally
uses the new metadata when available:

1. Take the tablet snapshot rowsets.
2. Rowsets with complete Variant schema metadata are deduplicated by the rowset hash.
3. Rowsets without the new metadata are not deduplicated.
4. For each selected rowset, scan only representative segment ids when present.
5. If representative ids are missing, duplicated, or invalid, scan the whole rowset and do not use
   that rowset for metadata-based deduplication.
6. Merge the collected schemas with the existing `get_least_common_schema` logic.

Segment loading should use the existing segment cache where possible so repeated DESCRIBE calls do
not rebuild the same readers unnecessarily.

The rowset-level hash is a fingerprint. This design accepts the practical collision risk of 128-bit
fingerprints. If the project later requires deterministic zero-collision behavior, the design must
be extended with a sidecar or dictionary that stores the full canonical schema.

## Proto And Code Touch Points

Expected code areas:

- `gensrc/proto/olap_file.proto`: add compact Variant schema hash and representative segment fields
  to both `RowsetMetaPB` and `RowsetMetaCloudPB`.
- `be/src/cloud/pb_convert.cpp`: copy and move the new fields in all four conversion directions.
- `be/src/storage/rowset/rowset_meta.*`: add helpers and clear new fields on unsafe merge.
- `be/src/storage/rowset/rowset_writer.h`: carry in-memory segment canonical key data.
- `be/src/storage/segment/segment_writer.*` and `be/src/storage/segment/vertical_segment_writer.*`:
  expose segment-level Variant schema key/hash after footer finalization.
- `be/src/storage/rowset/segment_creator.cpp` and rowset writer paths: pass the in-memory key into
  rowset metadata construction.
- `be/src/exec/common/variant_util.*`: use rowset hash and representative segment ids in the
  DESCRIBE-only schema calculation path.

## Tests

Required focused tests:

- BE unit tests for same-schema and different-schema multi-segment Variant rowsets.
- BE unit tests for missing canonical key fallback and no-Variant behavior.
- BE unit tests for sparse/doc/typed/nested/root-only layouts falling back instead of writing
  optimization metadata.
- BE unit tests for `RowsetMeta::merge_rowset_meta` clearing the new fields.
- Segcompaction coverage for final segment id correctness or fallback.
- Cloud/proto tests for all `RowsetMetaPB <-> RowsetMetaCloudPB` conversion directions.
- Regression coverage for `describe_extend_variant_column=true` showing DESCRIBE output remains
  unchanged with multiple rowsets and segments.
- Performance evidence comparing segment open count or fetch time before and after the optimization.
