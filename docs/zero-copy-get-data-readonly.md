# Zero-copy ColumnVector `get_data()` Read-only Design

## Goal

Fixed-length scanner columns can reference one scanner page directly. The execution layer must be
able to keep that page shared across read-only consumers, while still preserving the traditional
owned `PaddedPODArray` contract for callers that write into a `ColumnVector`.

The interface split is:

- `ColumnVector::get_data() const`: returns a read-only view. It can point either to owned
  `PaddedPODArray` storage or to one external scanner page.
- `ColumnVector::get_data_mutable()`: returns owned mutable `PaddedPODArray` storage and
  materializes an external page first if needed.
- `ColumnVector::get_data_with_padding() const`: remains only for paths that require Doris-owned
  padded storage. It materializes on non-const access and DCHECKs the const access is not external.

This keeps zero-copy as the normal read path. Materialization becomes local to a real writer instead
of being forced at sharing boundaries such as multicast.

## Call-site Classification

All `ColumnVector` callers should fall into one of these groups.

| Group | Required API | Reason |
| --- | --- | --- |
| Input-only expression, predicate, hash, compare, serialize read | `get_data()` | Must not break external-page sharing. |
| Destination vector, null map, offsets, selector output | `get_data_mutable()` | The caller writes or resizes the container. |
| In-place mutation of an existing vector | `get_data_mutable()` | The caller intentionally owns and mutates this column. |
| Nullable condition normalization | New adjusted condition column | `NULL` is false, but the input nested bool column can be scan-backed and shared. |
| Template code shared with decimal or complex columns | Local helper / type branch | Decimal and complex columns still expose mutable `get_data()` and are not part of this split. |
| Array offsets | `get_data_mutable()` for non-const offsets, `get_data_with_padding()` for const offsets | Offsets need padded owned storage for prefix access. |

The important behavioral rule is that no read-only consumer should cause external data to be copied,
and no writer should write through a view of the scanner page.

## Multicast And Shared Blocks

The earlier multicast-local workaround materialized blocks before sharing. That avoided corruption
for multicast but lost the main optimization and did not cover other shared-block shapes.

With the read-only split, multicast needs no special materialization. Each consumer can continue to
share the same external page until a downstream operation actually writes. At that point only that
consumer's column is materialized; sibling blocks still keep the zero-copy page.

Covered scenarios:

- one multicast consumer filters a shared external nullable bool column;
- other consumers still point at the original external page;
- multiple consumers independently filter and materialize their own results;
- nullable condition handling builds an adjusted bool column instead of modifying shared input.

## Risk Patterns Checked

These patterns should be reviewed whenever new call sites are added:

- `get_data()[i] = ...` on `ColumnVector`;
- `get_data().resize/push_back/reserve/assign/clear` on `ColumnVector`;
- binding mutable references to `get_data()` for a fixed-length vector;
- changing a nullable bool nested column in place for IF/CASE/COALESCE style condition handling;
- template code that assumes all numeric-like columns have the same mutable API.

Mechanical conversion is not enough. For each hit, confirm whether the column is source data,
destination data, or a reusable internal buffer.

## Exit Test Set

The key local validation set is:

- `ColumnVectorTest.raw_data_and_insert_indices_from_external_page`
- `ColumnVectorTest.filter_by_selector_from_external_page`
- `ColumnArrayTest.FilterExternalFixedLengthDataKeepsOffsetPrefix`
- `ScannerDirectProjectionTest.*`
- `MultiCastDataStreamerTest.PullAndFilterExternalNullableBoolForManyConsumers`
- `MultiCastDataStreamerTest.FilterOneConsumerKeepsSiblingExternalPage`
- `ShortCircuitUtilTest.*`
- `VConditionExprCoalesceTest.*`

Additional checks before push:

- `./build.sh --be`
- the targeted BE UT filter above
- repository clang-format wrapper
- repository format check wrapper
- `git diff --check`

If a failure appears outside this set, classify it by the same read/write rule before patching. A
fix that only materializes an upstream shared block is suspicious unless the upstream node itself is
the writer.
