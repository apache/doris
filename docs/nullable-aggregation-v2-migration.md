<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Nullable aggregation V2 migration

Reference source: `e0bf4874070ba0b6837ad93f695ea63532b77a47` (the actual target
branch, rechecked against the scratch-reader changes already present there).

## Execution and format contracts

All ordinary nullable creators now instantiate `AggregateFunctionNullUnaryInlineV2`
or `AggregateFunctionNullVariadicInlineV2`. There is no V1 execution class or creator
branch. COUNT and aggregates that implement their own nullable semantics retain
their specialized implementations.

The variadic wrapper unwraps every nullable argument and accepts a row only when
all arguments are non-NULL. Its batch paths preserve selected destinations and
the existing foreach batch-normalization contract. Native streaming and the legacy String codec share this preparation, including arrays whose hidden NULL rows shift physical offsets. Streaming SUM/AVG, including
decimal variants, write native states directly with the input null map. Other
streaming functions retain one aligned temporary state rather than constructing
an entire batch of wide/string/set states.

| Entry | Execution | Intermediate/persisted representation |
| --- | --- | --- |
| GROUP BY update, selected/nonselected | Unary or variadic V2 | Nullable native nested state column; state validity is independent of SQL result nullability |
| Global aggregation and reader ranges | V2, including empty ranges | Same native state contract |
| Streaming aggregation/passthrough | V2; SUM/AVG have native null-map kernels | Same native state contract |
| Window and incremental frame evaluation | V2 with flag and frame null count | Existing window state layout |
| STATE and COMBINE | V2 underneath the AGG_STATE boundary | Published AGG_STATE codec |
| MERGE and UNION, scalar/batched/selected | V2 underneath the AGG_STATE boundary | Published codec on input; UNION returns that codec |
| FINALIZE | Initialized V2 state, one serialized row at a time | Reader corresponding to the AGG_STATE codec |
| AGG_STATE creation/reconstruction from Thrift/PB/tablet metadata | Same session-independent adapter | Same codec before and after reconstruction |
| Import, segment writing/reading, storage aggregation, compaction | Same V2 function with its storage codec adapter | Historical physical type; no new segment layout |
| Block/Exchange, execution version 20+ | V2 native columns | Nullable nested columns for both arities |
| Block/Exchange, execution version below 20 | V2 with a wire-only codec adapter where needed | Old unary flag selects its historical wire codec; variadic uses String |

### Three different NULL concepts

* A SQL NULL outside an AGG_STATE value has no serialized state to read.
* An empty aggregate state is a non-NULL AGG_STATE value. For nullable SUM its
  historical encoding is the single byte `0`. BITMAP_INTERSECT has a nonnullable
  final SQL result, but its no-input partial still needs an internal NULL mark.
* A native nullable intermediate column carries an internal aggregate-result
  null map. It must not be mistaken for an outer SQL NULL wrapper.

UNION/MERGE use the logical argument type to distinguish the outer SQL wrapper
from an internal nullable intermediate. The evaluator materializes constant
argument columns before dispatch. FINALIZE preserves its existing outer-NULL
handling.

### NULL-only partials and selected readers

A NULL-only partial is skipped before invoking a nested merge. In particular,
`INTERSECT({1,2}, NULL-only, {2,3})` must yield `{2}`; a non-NULL empty bitmap
still yields an empty intersection. Merely OR-ing the destination flag and then
merging every nested payload is incorrect for bitmap intersection.

Native SUM/AVG merge validity updates run in the same pass as payload updates.
The unary no-NULL batch path also marks validity and adds the wrapper prefix to
the nested destination offset, including for a nonnullable SQL result. Stable
unary/variadic transfers preserve their version, and the byte adapter forwards
stable transfer so ordered DISTINCT retains order.
Old unmarked version-19 columns also restore destination validity, so a later
in-memory merge preserves the decoded contribution. A dedicated test decodes
with scalar, range, batch and selected readers, then merges the state again.

The string MIN/MAX selected reader now skips unselected rows before decoding.
This also permits nullable streaming to leave no readable nested payload under
a NULL mark. Generic temporary states are destroyed after each merge, including
exception exits; skipped rows are neither constructed nor decoded.

The ordinary aggregation sink and spill-source merge paths consult the actual
reader's `needs_deserialize_and_merge_scratch()` contract and allocate aligned
storage only when required, retaining a lazy Arena across batches and releasing it on close. The nullable and COMBINE wrappers propagate that
capability. A fixed-width serialized column alone is not evidence that a reader
needs no scratch.

## Persistent and rolling-upgrade compatibility

**AGG_STATE keeps its persistent physical column types.** `AggregateFunctionLegacyNullableState` is a byte-codec adapter, not a
second aggregation implementation. It is instantiated with the concrete V2
function type so the generic byte readers can inline per-state operations instead
of adding virtual calls to every decoded row. It delegates creation, updates, reset,
merge, destruction and final results to the V2 function. Only its column
serialization/readers use the published flag-plus-nested byte stream.

Consequently, nullable SUM/AVG/etc. AGG_STATE columns remain String. COUNT's
non-nullable fixed states and existing native Bitmap/Array/Map representations
retain their prior codecs. `DataTypeAggState::create_column`, serialized type,
SerDe, PB metadata reconstruction, and storage converters agree without exposing
`Nullable(FixedLengthObject)` to the old fixed-state storage converter.

There is no session-switch-dependent on-disk format and no new tablet/PB field.
Existing per-function version restrictions still apply.

Nullable BITMAP_INTERSECT with a nonnullable final SQL result needs one byte-codec
extension: a no-input state written at version 20 is `[0]`. A valid empty bitmap
keeps its historical `[1,1,0]` encoding (length-prefix plus empty bitmap payload),
and all historical nonempty values retain their bytes. Zero cannot start a valid
historical bitmap stream, whose length-of-varint byte is in `[1,9]`. The decoder
recognizes the marker independently of metadata version and preserves it through
version-19 schema compaction. In-memory flag values are 0 (fresh no-input),
1 (valid), and 2 (decoded persistent no-input); merging state 2 into an empty state
preserves the marker for re-encoding.

Historical NULL-only bitmap states that were already serialized as an actual
empty bitmap contain no recoverable distinction. They are read as empty bitmap
values, preserving their published value interpretation; this migration cannot
reconstruct information absent from the bytes. Nonnullable-input bitmap states
keep their historical native codec and canonical empty-result representation.

Fresh marker emission requires the aggregate function's execution version to be
at least 20. Ordinary functions follow the query version; an AGG_STATE codec
follows its type metadata version. Reading an existing marker also preserves it,
even through version-19 schema compaction.

Keep FE `be_exec_version=19` throughout the BE rolling upgrade. Advance it only
after every BE that can execute, read or compact these states supports version 20.
This controls newly created state types and ordinary state expressions; it does
not downgrade an already existing version-20 type. Version-20 AGG_STATE metadata
itself is rejected by older BEs, even when some payload bytes remain unchanged.
The configuration is explicit; the retired session switch does not negotiate
backend support. Older binaries also cannot read new markers in version-19
schemas. After creating version-20 types or writing new-format states, introducing
older BEs or downgrading requires restoring compatible metadata and data; changing
just the current query/session settings cannot undo either boundary.

Execution version 20 identifies the unified native query-intermediate contract.
Before version 20, a coordinator's existing Thrift field 183 identifies its unary
wire codec; old variadic intermediates are always String. The compatibility
adapter changes the codec for those requests, while computation remains V2.

The published session name `enable_aggregate_function_null_v2` remains accepted
as a deprecated, hidden no-op for saved sessions and clients. New FE requests
always send the old protocol field as false, selecting the common String codec
for old V1-only peers and old peers with a V2 switch. Version 20 ignores it. BE no longer
has the corresponding execution attribute or runtime toggle. Field id 183 is
retained for old requests and must never be reused.

## Reproducing verification

The full baseline UT build encounters unrelated Variant test constructor/API
mismatches. The standard runner now supports an optional source regex; its
default remains the complete test target. Runner/support sources are always
included. The focused aggregation run uses:

```bash
DORIS_BE_UT_SOURCE_FILTER='exprs/aggregate/|exprs/function/function_agg_state_finalize_test.cpp|core/data_type/data_type_agg_state_test.cpp|core/data_type_serde/data_type_serde_agg_state_test.cpp|storage/iterator/olap_data_convertor_test.cpp|core/block/block_test.cpp' \
  ./run-be-ut.sh --run --filter="$(cat docs/performance/nullable-aggregation/gtest-filter.txt)" -j 32
```

407 related ASAN UT passed after the review fixes. The new tests cover nonnullable SQL bitmap results,
old-schema marker preservation, mixed nullable variadic arguments, nullable and nonnullable
results, four NULL ratios, native and persistent codecs, selected/nonselected
merge, wide strings, empty batches, all-skipped batches, outer SQL NULL, bitmap
intersection partials, legal historical SUM byte fixtures, PB type and Block
round trips, and the storage converter boundary. Existing lifetime, UNION,
FINALIZE, foreach and scratch tests are reused.

The SQL suite `datatype_p0/agg_state/test_nullable_aggregation_v2_migration`
exercises aggregation phases, parallelism, windows, bitmap partials, STATE /
COMBINE / MERGE / UNION / FINALIZE, persisted numeric/string/bitmap/collection/
array/map states, compaction, and the retired session name. Expected files must
be generated by `run-regression-test.sh`, not edited by hand.

## Performance methodology and results

The historical measurements in PR #60228 are context only, not measurements of
this migration. In particular, its operator timings are not Q78 wall time.

`be/benchmark/benchmark_nullable_aggregation.hpp` provides the same harness for
the reference checkout and migrated code. Arguments are batch rows, NULL
percentage, reference V1/V2 mode, and (for merge) grouping cardinality. It covers
BIGINT and DECIMAL128 SUM/AVG with 0%, 1%, 50%, and 100% NULLs, and 64 versus
4096 groups. `reader_scratch_bytes` reports the declared external scratch
requirement; it is not a malloc allocation measurement.

Build through the normal entry point with RELEASE/O3 in `custom_env.sh`:

```bash
DORIS_BE_BENCHMARK=nullable_aggregation ./build.sh --benchmark -j 32
```

The named benchmark selection excludes unrelated benchmark headers with stale
Variant storage APIs. For the reference checkout only, add
`-DDORIS_NULLABLE_AGGREGATION_REFERENCE` to `EXTRA_CXX_FLAGS`; its mode argument
then selects the original V1 and V2. On the migrated checkout both modes execute
the same V2 implementation. Run from the checkout's output directory:

```bash
./lib/benchmark_test --benchmark_filter='nullable_' \
  --benchmark_min_time=0.2s --benchmark_repetitions=5 \
  --benchmark_out=nullable-aggregation.json --benchmark_out_format=json
```

End-to-end validation additionally requires the same SQL/DDL/data, configuration,
parallelism and hardware for both builds, warmed repeated measurements, Q78,
streaming and low-cardinality cases, complex states, AGG_STATE merge/finalize,
raw query profiles, CPU/allocation measurements and memory peaks. Operator
avg/min/max must be reported separately from SQL wall time.

Measured results, raw profiles, allocation-counter limits and the final acceptance checklist are recorded in [the reproducibility report](performance/nullable-aggregation/README.md).
