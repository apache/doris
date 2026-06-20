# FIX-R3-RESIDUAL — drop the `"paimon".equals` gate on the VERBOSE backends block

> Single-task loop (AGENT-PLAYBOOK): design → design red-team → implement → impl verify → build+UT → commit → summary.
> Source finding: `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R3 (MINOR, regression, partial→MINOR).
> Project rule: memory `catalog-spi-plugindriven-no-source-specific-code` (no source-name branches in the generic SPI node).

# Problem

`PluginDrivenScanNode.getNodeExplainString` (the generic SPI scan node) re-emits the VERBOSE per-backend
scan-range detail block (`backends:` + per-file `path start/length` lines + `dataFileNum/deleteFileNum/
deleteSplitNum`) only when the catalog type is paimon:

```java
// PluginDrivenScanNode.java:305-309
if (detailLevel == TExplainLevel.VERBOSE && !isBatchMode()
        && "paimon".equals(
                desc.getTable().getDatabase().getCatalog().getType())) {
    appendBackendScanRangeDetail(output, prefix);
}
```

Three defects:

1. **MaxCompute VERBOSE EXPLAIN regression.** The parent `FileScanNode.getNodeExplainString` emits this
   block **unconditionally** for `VERBOSE && !isBatchMode()` (`FileScanNode.java:151-153`). Legacy
   `MaxComputeScanNode extends FileQueryScanNode extends FileScanNode` and did **not** override
   `getNodeExplainString` (verified in git `73832991962^`: class decl only, no override) → it inherited the
   unconditional block. After the SPI cutover MaxCompute routes through `PluginDrivenScanNode`
   (`PhysicalPlanTranslator:737-746`, `instanceof PluginDrivenExternalTable`), whose override does NOT call
   super and gates the block to paimon → cut-over MaxCompute VERBOSE EXPLAIN silently loses the block.
2. **Layering violation.** A hardcoded source-name branch (`"paimon".equals(...getType())`) in the generic
   node shared by every SPI connector. Directly violates the project rule (memory
   `catalog-spi-plugindriven-no-source-specific-code`): universal `FileScanNode` behavior must be emitted
   unconditionally (like the sibling `inputSplitNum` / `partition=N/M` / `pushdown agg=` lines in this very
   method), connector-specific behavior must delegate via `ConnectorScanPlanProvider.appendExplainInfo`.
3. **False comment.** The inline comment (`:299-304`) claims the gate keeps "es/jdbc/max_compute VERBOSE
   output … byte-unchanged" — wrong: it is exactly what regresses MaxCompute.

# Root Cause

`PluginDrivenScanNode.getNodeExplainString` reimplements the `FileScanNode` body (custom
TABLE/CONNECTOR/QUERY/PREDICATES format, so it cannot call `super`) and re-emits each inherited line by
hand. For the VERBOSE backends block the re-emission was wrongly conjoined with a paimon source-name gate
(added by FIX-PAIMON-EXPLAIN-GAP `d4526013364`, with the stated but incorrect intent of "not perturbing
other connectors"), instead of mirroring the parent's gate verbatim (`VERBOSE && !isBatchMode()`).

# Design

Remove the `"paimon".equals(...)` conjunct. The remaining gate `detailLevel == VERBOSE && !isBatchMode()`
is then **identical to the parent `FileScanNode`'s** gate (`FileScanNode.java:151`). Replace the false
comment with the truthful "emit unconditionally for every plugin connector, like the sibling universal
lines" rationale.

```java
if (detailLevel == TExplainLevel.VERBOSE && !isBatchMode()) {
    appendBackendScanRangeDetail(output, prefix);
}
```

`paimonNativeReadSplits` stays where it is — behind the SPI `appendExplainInfo` delegation
(`:315-323`) — so connector-specific EXPLAIN remains connector-side. No change there.

## Who is affected — CORRECTED after design red-team (`wf_3518653b-3cb`)

> The first draft of this doc wrongly scoped the change to "paimon + maxcompute" and claimed "es/jdbc: not
> routed → no change". The red-team **refuted** that with code evidence and I verified it independently.

`CatalogFactory.java:51`: `SPI_READY_TYPES = {"jdbc", "es", "trino-connector", "max_compute", "paimon"}` —
**all five** become `PluginDrivenExternalCatalog` → `PluginDrivenExternalTable` → routed to
`PluginDrivenScanNode` (`PhysicalPlanTranslator:737`). A plain `SELECT … FROM <es|jdbc>_catalog.db.tbl`
reaches the table-scan **else**-branch (only the jdbc-**TVF** uses `PassthroughQueryTableHandle` → the
**if**-branch, unaffected). `supportsBatchScan` defaults `false` (only MaxCompute overrides it), so
`!isBatchMode()` is true for es/jdbc → the gate fires. So removing the conjunct emits the `backends:` block
for **all 5** SPI connectors.

## Why this is safe (no NPE)

- **Always file-based ranges.** `PluginDrivenScanNode` produces only `PluginDrivenSplit` (`extends
  FileSplit`), so every `scanRangeLocations` entry carries a populated `FileScanRange` — exactly what
  `appendBackendScanRangeDetail` dereferences (`locations.getScanRange().getExtScanRange()
  .getFileScanRange().getRanges()`). es/jdbc render a synthetic per-split path (`es://<index>/<shard>`,
  `jdbc://virtual`); no real file, but NPE-safe. (red-team C-SAFE: confirmed, 4 independent verifiers.)
- **`getDeleteFiles` null-guard.** The block calls `getDeleteFiles(rangeDesc)`; the override returns empty
  for a range without table-format params and for a null provider (guarded + unit-tested in
  `PluginDrivenScanNodeDeleteFilesTest`). Non-paimon ranges → `deleteFileNum=0`, no NPE.
- **Empty scan.** If `scanRangeLocations` is empty the loop body never runs → only a bare `backends:` line
  is printed (same as the parent today). No regression vs `FileScanNode`.
- **Batch-mode.** The one range shape with a null `getRanges()` (split-source-only) exists only when
  `isBatchMode()==true`, and the block stays gated by `!isBatchMode()` (cached, shared by both paths).

## Parity / behavioral delta

- **paimon:** unchanged (was already in the gate; still emitted; `test_paimon_deletion_vector_oss` still
  asserts `deleteFileNum`). Byte-identical.
- **maxcompute & trino-connector:** the `backends:` block reappears under VERBOSE — **restores** pre-cutover
  legacy behavior (both legacy nodes extended `FileQueryScanNode` and inherited the unconditional block).
- **es / jdbc:** **NEW** VERBOSE output — their legacy dedicated scan nodes (`EsScanNode` /
  `JdbcScanNode`) did not emit a `FileScanNode` backends block. This is the rule-mandated, consistent
  choice: it is the same category as the sibling universal lines (`partition=N/M`, `pushdown agg=`) that
  this override **already** emits unconditionally for es/jdbc. Accepted (broadened scope, documented here +
  in the commit message). No regression test pins these connectors' VERBOSE EXPLAIN text (red-team grep +
  my independent grep both empty), so nothing breaks.
- **future hudi-SPI:** gains parity with every other `FileScanNode` (correct by the same rule).

# Implementation Plan

Single edit in `fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java`:
1. Replace the comment block `:299-304` (false "GATED to paimon … byte-unchanged" rationale) with the
   truthful unconditional rationale.
2. Drop the `&& "paimon".equals(desc.getTable().getDatabase().getCatalog().getType())` conjunct
   (`:306-307`), leaving `if (detailLevel == TExplainLevel.VERBOSE && !isBatchMode())`.

No SPI signature change, no connector change, no BE change.

# Risk Analysis

- **Behavioral change is diagnostic-only** (EXPLAIN VERBOSE text); zero query/data-path impact (review §R3:
  classification regression, severity MINOR).
- **Restoration, not new behavior**, for the only affected live connector (maxcompute). The block is the
  same code path the parent runs for hive/iceberg/maxcompute-legacy.
- **No NPE risk** (file-based ranges + guarded `getDeleteFiles`), see above.
- **Risk if NOT done:** the source-name branch stands as precedent for the next SPI connector and the
  MaxCompute EXPLAIN regression persists.

# Test Plan

## Unit Tests

> **Decision REVISED after red-team (R3-TEST-1/2):** my first-draft "no UT feasible" claim was refuted.
> `PluginDrivenScanNodeSysHandleTest` already drives a real `PluginDrivenScanNode` end-to-end via
> `create(...)` with a `TestablePluginCatalog` whose catalog **type is a ctor param**, and the bare
> `backends:` header is emitted **unconditionally before** the per-backend loop — so with empty
> `scanRangeLocations` a node-level explain test is cheap and NPE-free. → **Add a UT.**

New: `PluginDrivenScanNodeVerboseExplainTest` (mirrors the `CALLS_REAL_METHODS` + `Deencapsulation.setField`
partial-node technique of `PluginDrivenScanNodeDeleteFilesTest` — only the fields the explain path reads are
injected; `scanRangeLocations` empty so the loop is skipped):
- `verboseEmitsBackendsBlockForNonPaimonConnector` — catalog type `max_compute`, VERBOSE → output contains
  `backends:`. **Kills the mutation**: re-adding `&& "paimon".equals(...getType())` drops the block for a
  non-paimon catalog → red.
- `verboseEmitsBackendsBlockForPaimon` — parity guard: paimon still emits the block.
- `nonVerboseOmitsBackendsBlock` — NORMAL level → no `backends:` (pins the surviving `VERBOSE` gate).

Existing tests stay relevant: `PluginDrivenScanNodeDeleteFilesTest` (NPE-safety of `getDeleteFiles`),
`PluginDrivenScanNodeExplainStatsTest` (static EXPLAIN accounting helpers).

Regression gate: run the `fe-core` compile + the `org.apache.doris.datasource.PluginDrivenScanNode*` test
classes (must stay green) + the paimon connector module tests (no contract touched).

## Out of scope (flagged by red-team, NOT fixed here)

- **R3-LAYER-2:** a sibling connector-specific gate remains in this method — `"es_http".equals(props.get(
  PROP_FILE_FORMAT_TYPE))` for the `ES terminate_after:` line (`~:336`) and the in-node ES limit pushdown.
  It survives the no-source-name rule *literally* (it keys on a file-FORMAT-type property, not
  `getCatalog().getType()`), but is the same *spirit* of in-node connector-specific EXPLAIN. Left as a
  separate pre-existing residual for a future `ConnectorScanPlanProvider.appendExplainInfo` delegation; not
  part of this one-conjunct fix.

## E2E Tests

- paimon: existing `test_paimon_deletion_vector_oss` (asserts `deleteFileNum` present) unchanged — gated
  (`enablePaimonTest=false` default), not run locally.
- maxcompute: no regression test asserts `backends:` for maxcompute (review §R3), and maxcompute e2e needs
  a real MaxCompute endpoint (gated/offline). Adding an e2e is out of reach in this environment → **none
  added**; documented here (fail-loud, Rule 12).
