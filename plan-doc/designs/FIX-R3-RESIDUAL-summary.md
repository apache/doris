# FIX-R3-RESIDUAL — Summary

> Source finding: `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R3 (MINOR, regression).
> Design: `FIX-R3-RESIDUAL-design.md`. Design red-team: `wf_3518653b-3cb` (3 lenses, finder→verifier).

## Problem

`PluginDrivenScanNode.getNodeExplainString` (the generic SPI scan node) re-emitted the VERBOSE per-backend
`backends:` block (per-file path lines + `dataFileNum/deleteFileNum/deleteSplitNum`) **only when**
`"paimon".equals(catalog.getType())`. The parent `FileScanNode` emits it **unconditionally** under
`VERBOSE && !isBatchMode()`.

## Root Cause

The override reimplements the `FileScanNode` explain body (custom format, no `super` call) and re-emits each
inherited line by hand. The VERBOSE backends re-emission was wrongly conjoined with a paimon source-name
gate (FIX-PAIMON-EXPLAIN-GAP `d4526013364`), instead of mirroring the parent gate verbatim. Effects:
1. **MaxCompute (and trino-connector) VERBOSE EXPLAIN regression** — both legacy nodes
   (`MaxComputeScanNode`/`TrinoConnectorScanNode extends FileQueryScanNode`) inherited the unconditional
   block; after SPI cut-over they route through `PluginDrivenScanNode` and lost it.
2. **Layering violation** — a hardcoded source-name branch in the connector-agnostic generic node (project
   rule: emit universal `FileScanNode` info unconditionally; delegate connector-specific via the SPI).
3. **False inline comment** claiming the gate kept "es/jdbc/max_compute VERBOSE output byte-unchanged".

## Fix

`fe/fe-core/.../datasource/PluginDrivenScanNode.java` — removed the
`&& "paimon".equals(desc.getTable().getDatabase().getCatalog().getType())` conjunct, leaving
`if (detailLevel == TExplainLevel.VERBOSE && !isBatchMode())` (identical to the parent gate), and rewrote
the inline comment to state the unconditional-universal rationale. `paimonNativeReadSplits` stays behind the
`ConnectorScanPlanProvider.appendExplainInfo` delegation (unchanged).

### Scope (corrected by red-team — broader than the review's "maxcompute" framing)

`SPI_READY_TYPES = {jdbc, es, trino-connector, max_compute, paimon}` all route through this node. The fix
emits the block for all five:
- **paimon:** unchanged (still emitted; byte-identical).
- **maxcompute, trino-connector:** **restored** pre-cutover legacy behavior.
- **es, jdbc:** **new** (harmless) VERBOSE output — the rule-mandated, consistent choice; same category as
  the sibling `partition=N/M` / `pushdown agg=` lines already emitted unconditionally for them. Paths render
  synthetic (`es://<index>/<shard>`, `jdbc://virtual`); NPE-safe (`PluginDrivenSplit extends FileSplit` →
  always a `FileScanRange`; `getDeleteFiles` null-guarded).

Out of scope (flagged, not fixed): the sibling `"es_http".equals(...file_format_type)` `ES terminate_after:`
gate (R3-LAYER-2) — survives the rule literally (file-format key, not `getType()`); separate residual.

## Tests

New `PluginDrivenScanNodeVerboseExplainTest` (3 tests, `CALLS_REAL_METHODS` + `Deencapsulation` partial-node
pattern, empty `scanRangeLocations` so the loop is skipped and only the bare `backends:` header prints):
- `verboseEmitsBackendsBlockForNonPaimonConnector` (`max_compute`, VERBOSE → contains `backends:`).
- `verboseEmitsBackendsBlockForPaimon` (parity — paimon still emits).
- `nonVerboseOmitsBackendsBlock` (NORMAL → no `backends:`; pins the surviving VERBOSE gate).

**RED→GREEN verified empirically:** with the gate re-added, `verboseEmitsBackendsBlockForNonPaimonConnector`
fails (actual `max_compute` output has no `backends:`); with the gate removed, all 3 pass. The negative
NORMAL-level test passing proves `backends:` is genuinely conditional, so the positive assertions are
meaningful.

## Result

- `PluginDrivenScanNode*Test` (all classes incl. the new one) GREEN; fe-core compiles; checkstyle clean
  (validate phase). Build cache disabled.
- Behavioral change is **diagnostic-only** (VERBOSE EXPLAIN text); zero query/data-path impact. No
  regression test pins these connectors' VERBOSE EXPLAIN text (red-team + independent grep both empty).
- **e2e:** paimon e2e gated (`enablePaimonTest=false`); maxcompute/es/jdbc VERBOSE-EXPLAIN e2e needs live
  endpoints (offline) → none added (documented; fail-loud).
