# P4-T06e FIX-ISKEY-METADATA — Review Rounds

> Issue P3-10 / NG-6 / F3 / F10 (minor, read/metadata, regression).
> Design: `plan-doc/tasks/designs/P4-T06e-FIX-ISKEY-METADATA-design.md`.
> Flow: design → design-validation workflow → implement → guards → impl-review workflow → commit.

## Decision recap

Cutover marked every MaxCompute column `isKey=false` (5-arg `ConnectorColumn` ctor default), so
`DESCRIBE <mc_table>` showed `Key=NO`; legacy `MaxComputeExternalTable.initSchema` set `isKey=true`
for all columns. **User decision (2026-06-08): Fix — set `isKey=true` (legacy parity)**,
connector-local, no SPI change.

## Round 0 — Design validation (workflow `wa9t0emta`, 3 lenses, clean-room) → 0 mustFix

Lenses: completeness/other-sites · safety/parity · test-quality. **0 mustFix**; design corrections
folded in pre-implementation.

- **completeness** — confirmed exactly 2 `ConnectorColumn` sites (`:138`/`:150`), the single FE
  conversion point (`ConnectorColumnConverter.convertColumn` threads `isKey`), DESCRIBE reads
  `Column.isKey()` via `IndexSchemaProcNode:92`. 1 **shouldFix (real)**: my design wrongly claimed
  `information_schema.columns.COLUMN_KEY` was affected — it is **OlapTable-gated**
  (`FrontendServiceImpl.describeTables:962-965`), empty for MC before+after, legacy never showed it
  either → **scoped the fix to DESCRIBE-only; removed the information_schema assertion** from the
  design/e2e. Noted a 3rd (harmless) `ConnectorColumn` site `PluginDrivenExternalTable:139-140`
  (rename path) that *preserves* `isKey` → folded into design Completeness.
- **safety/parity** — could not break it. 1 nit (isReal=false): `isKey` is **not purely
  display-only** — `UnequalPredicateInfer:278` + BE slot/column descriptors
  (`DescriptorToThriftConverter:67`, `ColumnToThrift:59`) read it non-OLAP-guarded; but legacy fed
  them `true`, so the fix restores exactly what they consumed (every other `isKey` branch is
  OLAP/Schema-guarded and unreachable for MC). **Softened the design's "display-only" wording** to
  "restores exact legacy `isKey=true` all planning/BE paths already consumed".
- **test-quality** — `buildColumn` test is non-vacuous and kills the `isKey true→false` mutation
  (verified). 1 **shouldFix (real)**: the helper test can't catch a call site that *bypasses*
  `buildColumn` (reverts to 5-arg) — `getTableSchema` needs an unmockable live `Table`; **e2e
  DESCRIBE is the load-bearing gate** → acknowledged in design + a Rule-9 "why" comment in the test
  class. 1 nit (real): helper-vs-inline is borderline (the 6-arg ctor's `isKey=true` is already
  pinned by `ConnectorColumnTest:63`) → **kept the helper** for an MC-module mutation guard +
  intent documentation + 2-site centralization (justified in design).

## Guards (post-implementation)

- **Build:** `:fe-connector-maxcompute -am` BUILD SUCCESS (only the connector module touched; no
  SPI/fe-core change).
- **UT:** `MaxComputeConnectorMetadataIsKeyTest` 3/3; collateral pure-unit MC suite (Capability /
  DropDb / ValidateColumns / ScanPlanProvider / BuildTableDescriptor + IsKey) **37/37**, 0
  failures.
- **checkstyle:** 0 violations. **import-gate:** clean.
- **mutation:** `buildColumn` `isKey true→false` → `Tests run: 3, Failures: 2` (kills
  `testBuildColumnMarksKeyTrue` + `testBuildColumnKeyIndependentOfNullable`); restored green.

## Round 1 — Impl review (workflow `wrx0n11ol`, 2 lenses, clean-room) → converged

Lenses: correctness-parity · test-quality. **0 mustFix · 0 shouldFix.** Verdict: ready to commit.

- **correctness-parity** — **0 findings.** Verified on the final code: `buildColumn` sets `isKey=true`
  (6-arg ctor delegation, `isAutoInc=false`); both `getTableSchema` sites route through it (data
  `nullable=col.isNullable()`, partition `nullable=true`); the partition `true` is in the *nullable*
  arg position (not swapped with isKey); the only `new ConnectorColumn` in MC prod is now inside
  `buildColumn`. Exact legacy parity vs `MaxComputeExternalTable:177-178/189-190`. Fix propagates to
  DESCRIBE (`ConnectorColumnConverter:67`, preserved by `PluginDrivenExternalTable:140` rename path).
  Diff is surgical (helper + 2 swaps + import).
- **test-quality** — 2 nits, no blockers. (a) nit isReal=false: independently confirmed the
  `isKey true→false` mutant is killed and all 3 assertions are non-vacuous (`ConnectorType` has a
  real `equals()`); Rule-9 comments factually accurate. (b) nit isReal=true: the call-site wiring
  has no killing unit test (disclosed DV-017); the reviewer noted the "no usable public constructor"
  phrasing was slightly overstated — `TableSchema`/`Column` are public-constructable; the precise
  blocker is `Table`'s **package-private** ctor + no Mockito, and the only offline workaround (a
  `com.aliyun.odps`-package fixture subclass overriding `getSchema()`) has no repo precedent (sibling
  `getColumnHandles` is identically untested). **Folded in: softened the test-class javadoc to the
  precise blocker** (test 3/3 + checkstyle 0 re-confirmed after the edit). No new prod change.

**No prod logic change in Round 1** — only a test-javadoc wording precision.
