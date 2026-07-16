# Cache-invalidation fixes (R1–R4) — targeted adversarial re-review (2026-07-10)

> Re-review of the four fixes that closed the prior clean-room review's findings
> (`cache-invalidation-cleanroom-review-2026-07-10.md`). Commits `6df649d722d..HEAD`:
> `7b7b3c25953` R2, `a562c91e55b` R1, `43db3e8214f` R4, `d26bfa52eea` R3.
> Run `wf_b730a7d4-6a3`: **6 blind finders** (one per fix R1/R2/R3/R4 + cross-cutting completeness +
> regression/invariance), each finding to be adversarially verified by 3 independent lenses
> (correctness / does-it-reproduce / refute-hardest) and kept only at ≥2/3.
>
> **Verdict: CLEAN. 0 findings raised, 0 survived. The four fixes are correct AND complete — no
> follow-ups required.** (531k subagent tokens, 191 tool calls, 0 agent errors; each finder did
> 11–64 code-grounded tool calls before concluding.) Contrast the prior review, which found the
> *pre-fix* code incomplete; the fixes now fully close all four findings with nothing new introduced.

## What each lens verified (all concluded "no genuine defect")

- **R1 — replay propagation** (drop/create/dropDb). EditLog routing reaches the overrides
  (`OP_DROP_TABLE → ctl.replayDropTable`, `OP_DROP_DB → Env.replayDropDb → externalCatalog.replayDropDb`,
  `OP_CREATE_TABLE → externalCatalog.replayCreateTable`); coordinator and replay key identically;
  `DropInfo` maps correctly; the drop resolves the remote name BEFORE `super` unregisters; the view
  and table drop branches are both covered by the single replay override; `createDb`/`replayCreateDb`
  intentionally not hooked (coordinator parity).
- **R2 — iceberg/paimon `invalidateDb`.** The db-scoped predicate matches exactly the keys
  `beginQuerySnapshot` stores (iceberg `namespace().equals(Namespace.of(db))`, paimon
  `getDatabaseName().equals(db)`); all three callers (`dropDb`, replay, `RefreshManager`) pass the
  REMOTE db name — the same correlation the accepted `invalidateTable` uses; `PaimonConnector.invalidateDb`
  clears BOTH `latestSnapshotCache` AND `PaimonSchemaAtMemo`; `DROP DATABASE FORCE`'s connector-internal
  cascade is covered because the clear is db-scoped; iceberg's path-keyed manifest cache is intentionally
  kept.
- **R3 — guarded put** (dormant). The guard mirrors `getWithManualLoad`'s pre-put + post-put
  (`removeLoadedValue`) checks exactly; a newer concurrent write for the same key is never dropped; the
  captured generation is shared safely across the multi-key loop (any flush bumps the shared counter);
  line 189 is the ONLY raw `put` in `CachingHmsClient` (the other three reads use the guarded `get`
  path); the framework additions are additive (no behavior change for iceberg/paimon, which never call
  them).
- **R4 — RENAME** (coordinator + replay). An atomic swap (`RENAME t→t2; RENAME t3→t`) leaves no stale
  pin — each rename invalidates the source's old remote name (the live read-pin key) plus the target's
  new name, and the vacated local name is unregistered so nothing re-pins it; invalidate-after-mutate is
  safe (a failed rename aborts before the cache is touched); coordinator↔replay keys match; replay never
  force-inits.
- **Completeness (cross-cutting).** No unfixed path of the same class remains on the live surface:
  maxcompute/jdbc/es/trino are SPI-ready but hold NO connector-owned metadata cache; **hudi** (reached by
  the hive gateway's `forEachBuiltSibling`) builds a raw `ThriftHmsClient` and reads fresh, so
  `HudiConnector` correctly needs no `invalidateDb`; every replay hook resolves through
  `getDbForReplay`/`getTableForReplay` (empty when uninitialized), so no follower force-init.
- **Regression / invariance (live paimon/iceberg).** No recursion via `forEachBuiltSibling` (siblings
  clear their own caches, don't re-fan-out; standalone paimon/iceberg don't fan out at all); no NPE /
  deadlock; no cost added to the query hot path (invalidate\* is DDL-only); coordinator↔replay key parity
  holds; the deliberate "target/create name not remote-resolved" is consistent on both sides (not a new
  divergence); `invalidateDb` stays a no-op SPI default (fe-core connector-agnostic, non-caching
  connectors safe).

## Disposition
The connector-cache invalidation follow-up (D1/D2 → R1–R4) is **DONE and validated**. Remaining owed
work is the heterogeneous-HMS docker **e2e** (§5 of the design doc — paimon/iceberg drop+recreate and
rename-swap are testable NOW as live regressions), and the next phase is the atomic HMS cutover (Phase 2
of `hms-cutover-execution-plan-2026-07-10.md`).
