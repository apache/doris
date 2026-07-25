## Overall: no major issues. Report is well-aligned with the evidence and has correctly folded in all three ADJUSTED corrections. A few minor edits below.

### Punch-list

**[low-med] §5.2 -> internal contradiction:** "全部 8 个写路径 getMetadata 接缝都经同一 funnel,**读写共享同一 memoized ConnectorMetadata**" is stated universally, but §3/HP-2 says jdbc's `buildInsertSql` news up a non-funneled `JdbcConnectorMetadata` — so for jdbc, read and write do NOT share one memoized metadata. -> Scope the sentence to the write-*transaction* connectors (hive/maxcompute/iceberg); add a clause that jdbc `buildInsertSql` is a connector-internal, non-funneled exception (P2, HP-2), correctness-neutral because jdbc write is BE auto-commit / `NoOpConnectorTransaction`.

**[low] §1 -> phrasing vs audit severities:** "唯一接近 P0/P1 的真缺口是 hudi" reads as if mc/es aren't P1, but audits mark MC-1 and ES-F1/F2 as **P1**. -> Soften to "唯一 loop-amplified(iceberg-式)P1 缺口是 hudi;maxcompute/es 为 constant-factor P1",消除与 §5.1/§9 的口径分歧.

**[low] §2 -> unverifiable attribution:** "fe-core 中经 funnel 的 call sites 约 58–59 处(INPUT A/C 计 58,INPUT B 计 59)" — INPUT C lists the seams but states no aggregate of 58. -> Drop the per-input attribution: "约 58–59(计法差异,不影响结论)".

**[low] §2 -> consumer count only partly grounded:** "ConnectorPartitionViewCache 被 hive/iceberg/paimon 用 … 已有 3 个消费者". INPUT D confirms only **hive + paimon**; iceberg's use of the *generic* class is from INPUT A (not in the per-connector JSON). The javadoc-stale ("no consumers yet") claim still holds (≥2 confirmed). -> Attribute iceberg to INPUT A, or state "≥2 confirmed consumers (hive, paimon)".

**[low] §9 cleanup list -> incomplete cite:** hive stale-doc list names `HiveConnector.wrapWithCache` but omits its line (hive audit pins `wrapWithCache:667-668`). -> Add `:667-668`.

### Checks that PASSED (state for confidence)
- **"已通用" vs INPUT C:** matches — INPUT C CONFIRMS the routing half is universal; the report correctly scopes true one-load-per-statement to iceberg only (§3 table, §4(2)) and never claims connectors memoize on the metadata instance. **No overclaim.**
- **Migration status:** none mis-stated; hudi correctly "sibling / not in SPI_READY_TYPES / live", others correctly live.
- **No recommendation built on a knocked-down finding:** the three ADJUSTED corrections (ES-F2 is not a zero-new-cache fix; HD-P03 "3x" is an upper bound; hudi authz-safety rests on empty `getCapabilities`, not the iceberg-specific guard) are all correctly incorporated.
- **Roadmap completeness / no over-engineering:** every real P1 (hudi, maxcompute MC-1, es ES-F1/F2) is in the roadmap; jdbc/es/trino/paimon L1 caching correctly contraindicated, not assigned.
- **authz/session=user (security):** iceberg correctly the sole `SUPPORTS_USER_SESSION`; all 7 others correctly single-identity — **no false negative** (incl. paimon REST vended-token correctly noted as table-level, not user-keyed; gate hard-wired to `IcebergConnector.java` correctly flagged as not protecting a future 2nd session=user connector).