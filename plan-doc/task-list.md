# Task List — CI 973411 paimon-SPI regression fixes

Source RCA: `memory/catalog-spi-ci-973411-4fails-rca.md` + workflow `wf_e1c3d93c-22c` (adversarially verified).
Build 973411, HEAD e1d6f88. All 4 test files are byte-identical to master ⇒ all 4 are SPI-migration regressions.

- [x] FIX-1 — test_create_paimon_table: paimon-over-HMS create-db classloader split (PaimonCatalogFactory.assembleHiveConf) — DONE (16/16 UT, checkstyle clean)
- [ ] FIX-2 — test_mysql_mtmv: connector-null NPE during mv_infos scan (PluginDrivenMvccExternalTable.materializeLatest)
- [ ] FIX-3 — test_paimon_mtmv: Duplicated p_NULL partition naming (ListPartitionItem.toPartitionKeyDesc)
- [ ] FIX-4 — test_paimon_table_meta_cache: restore paimon table snapshot cache (PaimonConnector + Connector SPI + fe-core refresh wiring)

Order: 1 → 2 → 3 → 4 (smallest/lightest module first; #4 largest). TDD per fix, independent commit each.
