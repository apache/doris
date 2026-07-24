# Task List — P5 paimon fullpath-review fixes (2026-06-11)

> Source: `plan-doc/reviews/P5-paimon-fullpath-review-2026-06-11.md`.
> Scope = user-selected "BLOCKERs + key MAJORs".
> **Commits HELD** (project rule: no commit unless asked; B7 uncommitted in tree). Implement + test + document each; present grouped diffs at end.
> Per fix: design doc `plan-doc/tasks/designs/P5-fix-<id>-design.md` → impl → build+UT → summary.

## Progress

| # | id | sev | area / file(s) | design | impl | build+UT | status |
|---|----|-----|----------------|--------|------|----------|--------|
| 1 | FIX-STORAGE-CREDS | BLOCKER×2 | PaimonConnectorProperties / applyStorageConfig (s3/oss canonical keys + DLF OSS) | ✅ | ✅ | ✅ (38/0) | ✅ |
| 2 | FIX-REST-VENDED | BLOCKER | SPI vendStorageCredentials + scan-props overlay (REST vended creds → BE) | ✅ | ✅ | ✅ (conn 15/0; fe-core 2/0) | ✅ |
| 3 | FIX-NATIVE-PARTVAL | BLOCKER+MAJOR | PaimonScanPlanProvider (port serializePartitionValue: DATE/LTZ/TIME/BINARY/float + session TZ) | ✅ | ✅ | ✅ (7/0) | ✅ |
| 4 | FIX-CPP-READER | BLOCKER | scan plan / split serialization (enable_paimon_cpp_reader) | ✅ | ✅ | ✅ (12/0) | ✅ |
| 5 | FIX-TZ-ALIAS | MAJOR | PaimonConnectorMetadata (full SHORT_IDS+4 tz alias map) | ✅ | ✅ | ✅ (37/0) | ✅ |
| 6 | FIX-HMS-CONFRES | MAJOR | SPI loadHiveConfResources + buildHmsHiveConf overlay (hive.conf.resources) | ✅ | ✅ | ✅ (42/0 conn; fe-core compiles) | ✅ |
| 7 | FIX-TABLE-STATS | MAJOR | PaimonConnectorMetadata (getTableStatistics override) | ✅ | ✅ | ✅ (4/0) | ✅ |
| 8 | FIX-READ-NOTNULL | MAJOR | PaimonTypeMapping / mapFields (nullable parity) | ✅ | ✅ | ✅ (12/0) | ✅ |

Legend: ⬜ todo / 🔄 in progress / ✅ done

## Notes
- e2e for credential/native-render fixes needs live paimon + S3/OSS/REST infra (CI-skipped) → focus runnable FE **unit tests** (connector module has FakePaimonTable / RecordingPaimonCatalogOps / PaimonCatalogFactoryTest / PaimonScanPlanProviderTest harness). Note live-e2e as gated.
- Confirm each finding against CURRENT code before editing (report is review-only; line numbers may have drifted).
- Connector must not import fe-core (`bash tools/check-connector-imports.sh`).
