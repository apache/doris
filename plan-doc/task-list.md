# Task List — CI 968994 paimon regression fixes

Build 968994 (commit 3d93f195eff). 32 failures. Root: recent self-contained packaging
commits are internally incomplete + one SPI explain-gap regression. F (hive_ctas) = stale, excluded.

- [x] FIX-A — bundle s3-transfer-manager (Class A: s3 FileIO/AWS SDK interceptor skew; 6 direct + 18 collateral) — `75496c94e36`
- [x] FIX-B — bundle hadoop-huaweicloud (Class B: obs cross-loader cast; paimon_base_filesystem) — `3c7adfe1de1`
- [x] FIX-C — paimon-hive-shade module, relocate thrift (Class C: TFramedTransport NoClassDefFound; 2 tests) — `5ac8c302596`
- [x] FIX-E — PluginDrivenScanNode/PaimonScanPlanProvider explain emission (Class E: 5 explain-mismatch) — `<pending>`

Excluded:
- F — external_table_p0.hive.write.test_hive_ctas_to_doris: pre-existing stale test (auto-partition-name
  truncation #56304 on master), not a branch regression. Track upstream / exclude from gating.
