# FIX-SHOWCREATE-PLUGIN-PROPS — summary

## Problem
`test_nereids_refresh_catalog`: `SHOW CREATE TABLE` on a JDBC external table emitted `LOCATION ''` +
`PROPERTIES(... "password"=... )` vs the committed `ENGINE=JDBC_EXTERNAL_TABLE;`. A correctness regression and a
JDBC credential leak.

## Root Cause
Branch commit `98a73bf7692` (D-046 paimon parity) added LOCATION+PROPERTIES emission to the SHARED
`PLUGIN_EXTERNAL_TABLE` branch of `Env.getDdlStmt` (`Env.java:4929-4960`), gated only on `!properties.isEmpty()`.
JDBC/ES/Trino tables are plugin-driven with non-empty `getTableProperties()` (connection props incl. credentials), so
they wrongly got the paimon treatment; legacy was comment-only.

## Fix
`Env.java`: gate the LOCATION+PROPERTIES emission additionally on
`TableType.PAIMON_EXTERNAL_TABLE.name().equals(pluginExternalTable.getEngineTableTypeName())` — only the paimon engine
type (the sole plugin-driven connector whose legacy DDL carried LOCATION/PROPERTIES) renders them. JDBC/ES/Trino/
MaxCompute revert to comment-only; the credential leak is closed. Rejected rebaselining the `.out` (would entrench the
leak).

## Tests
- Build: `-pl :fe-core -am compile` → BUILD SUCCESS; fe-core checkstyle clean.
- Adversarial review: VERDICT SOUND — verified paimon (+ its sys-table unwrap) still renders LOCATION/PROPERTIES;
  jdbc/es/trino/maxcompute revert to comment-only matching committed `.out`; `getTableProperties()` has no other DDL
  consumer (leak fully closed); only `"paimon"` maps to `PAIMON_EXTERNAL_TABLE`.
- E2E: `external_table_p0/nereids_commands/test_nereids_refresh_catalog` must pass unchanged against its committed
  `.out` (CI external pipeline).

## Result
Implemented + locally verified (compile/checkstyle/static review). Restores legacy plugin-table DDL and closes the
credential leak.
