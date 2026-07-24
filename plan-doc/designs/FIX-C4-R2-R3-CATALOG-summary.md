# FIX-C4 / R2-catalog / R3-catalog — summary (DONE)

> Combined fix for three MINOR findings from `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md`.
> Design: `FIX-C4-R2-R3-CATALOG-design.md`. Single commit ("可合一"). Two red-team passes (design + impl), both clean.

## What changed

| Fix | Change | Files |
|-----|--------|-------|
| **C4** | Thread `Config.hive_metastore_client_timeout_second` (env key) into `HmsMetaStoreProperties.toHiveConfOverrides(String)` instead of the hardcoded `"10"` | `DefaultConnectorContext` (producer), `HmsMetaStoreProperties` (api), `HmsMetaStorePropertiesImpl` (spi), `PaimonConnector` (consumer) |
| **R2-catalog** | `PaimonConnectorProvider.validateProperties` **warns** (no reject, no strip) on dead `meta.cache.paimon.table.*` keys | `PaimonConnectorProvider` |
| **R3-catalog** | `PaimonConnectorMetadata.listDatabaseNames` **rethrows** `RuntimeException("Failed to list databases names, catalog name: <name>", e)` instead of swallowing to `emptyList()` | `PaimonConnectorMetadata` |

Plus 3 stale `{@code …toHiveConfOverrides()}` doc-mentions updated to `(String)` (`KerberosAuthSpec`, `PaimonCatalogFactory` ×2 — doc hygiene, not gated).

## Decisions & facts

- **C4 parity:** `Config.hive_metastore_client_timeout_second` default = `10` (`Config.java:2106`), so the threaded value
  is byte-identical (`"10"`) when `fe.conf` is unset; an operator who raises it (e.g. `60`) without a per-catalog
  `hive.metastore.client.socket.timeout` now gets `60` (legacy behavior), restoring `HMSBaseProperties:204-208`. The
  user-override guard (`raw.get("hive.metastore.client.socket.timeout")` blank-check) is unchanged. Only the HMS branch
  threads the value — DLF (`toDlfCatalogConf`)/JDBC/REST/FS have no socket-timeout default (legacy parity).
  Clean signature change (no test-only overload); 11 call-sites updated (9 in `HmsMetaStorePropertiesTest`, anon impl +
  caller in `MetaStorePropertiesContractTest`).
- **R2 — keys are genuinely dead on the plugin path (empirically proven, two reviews agree):**
  `ExternalTable.getMetaCacheEngine()` returns `"default"` and PluginDriven tables do **not** override it, so a cut-over
  paimon table routes `ExternalMetaCacheMgr.getSchemaCacheValue` to the generic `"default"` cache — never
  `PaimonExternalMetaCache` (engine `"paimon"`). `meta.cache.paimon.table.*` sizes only
  `PaimonExternalMetaCache.tableEntry`, reached solely via `getPaimonTable`/`getLatestSnapshotCacheValue` ← legacy
  `PaimonExternalTable`/`PaimonScanNode`. A design-red-team verifier's "may be live" counter-claim was refuted by this
  `="default"` routing trace. **Warn-only chosen over the report's "strip"** (user-confirmed): strip mutates persisted
  props (SHOW CREATE CATALOG would stop echoing the user's input) and the key is paimon-specific so it belongs in the
  connector, not the connector-agnostic `PluginDrivenExternalCatalog` (the report's cited location — wrong layer per the
  "no source-specific code in the generic SPI layer" rule). Re-validating a dead knob is pointless (report agrees).
- **R3 — rethrow matches legacy exactly** (`PaimonMetadataOps:340`, same message incl. catalog name) **and** every other
  connector (Hive/Hudi/JDBC/MC/Trino all propagate; paimon was the sole swallower). The connector's old comment claiming
  "Full read-vs-DDL parity" while swallowing was false; rewritten. Propagation is clean: the bridge
  `PluginDrivenExternalCatalog.listDatabaseNames:226` does not catch, so the `RuntimeException` reaches DB-init exactly
  as legacy did. `executeAuthenticated` (M-11 Kerberos wrap) preserved. `RuntimeException` is unchecked → no signature
  change; `LOG`/`Collections` imports still used elsewhere.

## Verification

- **Builds:** fe-core `compile` BUILD SUCCESS (DefaultConnectorContext); paimon `package -Dassembly.skipAssembly=true`
  BUILD SUCCESS; metastore-api/spi `test` BUILD SUCCESS.
- **Tests:** paimon **280/0/0** (+1 skip = gated `PaimonLiveConnectivityTest`); `PaimonConnectorValidatePropertiesTest`
  14/0/0 (+1 R2 no-reject); `PaimonConnectorMetadataReadAuthTest` 12/0/0 (R3 migrated swallow→rethrow, M-11 coverage
  kept); `HmsMetaStorePropertiesTest` 16/0/0 (+3 C4); `MetaStorePropertiesContractTest` 3/0/0.
- **Mutation (by construction):** C4 `threadedSocketTimeoutDefaultFlowsThrough` asserts `"60"` (old hardcoded `"10"`
  cannot satisfy); R3 test asserts `assertThrows` (old swallow-to-empty cannot throw). R2 test is a regression-guard
  (warn-only has no behavioral mutation to catch — it pins "do not re-add rejection").
- **checkstyle 0** across all touched modules; `tools/check-connector-imports.sh` exit 0.
- **Red-team ×2:** design (`wf_444e33b9-5c6`, GO-WITH-CHANGES — all corrections folded in) + impl (`wf_b3d35e64-6b9`,
  COMMIT — 0 actionable / 13 self-resolving NITs).
- **e2e:** gated (`enablePaimonTest=false`) — NOT run.

## Out-of-scope (documented, not changed)

- **C4 SPI gate vs legacy enum-name gate:** the SPI guards the socket-timeout default on
  `StringUtils.isBlank(raw.get("hive.metastore.client.socket.timeout"))`; legacy guarded on
  `userOverriddenHiveConfig.containsKey(ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.toString())`. Equivalent for the
  documented key; this predates C4 (C4 only swapped the value `"10"` → threaded default) and the SPI form is the
  more-correct one. Left per Rule 3/7.
- R2 log wording "property/properties {}" reads awkwardly but is accurate — cosmetic, left as-is.
