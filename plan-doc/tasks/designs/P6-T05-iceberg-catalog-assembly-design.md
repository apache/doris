# P6-T05 — Iceberg 5-flavor catalog property assembly (design)

> Branch `catalog-spi-10-iceberg`. Scope = the **5 `CatalogUtil`-built flavors**: REST / HMS / GLUE / HADOOP / JDBC.
> s3tables (T06) + dlf (T07) are bespoke (`new S3TablesCatalog().initialize` / `new DLFCatalog().setConf().initialize`) and out of scope here.
> Iceberg stays OUT of `SPI_READY_TYPES` (no flag flip; only P6.6). UT + checkstyle + import-gate are the only gates now; docker plugin-zip e2e is the real gate at P6.6.

## Problem

`IcebergConnector.createCatalog()` is a skeleton: it copies ALL props (legacy-parity pass-through — fine), sets `catalog-impl`, removes `type`, and filters Hadoop conf by a naive `hadoop./fs./dfs./hive.` prefix. It DROPS the per-flavor **derivation** that legacy fe-core (`AbstractIcebergProperties` + the 7 `Iceberg*Properties#initCatalog`) performs:

- common: manifest-cache keys (`io.manifest.cache-*`), warehouse → `CatalogProperties.WAREHOUSE_LOCATION`
- S3FileIO-from-storage (`s3.*` / `aws.region`) for REST/JDBC/HADOOP
- REST: oauth2 / sigv4-glue / vended-creds header / timeouts / prefix
- GLUE: region/endpoint + AK-SK-provider OR assume-role + S3 creds
- JDBC: user/password/`jdbc.*` passthrough + DriverShim + `iceberg.jdbc.catalog_name` (positional, removed from map)
- HMS: HiveConf via metastore-spi (`hive.metastore.uris` + auth + storage overlay)

The base **copy-all** already matches legacy `getOrigProps()`; the GAP is the **derivation** (Doris alias → iceberg key) + S3FileIO + HMS HiveConf + JDBC mechanics. So T05 = add derivations on top of the existing copy-all base, not rewrite it.

## Two cross-module decisions (CORRECT the stale HANDOFF)

### D-061 — S3FileIO `s3.*`/`aws.*` dialect: connector reads `S3CompatibleFileSystemProperties` (NO new hook)

HANDOFF said "port `toS3FileIOProperties` to the connector" — **not feasible as written** (it reads fe-core `S3Properties` typed getters the connector can't import). But `fe-filesystem-api` **already** exposes `S3CompatibleFileSystemProperties` (JDK-types-only: `getEndpoint/getRegion/getAccessKey/getSecretKey/getSessionToken/getRoleArn/getExternalId/getUsePathStyle/hasAssumeRole/hasStaticCredentials`). S3/OSS/COS/OBS all `implements` it; the connector import allowlist permits `org.apache.doris.filesystem.*`.

So the connector reads the typed getters off `ctx.getStorageProperties()` and builds the iceberg `s3.*`/`aws.*` keys itself (the iceberg key spelling lives in the connector, which depends on the iceberg SDK). Single fe-filesystem source of truth (design D-003), no drift, assume-role preserved. **No new method on `ConnectorContext` or `fe-filesystem-api`.**

Legacy "prefer non-`S3Properties` S3-compatible" selection (honor explicit OSS/COS choice) → connector picks, among `S3CompatibleFileSystemProperties` storages, the first whose `type() != S3` else the first (mirror).

### D-062 — metastore-spi reuse for HMS (and T07 DLF): flavor-explicit `bindForType`

The 5 metastore-spi providers dispatch on the hardcoded `paimon.catalog.type` (`MetaStoreParseUtils.CATALOG_TYPE_KEY`). Iceberg carries `iceberg.catalog.type` → `supports(Map)` never matches → `bind` throws. The binding LOGIC (`HmsMetaStorePropertiesImpl.toHiveConfOverrides` / `DlfMetaStorePropertiesImpl.toDlfCatalogConf`) is metastore-agnostic and **reused as-is**.

Fix = decouple dispatch from the key:
- `MetaStoreProvider` gains `boolean supportsType(String catalogType)` (e.g. `"hms".equalsIgnoreCase(type)`); existing `supports(Map p)` becomes `supportsType(p.get(CATALOG_TYPE_KEY))` → **paimon behavior byte-identical**.
- `MetaStoreProviders.bindForType(String flavor, props, storageHadoopConfig)` iterates providers calling `supportsType(flavor)`.
- iceberg connector resolves its flavor from `iceberg.catalog.type` and calls `bindForType("hms", props, storageConf)`.
- iceberg does NOT call paimon's `validate()` (paimon-ism: `requireWarehouse` for all flavors; iceberg HMS does not require warehouse). It only uses `toHiveConfOverrides()`.

## Verified iceberg-SDK constant values (parity-test literals)

| const | value |
|---|---|
| `CatalogProperties.CATALOG_IMPL` | `catalog-impl` |
| `CatalogProperties.WAREHOUSE_LOCATION` | `warehouse` |
| `CatalogProperties.URI` | `uri` |
| `CatalogProperties.IO_MANIFEST_CACHE_ENABLED` | `io.manifest.cache-enabled` (DOTTED; default false) |
| `…_EXPIRATION_INTERVAL_MS` | `io.manifest.cache.expiration-interval-ms` |
| `…_MAX_TOTAL_BYTES` | `io.manifest.cache.max-total-bytes` |
| `…_MAX_CONTENT_LENGTH` | `io.manifest.cache.max-content-length` |
| `CatalogUtil.ICEBERG_CATALOG_TYPE` | `type` (removed before build) |
| `ICEBERG_CATALOG_{REST,HIVE,HADOOP,GLUE,JDBC}` | the 5 impl FQCNs |
| `S3FileIOProperties.{ENDPOINT,PATH_STYLE_ACCESS,ACCESS_KEY_ID,SECRET_ACCESS_KEY,SESSION_TOKEN}` | `s3.endpoint` / `s3.path-style-access` / `s3.access-key-id` / `s3.secret-access-key` / `s3.session-token` |
| `AwsClientProperties.{CLIENT_REGION,CLIENT_CREDENTIALS_PROVIDER}` | `client.region` / `client.credentials-provider` |
| `AwsProperties.{CLIENT_FACTORY,CLIENT_ASSUME_ROLE_ARN,…_EXTERNAL_ID,…_REGION,REST_ACCESS_KEY_ID,REST_SECRET_ACCESS_KEY,REST_SESSION_TOKEN}` | `client.factory` / `client.assume-role.*` / `rest.*` |
| `OAuth2Properties.{CREDENTIAL,TOKEN,SCOPE,OAUTH2_SERVER_URI,TOKEN_REFRESH_ENABLED}` | `credential` / `token` / `scope` / `oauth2-server-uri` / `token-refresh-enabled` |

> ⚠ recon agent guessed `client.credentials.credential`/`io-manifest-cache-enabled` — WRONG. Use the SDK constants directly in code; read legacy `IcebergRestProperties` for the exact emitted keys + input aliases per flavor before writing each flavor's test.

## Structure (mirror paimon)

`IcebergCatalogFactory` (PURE static, offline-testable — plain Map/`S3CompatibleFileSystemProperties` in, Map out):
- `resolveFlavor` / `resolveCatalogImpl` — exist
- `appendCommonProperties(props, opts)` — warehouse + manifest cache (verified dotted keys)
- `appendS3FileIOProperties(opts, chosenS3)` — `S3CompatibleFileSystemProperties` → `s3.*`/`aws.region`/assume-role
- `chooseS3Compatible(List<StorageProperties>)` — prefer non-S3 subtype
- per-flavor: `appendRestProperties` / `appendGlueProperties` / `appendJdbcProperties` (HMS/HADOOP need no extra catalog-prop derivation beyond common+S3FileIO+impl)
- `stripDorisType(opts)` — remove `type`, assert `catalog-impl` present (mirror legacy `buildIcebergCatalog` precondition)

`IcebergConnector.createCatalog()` (LIVE): resolve flavor → base copy-all + `appendCommonProperties` + impl → per-flavor switch (REST/GLUE/JDBC append; HMS `bindForType` → HiveConf as conf; HADOOP/JDBC build Hadoop conf + S3FileIO from storage) → TCCL-pin + `executeAuthenticated` → `CatalogUtil.buildIcebergCatalog`. JDBC: DriverShim register + `catalog_name` positional.

## Test plan (no Mockito; assert assembled prop MAPS vs legacy literal keys — not just class names)

Extend `IcebergCatalogFactoryTest` (pure): per-flavor `appendX` emits the exact literal keys/values; manifest-cache dotted keys; S3FileIO maps `S3CompatibleFileSystemProperties` getters (fake impl) → `s3.*`/`aws.region` incl assume-role; `chooseS3Compatible` prefers non-S3. New `FakeS3CompatibleStorageProperties` test double. metastore-spi: `bindForType("hms",…)` returns HMS impl, paimon `supports(Map)` unchanged (add test in metastore-spi module). Each assertion carries a WHY + mutation that reddens it (Rule 9).

## Risks (UT-invisible; P6.6 docker only)
- glue `com.amazonaws.glue.catalog.credentials.*` provider class origin still unconfirmed (T04 R-2) — wire keys per legacy, runtime-verify at cutover.
- HMS HiveConf cross-loader identity + thrift relocate (T04 R-1) — same as paimon B1 cutover-blocker.
- field-id loss in `ConnectorColumn` (P6.2+ scan) — not T05.
