# P6-T10 — Metastore module split (filesystem-style) + iceberg per-flavor CREATE-CATALOG validation + live test

> Branch `catalog-spi-10-iceberg`. Status: **DESIGN v2 — awaiting approval of scope/sequencing**.
> Recon: workflow `wf_8ae4353f-9a8` (rules, adversarially verified `complete-and-exact`) + module/pom/assembly survey.
> User decisions so far: (1) full per-flavor validation; (2) validation lives in the shared metastore layer; (3) **restructure metastore into per-engine modules like fe-filesystem**; (4) impls do NOT belong bundled in `-spi` (matches the repo's own filesystem layer where `-spi` is interface-only and impls live in per-backend modules).

## 0. Two separable deliverables
This task now contains a **structural refactor** (A) that is largely orthogonal to a **feature** (B):
- **(A) Metastore module split** — mirror `fe-filesystem`: `-spi` becomes extension-point-only; per-engine impl modules `fe-connector-metastore-paimon` / `fe-connector-metastore-iceberg`. Touches paimon packaging (highest CI-risk area). **Behavior-preserving.**
- **(B) Iceberg validation feature** — per-flavor CREATE-CATALOG rules (verified, §4) in the new iceberg module + `validateProperties` wiring + live test.

**Sequencing (mandatory): finish A and verify paimon green (UT + plugin-zip) BEFORE starting B.** A is a pure move/refactor; if paimon CI/UT/packaging breaks, it is A's fault and must be caught at the A-checkpoint, not entangled with B.

## 1. Goals
- Metastore module layout mirrors `fe-filesystem`: `-api` = contracts, `-spi` = extension point + engine-neutral shared bases, `-paimon`/`-iceberg` = per-engine impls+providers+`META-INF`. (Realizes the user's "MetastoreProperties capability lives in the shared metastore layer, engines as peers".)
- paimon behavior + packaging **byte-identical** (D-062). Verified at the A-checkpoint.
- iceberg `CREATE CATALOG` fails fast with **verbatim legacy messages** per flavor (REST/Glue/JDBC/HMS/DLF), logic in `fe-connector-metastore-iceberg`.
- env-gated `IcebergLiveConnectivityTest`.

## 2. Non-goals
- No flip (iceberg stays out of `SPI_READY_TYPES`; P6.6 only).
- No storage-validation work — already done upstream at fe-filesystem bind (`S3FileSystemProperties.validate()` etc.); connector receives validated `StorageProperties`.
- No scan/cache path (P6.2+).
- No change to the *meaning* of any paimon rule or message.

## 3. Constraints
- `ConnectorProvider.validateProperties(Map)` — prop map only; all iceberg rules are prop-based (verified) ⇒ sufficient.
- Connector import gate (`tools/check-connector-imports.sh`); no fe-core.
- ServiceLoader discovery is **classpath-scoped**: each connector's plugin-zip bundles only its engine's metastore module ⇒ `bindForType(flavor)` resolves within-engine at runtime; **no engine-qualified dispatch arg needed**. (Unit tests that put both engines on one classpath must scope their assertions per-engine.)
- `ParamRules` semantics to reproduce exactly: `isPresent` = trim-non-empty; `requireIf`/`forbidIf` = case-sensitive `Objects.equals`; `requireTogether` = any⇒all; `requireAtLeastOne`.
- No Mockito; fail-loud fakes; WHY + MUTATION test comments.

## 4. Verified legacy iceberg rules to port (verbatim; fire order preserved)
> All throw `IllegalArgumentException` → wrapped to `DdlException` by `PluginDrivenExternalCatalog.checkProperties`.

### REST — observable fire order (security/creds enums first, then eager body-throws, then deferred ParamRules)
1. `Invalid security type: <value>. Supported values are: none, oauth2` — `iceberg.rest.security.type` (case-insensitive) ∉ {none,oauth2}.
2. `Unsupported AWS credentials provider mode: <value>` — `iceberg.rest.credentials_provider_type` (case-insensitive, `-`→`_`) unknown; blank ⇒ DEFAULT (no throw).
3. `OAuth2 scope is only applicable when using credential, not token` — token present AND scope present.
4. `OAuth2 requires either credential or token` — security.type≈oauth2 (ci) AND neither credential nor token.
5. `iceberg.rest.role_arn is not supported for Iceberg REST catalog. Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, or iceberg.rest.credentials_provider_type instead` — `iceberg.rest.role_arn` present.
6. `iceberg.rest.external-id is not supported for Iceberg REST catalog. Use iceberg.rest.access-key-id and iceberg.rest.secret-access-key, or iceberg.rest.credentials_provider_type instead` — `iceberg.rest.external-id` present.
7. `OAuth2 cannot have both credential and token configured` — both present.
8. `Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is glue` — `iceberg.rest.signing-name`==`glue` (exact) ⇒ require signing-region AND sigv4-enabled present.
9. `Rest Catalog requires signing-region and sigv4-enabled set to true when signing-name is s3tables` — same for `s3tables`.
10. `iceberg.rest.access-key-id and iceberg.rest.secret-access-key must be set together` — requireTogether.
- No uri/warehouse requirement. Port as sequential checks in this order.

### Glue — fire order
1. `glue.access_key and glue.secret_key must be set together` (AK aliases `{glue.access_key, aws.glue.access-key, client.credentials-provider.glue.access_key}`; SK `{glue.secret_key, aws.glue.secret-key, client.credentials-provider.glue.secret_key}`).
2. `glue.endpoint must be set` (aliases `{glue.endpoint, aws.endpoint, aws.glue.endpoint}`).
3. `glue.endpoint must use https protocol,please set glue.endpoint to https://...` (present AND !`startsWith("https://")`, case-sensitive; verbatim incl. comma-no-space).
4. `At least one of glue.access_key or glue.role_arn must be set` (AK blank AND `glue.role_arn` blank).
- No warehouse requirement.

### JDBC — parse-time only
1. `Property uri is required.` (`{uri, iceberg.jdbc.uri}`).
2. `Property iceberg.jdbc.catalog_name is required.`
3. `Property warehouse is required.`
- driver_class/url rules are **lazy** (initCatalog) → already covered by connector `preCreateValidation` + `maybeRegisterJdbcDriver`. Not in validateProperties.

### HMS (identical to paimon connection rules; iceberg omits warehouse)
1. `hive.metastore.uris or uri is required` (`{hive.metastore.uris, uri}`).
2. `hive.metastore.client.principal and hive.metastore.client.keytab cannot be set when hive.metastore.authentication.type is simple` (authType==`simple` exact AND principal|keytab present).
3. `hive.metastore.client.principal and hive.metastore.client.keytab are required when hive.metastore.authentication.type is kerberos` (authType==`kerberos` exact AND principal|keytab blank).

### DLF (AK/SK/endpoint only — NO warehouse, NO OSS check; iceberg legacy enforces neither at validate)
1. `dlf.access_key is required` (`{dlf.access_key, dlf.catalog.accessKeyId}`).
2. `dlf.secret_key is required` (`{dlf.secret_key, dlf.catalog.accessKeySecret}`).
3. `dlf.endpoint is required.` (endpoint `{dlf.endpoint, dlf.catalog.endpoint}` blank AND region `{dlf.region, ...}` blank).

### hadoop / s3tables — no metastore rules (storage validated upstream).

## 5. Target module architecture (mirror fe-filesystem)
```
fe-connector-metastore-api      contracts: MetaStoreProperties (+validate() default no-op),
                                 HmsMetaStoreProperties / DlfMetaStoreProperties / RestMetaStoreProperties /
                                 JdbcMetaStoreProperties / FileSystemMetaStoreProperties   [UNCHANGED]
fe-connector-metastore-spi      extension point + shared framework + engine-neutral CONF bases:
                                 MetaStoreProvider, MetaStoreProviders, AbstractMetaStoreProperties,
                                 MetaStoreParseUtils, JdbcDriverSupport,
                                 AbstractHmsMetaStoreProperties (fields + toHiveConfOverrides + validateConnection),
                                 AbstractDlfMetaStoreProperties (fields + toDlfCatalogConf + validateConnection)
                                 (NO concrete engine impls; NO META-INF/services)
fe-connector-metastore-paimon   PaimonHms/Dlf/Rest/Jdbc/Fs (extend bases; validate()=warehouse[+OSS for DLF]
                                 +connection) + their providers + META-INF/services  [MOVED from -spi]
fe-connector-metastore-iceberg  IcebergHms/Dlf/Rest/Jdbc/Glue (extend bases; validate()=iceberg §4 rules;
                                 Hms/Dlf reuse base conf) + their providers + META-INF/services  [NEW]
```
- Shared HMS/DLF **conf** (`toHiveConfOverrides`/`toDlfCatalogConf`) + shared **connection** checks (`validateConnection()`) live once in the `-spi` bases. paimon's `validate()` = `requireWarehouse()` (+ `requireOssStorage()` for DLF) + `validateConnection()`; iceberg's `validate()` = (REST/Glue/JDBC: §4) or (HMS/DLF: `validateConnection()` only). ⇒ zero rule duplication, paimon messages/order byte-identical.
- Dispatch unchanged signature; classpath isolation makes `bindForType(flavor)` resolve within-engine at runtime.
- Connector deps: paimon → `-paimon`; iceberg → `-iceberg`. Both transitively get `-spi`+`-api`. Neither sees the other's impls in its plugin-zip.

## 6. Data flow (flip-time, iceberg)
`CREATE CATALOG type=iceberg` → `checkProperties` → `ConnectorFactory.validateProperties("iceberg",props)` → `IcebergConnectorProvider.validateProperties` → `MetaStoreProviders.bindForType(resolveFlavor(props), props, {})` → (iceberg provider on classpath) → `IcebergXxxMetaStoreProperties.validate()`. hadoop/s3tables ⇒ provider returns impl whose validate() is no-op. (Inert until P6.6.)

## 7. Edge cases
- Blank/unknown `iceberg.catalog.type` → `bindForType` throws (parity w/ connector createCatalog "Missing 'iceberg.catalog.type'"). Need iceberg hadoop/s3tables providers so bindForType doesn't throw for them (return no-op-validate impls).
- REST eager-vs-deferred tie-breaks (rule 3 before 7) preserved by sequential order.
- Glue https check byte-exact lowercase.
- `@ConnectorProperty` binding trims (whitespace ⇒ blank), matching `ParamRules.isPresent`.

## 8. Risks
- **R-A1 (paimon packaging)**: moving impls + `META-INF/services` to `-paimon` and rewiring paimon connector pom/plugin-zip risks the bundle (the project's #1 CI-break area). Mitigation: A-checkpoint = paimon UT green + `unzip -l doris-fe-connector-paimon.zip` shows the moved jars + the dispatch test green.
- **R-A2 (HMS/DLF conf base extraction)**: splitting one impl into base+engine could drift the conf bytes. Mitigation: move conf logic verbatim into the base; existing paimon conf tests must pass untouched.
- **R-A3 (iceberg conf rewire)**: iceberg `bindForType("hms"/"dlf")` now resolves to iceberg providers; their conf must equal what T05/T07 shipped. Mitigation: iceberg Hms/Dlf reuse the SAME base conf; assert conf maps unchanged vs T05/T07 tests.
- **R-B1 (message drift)**: per-message UT with verbatim strings.

## 9. Testing / verification gates
- **A-gate**: `fe-connector-metastore-{spi,paimon}` UT green; paimon connector UT green; `mvn -pl :fe-connector-paimon -am package` → plugin-zip contains the moved metastore-paimon jar + paimon providers discoverable; dispatch test moved to `-paimon` green.
- **B-gate**: `fe-connector-metastore-iceberg` UT (per-flavor verbatim-message + fire-order + dispatch/no-op/unknown); `fe-connector-iceberg` UT green incl. existing conf-parity tests; iceberg plugin-zip contains metastore-iceberg jar, NOT metastore-paimon; `IcebergLiveConnectivityTest` (env-gated). 
- Both: checkstyle 0; `check-connector-imports.sh` net; `grep` iceberg NOT in `SPI_READY_TYPES`; adversarial parity re-review of iceberg impls vs legacy.

## 10. TODO (ordered; A then B)
**Phase A — module split (behavior-preserving, paimon-green checkpoint):**
1. Create `fe-connector-metastore-paimon` module (pom; reactor entry in `fe-connector/pom.xml`).
2. Extract engine-neutral bases into `-spi`: `AbstractHmsMetaStoreProperties` (fields + `toHiveConfOverrides` + `validateConnection`), `AbstractDlfMetaStoreProperties` (fields + `toDlfCatalogConf` + `validateConnection`).
3. Move paimon impls → `-paimon` as `PaimonHms/Dlf/Rest/Jdbc/Fs` (extend bases; validate()=warehouse[+OSS]+connection); move their providers + `META-INF/services`; move the spi tests that assert paimon rules.
4. Slim `-spi`: remove concrete impls/providers/META-INF (keep framework+bases+`MetaStoreProvider(s)`).
5. Rewire paimon connector pom (dep `-paimon`) + plugin-zip.xml.
6. **A-gate verify** (paimon UT + packaging + dispatch). Commit.

**Phase B — iceberg validation feature:**
7. Create `fe-connector-metastore-iceberg` module (pom; reactor entry).
8. `IcebergHms/Dlf` (extend bases; validate()=`validateConnection`) + `IcebergRest/Jdbc/Glue` (validate()=§4) + `IcebergHadoop/S3Tables` no-op-validate (or a shared no-op) + providers + `META-INF/services`.
9. Per-flavor UT (verbatim messages + fire order) + dispatch/no-op/unknown UT.
10. Rewire iceberg connector pom (dep `-iceberg` instead of `-spi`-with-paimon-impls; still gets `-spi`+`-api` transitively) + plugin-zip.xml; verify `IcebergConnector` `bindForType("hms"/"dlf")` conf unchanged vs T05/T07 tests.
11. Wire `IcebergConnectorProvider.validateProperties` → `bindForType(flavor).validate()`; connector acceptance/rejection tests.
12. `IcebergLiveConnectivityTest` (env-gated).
13. **B-gate verify**. 
14. HANDOFF update (T10 DONE) + memory.
