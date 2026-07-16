# HMS cutover §4.1 — Hive connector Kerberos authenticator: implementation notes (2026-07-07)

> **✅ DONE — landed dormant in commit `e63b03fb490`.** First dormant commit of the cutover build-out (design doc `hms-cutover-retype-design-2026-07-07.md` §4.1). **Decision on §3 = option (C): new neutral `fe-connector-metastore-hms` module** (user sign-off 2026-07-07). Verified: fe-connector-metastore-hms + fe-connector-hive BUILD SUCCESS, `HiveConnectorPluginAuthenticatorTest` 5/5, 0 checkstyle (both modules). Provenance: lead recon + drafting subagent `a419cb99c60a3537d`, build-out subagent `ae17052f2b14afa31`. The remaining sections are the as-built record.

## 1. The blocker (verified)
After the flip, `HiveConnector` gets a `DefaultConnectorContext` whose `executeAuthenticated` = `authSupplier.get().execute(task)` with `authSupplier = () -> NOOP_AUTH` (2-arg ctor, `DefaultConnectorContext.java:100-101,164-166`; `NOOP_AUTH.execute` runs `task.call()` directly). The HMS metastore RPC runs via `ThriftHmsClient` whose `authAction = context::executeAuthenticated` (`HiveConnector.java:105`) → **SIMPLE auth for a Kerberos HMS = silent security downgrade.**

## 2. The fix (settled design — do NOT use iceberg's full context wrapper)
Substitute **only the `AuthAction`** given to `ThriftHmsClient` with a plugin-side Kerberos `doAs`; do **not** wrap the whole `ConnectorContext` in a plugin-loader-pinning `TcclPinningConnectorContext` (iceberg's approach). Reason: `ThriftHmsClient.doAs` (`ThriftHmsClient.java:529-538`) deliberately pins the RPC's TCCL to `ClassLoader.getSystemClassLoader()`; iceberg's wrapper would re-pin to the plugin loader inside `executeAuthenticated` and override it. hadoop + fe-kerberos are bundled **child-first** in the hive plugin (parent-first prefixes are only `org.apache.doris.connector.`/`.filesystem.`), so the plugin's `HadoopAuthenticator` and the plugin's `ThriftHmsClient` RPC share the SAME (plugin) UGI copy — the `doAs` correctly wraps the RPC with **zero** TCCL change.

Two verified corrections to the naive draft:
- `AuthAction.execute` is a **generic** method (`<T> T execute(Callable<T>)`) → a lambda cannot implement it (JLS 15.27.3, which is why the existing code uses a method reference). Use an **anonymous class**.
- `HadoopAuthenticator.doAs` takes `PrivilegedExceptionAction<T>`, not `Callable` → adapt with `callable::call` (mirrors `TcclPinningConnectorContext:108` `auth.doAs(task::call)`).

Authenticator selection must reproduce legacy `HMSBaseProperties.initHadoopAuthenticator` (`HMSBaseProperties.java:152-185`): storage-kerberos (`hadoop.security.authentication=kerberos`) first, else HMS-metastore kerberos via the metastore-spi parser `HmsMetaStoreProperties.kerberos()` (which itself covers the HDFS-kerberos backward-compat fallback). `buildPluginAuthenticator` is `static` + package-visible for KDC-free unit testing (mirrors iceberg).

## 3. ⛔ OPEN DECISION (blocks apply): metastore-parser module home
`MetaStoreProviders.bindForType("hms", …)` + `HmsMetaStoreProperties` need a **registered `MetaStoreProvider`** on the hive plugin classpath. Verified: the HMS provider is registered **only** in `fe-connector-metastore-iceberg` (`IcebergHmsMetaStoreProvider`) and `fe-connector-metastore-paimon` (`PaimonHmsMetaStoreProvider`) — there is **no neutral `fe-connector-metastore-hms`**. Options:
- **(a) depend on `fe-connector-metastore-iceberg`** (subagent default) — zero new code, reuses the canonical parser, but bundles an **iceberg-named** module (and its rest/glue/dlf/jdbc providers) into the **hive** plugin: a dependency-inversion smell at the foundation of the hive migration.
- **(b) depend on `fe-connector-metastore-paimon`** — same smell, different flavor.
- **(c) create a neutral `fe-connector-metastore-hms`** module (extract the HMS provider + `HmsMetaStoreProperties`; iceberg/paimon can later dedup onto it) — cleanest long-term, reused by ALL hive read-SPI, but new-module scope now.
- **(d) build the authenticator inside `fe-connector-hms`** (reuse its existing `HmsClientConfig`/`HmsConfHelper` HiveConf + add `fe-kerberos`), hand-reproducing the small legacy key logic — HMS-native, minimal deps, but bypasses the centralized metastore-spi parser (a minor inconsistency with the established "meta parsing via metastore-spi" pattern).

This choice is reused by the entire hive read-SPI (design §4.2), so it must be settled deliberately, not defaulted.

## 4. Drafted implementation (apply once §3 decided; the metastore import/dep lines follow the chosen option)

### `HiveConnector.java`
- Imports: `HmsMetaStoreProperties`, `MetaStoreProviders` (from the chosen module), `kerberos.HadoopAuthenticator`, `kerberos.KerberosAuthSpec`, `kerberos.KerberosAuthenticationConfig`, `org.apache.hadoop.conf.Configuration`, `java.util.Optional`, `java.util.concurrent.Callable`.
- New const `HADOOP_SECURITY_AUTHENTICATION = "hadoop.security.authentication"`; new fields `volatile HadoopAuthenticator pluginAuth; volatile boolean pluginAuthComputed;`.
- `createClient()` tail: build `HadoopAuthenticator auth = pluginAuthenticator()`; if non-null, `authAction = new ThriftHmsClient.AuthAction(){ public <T> T execute(Callable<T> c) throws Exception { return auth.doAs(c::call);} }`; else `authAction = context::executeAuthenticated` (byte-for-byte unchanged).
- `pluginAuthenticator()` lazy double-checked memo → `buildPluginAuthenticator(properties)`.
- `static HadoopAuthenticator buildPluginAuthenticator(Map<String,String> properties)`: storage-kerberos branch → `getHadoopAuthenticator(buildHadoopConf(properties))`; else `bindForType("hms", properties, emptyMap).kerberos()` present+`hasCredentials()` → `getHadoopAuthenticator(new KerberosAuthenticationConfig(principal, keytab, conf))` with `conf` set `hadoop.security.authentication=kerberos` + `hive.metastore.sasl.enabled=true`; else null.
- `private static Configuration buildHadoopConf(Map)`: plain `new Configuration()` (NOT HiveConf — HiveConf static-init drags hadoop-mapreduce onto the unit-test classpath), `setClassLoader(HiveConnector.class.getClassLoader())`, `properties.forEach(conf::set)`. *(Consider filtering to `hadoop.*`/auth keys rather than dumping all props — minor.)*

### `fe-connector-hive/pom.xml`
Add the chosen metastore module (+ `fe-kerberos` if option (d)). No assembly change for options (a)/(b)/(c): `plugin-zip.xml`'s runtime `dependencySet` bundles the new module + transitives (metastore-api/-spi, fe-kerberos, fe-foundation) into `lib/`; they are hadoop-free (no version clash with the plugin's `hadoop-common`). Verify with a redeploy smoke test.

### Test: `HiveConnectorPluginAuthenticatorTest` (JUnit 5, no Mockito; login is lazy so no KDC)
Mirror `IcebergConnectorPluginAuthenticatorTest`. Assert `buildPluginAuthenticator` returns non-null for storage-kerberos props and for HMS-metastore-kerberos-with-simple-storage props (the blocker case), and null for simple-auth / plain / kerberos-without-creds.

## 5. Residual risks (Rule 12)
- Cannot fully verify without a live KDC — unit tests assert built/not-built only; the real `loginUserFromKeytab` + doAs-wraps-RPC (and that the child-first plugin UGI copy is the one the RPC uses) needs a Kerberized-HMS redeploy smoke (the design's R-002 gate).
- Write-path HDFS Kerberos (`HiveWritePlanProvider`/committer use `context.executeAuthenticated` directly) is NOT covered by this commit — it still resolves to NOOP. It is dormant (P7.3) + full-ACID write is hard-rejected; insert-only write to Kerberos HDFS auth is a follow-up at the write cutover (wrap the write-path context there, where a plugin-loader TCCL pin is appropriate — no ThriftHmsClient SYSTEM-pin conflict on that path).
- Case-sensitivity of the storage-auth value + hive-site.xml-only krb settings not folded into the login conf — both mirror iceberg's existing behavior.
