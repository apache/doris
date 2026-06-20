# Problem

Build 968994 (Class C). Paimon catalogs with `metastore=hive` (HMS-backed) fail at
**catalog create** with:

```
java.lang.NoClassDefFoundError: org/apache/thrift/transport/TFramedTransport
  at java.lang.Class.getDeclaredConstructors0(Native Method)
  at org.apache.paimon.hive.RetryingMetaStoreClientFactory
       .constructorDetectedHiveMetastoreProxySupplier(RetryingMetaStoreClientFactory.java:199)
  ... HiveClientPool ... CachedClientPool ...
```

Affected regression tests (docker `enablePaimonTest=true`):
- `regression-test/.../external_table_p0/paimon/test_paimon_table.groovy`
  → `test_create_paimon_table` (line 44), uses a `metastore=hive` paimon catalog.
- `regression-test/.../external_table_p0/paimon/test_paimon_statistics.groovy` (line 33),
  same HMS-backed catalog.

`test_paimon_jdbc_catalog.groovy` uses `metastore=jdbc` and is **not** affected (no Thrift
metastore client involved).

---

# Root Cause

## The two thrift consumers that share one plugin classloader

The paimon plugin (`fe/plugins/connector/paimon/lib/*.jar`) is loaded by
`org.apache.doris.common.util.ChildFirstClassLoader`. That loader is **purely child-first
with no parent-first allowlist** (verified — `ChildFirstClassLoader.loadClass` always tries
`findClass` over the plugin jars first for *every* class, and only delegates to the parent on
`ClassNotFoundException`). A class therefore resolves parent-first **only if it is absent from
the plugin lib**.

Two code paths in the same plugin both want package `org.apache.thrift.*`:

1. **doris-gen Thrift serialization (RC-1 path).** `PaimonScanPlanProvider` calls
   `org.apache.thrift.TSerializer.serialize()` on `TFileScanRangeParams`, which implements the
   host fe-thrift **0.16.0** `org.apache.thrift.TBase`. This must resolve **parent-first**
   against the host `fe/lib/libthrift-0.16.0.jar` so the `TSerializer`, `TBase`, and the
   doris-gen type all come from one loader. RC-1 (commit `f5b787c5f15`) fixed an
   `IncompatibleClassChangeError` here by **excluding** `org.apache.thrift:libthrift` from the
   plugin (pom exclusion + `plugin-zip.xml` exclude), so `org.apache.thrift.TBase` is absent
   from the plugin and falls through to the parent 0.16.0.

2. **The paimon HMS Thrift metastore client (the failing path).** For `metastore=hive`,
   paimon's `org.apache.paimon.hive.RetryingMetaStoreClientFactory` reflectively enumerates
   the constructors of `org.apache.hadoop.hive.metastore.HiveMetaStoreClient`
   (`Class.getDeclaredConstructors0` at `RetryingMetaStoreClientFactory.java:199`). The bundled
   `hive-metastore-2.3.7.jar`'s `HiveMetaStoreClient.class` references the **thrift-0.9.x
   package** `org.apache.thrift.transport.TFramedTransport` in its constructor/method
   signatures. Resolving those signatures forces the JVM to load `TFramedTransport`.

## Why TFramedTransport is missing

- Host `fe/lib/libthrift-0.16.0.jar` moved `TFramedTransport` to a **new package**:
  it contains only `org/apache/thrift/transport/layered/TFramedTransport.class`. The
  **old** `org/apache/thrift/transport/TFramedTransport.class` is **absent** (verified).
- The plugin lib (verified contents) bundles `paimon-hive-connector-3.1-1.3.1.jar` and
  `hive-metastore-2.3.7.jar` (whose `HiveMetaStoreClient.class` references the old-package
  `org/apache/thrift/transport/TFramedTransport`) but bundles **no libthrift** at all.
- So `TFramedTransport` is absent from the plugin (→ delegate to parent) **and** absent from
  the parent 0.16.0 (moved package) → `NoClassDefFoundError`.

## Why the current RC-5 bundling does not help, and why the obvious "just add old libthrift" does not work

The current state (RC-5, `7841830809b`) bundles raw `hive-metastore-2.3.7.jar` +
`hive-common-2.3.9.jar` + raw `paimon-hive-connector-3.1-1.3.1.jar` with **original-package**
`org.apache.thrift.*` references throughout (verified: `CachedClientPool` references
`org.apache.thrift.TException`; `HiveMetaStoreClient` references
`org.apache.thrift.transport.TFramedTransport`).

You cannot satisfy both consumers at the original package in one loader:
- Bundling old `libthrift-0.9.3` (original package) into the plugin would supply
  `TFramedTransport` and fix the HMS path — **but** it would also put original-package
  `org.apache.thrift.TBase` into the plugin, which now loads **child-first** and splits from
  the host 0.16.0 `TBase`/doris-gen `TFileScanRangeParams` → re-introduces exactly the RC-1
  `IncompatibleClassChangeError`. This is the trap the pom comment at line ~156 names ("stays
  parent-first like the other connectors").
- Keeping libthrift parent-first (current state) means the HMS path's old-package
  `TFramedTransport` is unsatisfiable.

The conflict is structural: **one original `org.apache.thrift.*` namespace, two incompatible
versions required.** The fix must move the HMS client's thrift to a *different* package so the
two consumers stop sharing a namespace.

## The codebase already solved this exact problem (decisive precedent)

`org.apache.doris:hive-catalog-shade` (module pom at the doris-shade tree; verified copy at
`/mnt/disk1/yy/git/doris-shade/hive-catalog-shade/pom.xml`) uses `maven-shade-plugin` to:
- bundle `hive-metastore:3.1.3` (`HiveMetaStoreClient` at **original** hive package) **and**
  `paimon-hive-connector-3.1` + `paimon-hive-common` (`paimon.version` = **1.3.1**, the exact
  artifact we ship), and
- **relocate** `org.apache.thrift` → `shade.doris.hive.org.apache.thrift`.

Verified in `hive-catalog-shade-3.1.1.jar` / `-3.1.2-SNAPSHOT.jar`:
- paimon `CachedClientPool` → `shade.doris.hive.org.apache.thrift.TException` (relocated)
- `HiveMetaStoreClient` → `shade.doris.hive.org.apache.thrift.transport.TFramedTransport`
  (relocated, **present** in the jar)
- **No** original-package `org.apache.thrift.*` class anywhere in the shade jar.

So when the shaded `HiveMetaStoreClient`'s constructors are reflected, the JVM loads
`shade.doris.hive.org.apache.thrift.transport.TFramedTransport` — which exists in the jar — and
the doris-gen `TSerializer`/`TBase` 0.16.0 path is left completely untouched (it never touches
`shade.doris.hive.*`).

**Caveat that rules out "just depend on the existing shade as-is":** `hive-catalog-shade-3.1.1`
(the version pinned by `doris.hive.catalog.shade.version=3.1.1`) bundles **un-relocated,
original-package fastutil 6.5.x** (the fastutil relocation was only added in the unreleased
3.1.2-SNAPSHOT pom). Bundled into our **child-first** plugin, that ancient
`it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap` would shadow the modern
`fastutil(-core)` on the FE classpath → `NoSuchMethodError` (exactly the collision the paimon
pom comment at lines 135-139 already avoids by using plain `hive-common` instead of the shade).
The existing shade jar is also ~127 MB (hive-exec core + iceberg-hive-metastore + DLF), most of
which the paimon plugin does not need.

---

# Design

## Option evaluation

### Option (b) — disable the TFramedTransport constructor probe via config — REJECTED
The decompiled `RetryingMetaStoreClientFactory` (verified bytecode) has **no flag** that skips
the constructor probe. `createClient` iterates a fixed `PROXY_SUPPLIERS` map; the
`constructorDetectedHiveMetastoreProxySupplier` entry that triggers `getDeclaredConstructors0`
is hard-wired. The `PROXY_SUPPLIERS_SHADED` map is *added* (not substituted) and only when the
target class name equals the original `HiveMetaStoreClient` name; it does not avoid loading
`HiveMetaStoreClient`'s constructor signatures. **No safe config switch exists.** Even if the
probe were skipped, the actual client instantiation still loads `TFramedTransport` at connect
time. Rejected.

### Option (c) — lighter alternatives — REJECTED
- *Bundle old `libthrift-0.9.3` original-package*: re-breaks RC-1 (TBase split). Rejected.
- *Bundle host 0.16.0 libthrift child-first*: 0.16.0 lacks old-package `TFramedTransport`; does
  not fix the HMS path and additionally risks the TBase split. Rejected.
- *Hand-relocate only `TFramedTransport`*: the metastore client transitively touches a large
  thrift surface (`TSocket`, `TTransport`, `TBinaryProtocol`, `TException`, ...); partial
  relocation produces an inconsistent namespace. Must relocate the whole `org.apache.thrift`
  tree, which is what shading does. Rejected as a manual variant of (a).
- *Depend on the existing `hive-catalog-shade-3.1.1` artifact directly*: fastutil-6.5.x
  collision (above) + 127 MB. Rejected. (Bumping the pinned shade to 3.1.2-SNAPSHOT to get the
  fastutil relocation is possible but couples paimon to an unreleased shade and still ships the
  127 MB hive-exec/iceberg payload — not preferred.)

### Option (a) — new paimon-scoped shaded module — CHOSEN

Create a small, paimon-dedicated shade module that bundles **only** the paimon-hive + Thrift
metastore-client closure and relocates `org.apache.thrift` (and fastutil, defensively) to a
**paimon-private prefix**. `fe-connector-paimon` depends on this shaded artifact instead of raw
`paimon-hive-connector-3.1` + `hive-metastore-2.3.7` + `hive-common`. The main module keeps
its own original-package 0.16.0 thrift path (parent-first, untouched).

**Why a SEPARATE module and not shade `fe-connector-paimon` itself:** shading the connector
module would relocate `org.apache.thrift` *everywhere*, including
`PaimonScanPlanProvider`'s `org.apache.thrift.TSerializer` call on the host doris-gen
`TFileScanRangeParams` (which implements host 0.16.0 `org.apache.thrift.TBase`). Relocating that
to `org.apache.doris.paimon.shaded.thrift.TSerializer` while `TFileScanRangeParams` stays
`org.apache.thrift.TBase` would break serialization (no `TSerializer` for the host TBase). The
relocation must be confined to the metastore-client dependency tree, which a separate shade
module achieves cleanly. (This is the same reason `hive-catalog-shade` is its own module rather
than shading fe-core.)

## Module: `fe-connector-paimon-hive-shade`

Location: `fe/fe-connector/fe-connector-paimon-hive-shade/` (sibling of
`fe-connector-paimon`). Registered in `fe/fe-connector/pom.xml` `<modules>` **before**
`fe-connector-paimon` (build-order: the connector depends on it).

Coordinates: `org.apache.doris:fe-connector-paimon-hive-shade:${revision}`, packaging `jar`.

**Relocation prefix:** `org.apache.doris.paimon.shaded.thrift`
(distinct from `shade.doris.hive.org.apache.thrift` so it never collides with a parent-first
hive-catalog-shade should both ever coexist; paimon-private).

**Bundled (shaded-in) deps:**
- `org.apache.paimon:paimon-hive-connector-3.1:${paimon.version}` (1.3.1) — supplies
  `org.apache.paimon.hive.HiveCatalogFactory`, `HiveCatalog`, `RetryingMetaStoreClientFactory`,
  `CachedClientPool`, etc.
- `org.apache.hive:hive-metastore:2.3.7` (current RC-5 version, with the same server-side
  exclusions already in the connector pom: datanucleus/derby/bonecp/HikariCP/jdo/hbase/tephra,
  the stale hadoop-2.7.2 trio, guava, protobuf, logback/log4j12). The 2.3.7 `HiveMetaStoreClient`
  is the one whose `TFramedTransport` reference must be relocated.
- `org.apache.hive:hive-common:${hive.common.version}` (2.3.9) — supplies `HiveConf`. Bundling
  it here (instead of separately in the connector) keeps `HiveConf`, `HiveMetaStoreClient`, and
  paimon's factory **one consistent hive version (2.3.x)** inside one artifact, so the
  reflective `getProxy(HiveConf, ...)` / constructor signatures match by class identity.
- libfb303 rides transitively (paimon/hive metastore need it).
- `org.apache.thrift:libthrift:0.9.3` — **bundled and relocated**. This is the source of
  old-package `TFramedTransport`; after relocation it becomes
  `org.apache.doris.paimon.shaded.thrift.transport.TFramedTransport`, matching the relocated
  references in the shaded `HiveMetaStoreClient`/paimon classes. (libthrift's transitive
  `httpcore`/`httpclient` go to `provided`/excluded as hive-catalog-shade does.)

**Provided / excluded (NOT shaded in)** — resolved at runtime from the plugin's own child-first
lib or the host (must NOT be duplicated/relocated):
- `org.apache.hadoop:*` (hadoop-common / hadoop-client-api / hadoop-aws already bundled in the
  connector plugin; `Configuration`/`HiveConf`-vs-`Configuration` identity stays with the
  plugin's hadoop) → `<exclude>org.apache.hadoop:*</exclude>` in `artifactSet`.
- `org.apache.paimon:paimon-core` / `paimon-common` / `paimon-format` → **excluded** from the
  shade (they come from the connector plugin; paimon-core must stay one copy). Only
  `paimon-hive-connector-3.1` (the hive-metastore glue) is shaded here.
- `org.slf4j:*`, `org.apache.logging.log4j:*`, `commons-logging:*` → excluded (host).
- `com.google.guava:*`, `com.google.protobuf:*` → excluded (host/plugin).
- `org.apache.commons:*`, `commons-io:*`, `commons-codec:*` → excluded (host/plugin).

**Relocations:**
```xml
<relocation>
  <pattern>org.apache.thrift</pattern>
  <shadedPattern>org.apache.doris.paimon.shaded.thrift</shadedPattern>
</relocation>
<!-- defensive: hive-metastore 2.3.x drags an ancient fastutil; relocate it so it cannot
     shadow the modern fastutil on the FE classpath (NoSuchMethodError). -->
<relocation>
  <pattern>it.unimi.dsi.fastutil</pattern>
  <shadedPattern>org.apache.doris.paimon.shaded.fastutil</shadedPattern>
</relocation>
```
(`createDependencyReducedPom>false` is fine for an internal artifact; add the standard
`META-INF/*.SF|DSA|RSA` + `META-INF/maven/**` filter as hive-catalog-shade does.)

**Crucially do NOT relocate** `org.apache.paimon.*` (paimon classes stay at their real
package so the connector's SPI discovery of `org.apache.paimon.hive.HiveCatalogFactory` and the
`Catalog`/`HiveCatalog` types still line up with `paimon-core`) and **do NOT relocate**
`org.apache.hadoop.*` (so the shaded `HiveMetaStoreClient`'s `Configuration`/`HiveConf` are the
same classes the plugin's hadoop-common + this module's hive-common define).

## How `fe-connector-paimon` changes

In `fe/fe-connector/fe-connector-paimon/pom.xml`:
- **Remove** the raw `org.apache.paimon:paimon-hive-connector-3.1` dependency (lines 82-85).
- **Remove** the raw `org.apache.hive:hive-metastore:2.3.7` dependency block (lines 159-192)
  including its long exclusion list.
- **Remove** the raw `org.apache.hive:hive-common` dependency (lines 140-143) — `HiveConf` now
  comes (relocated-thrift-free, hive-2.3.9) from the shade module.
- **Add** `org.apache.doris:fe-connector-paimon-hive-shade:${project.version}`.
- Keep the `<exclude>org.apache.thrift:libthrift</exclude>` in both the pom (n/a now — no
  hive-metastore dep to exclude from) and **keep** the `plugin-zip.xml` exclude of
  `org.apache.thrift:libthrift` and `org.apache.doris:fe-thrift` (unchanged — the doris-gen
  TBase path still needs parent-first 0.16.0). The shade module carries its thrift relocated, so
  there is no original-package `org.apache.thrift.*` introduced into the plugin by this change.

`plugin-zip.xml` already bundles all non-excluded runtime deps into `lib/`, so the new shade jar
lands in `fe/plugins/connector/paimon/lib/` automatically.

## Interaction with RC-1 (the TBase split) — preserved

The plugin after this change contains:
- `org.apache.thrift.*` (the doris-gen serialization namespace): **absent** from the plugin
  (libthrift still excluded) → resolves parent-first to host 0.16.0. `PaimonScanPlanProvider`'s
  `TSerializer.serialize(TFileScanRangeParams)` keeps working. ✅
- `org.apache.doris.paimon.shaded.thrift.*` (the HMS client namespace): present in the shade
  jar, loaded child-first, self-consistent (paimon hive + HiveMetaStoreClient + libthrift 0.9.3
  all relocated to it). The doris-gen path never references this namespace. ✅

No original-package `org.apache.thrift.*` is added to the plugin → **RC-1 cannot regress.**

---

# Implementation Plan

1. **Create module** `fe/fe-connector/fe-connector-paimon-hive-shade/pom.xml`:
   - parent `org.apache.doris:fe-connector:${revision}`, artifactId
     `fe-connector-paimon-hive-shade`, packaging `jar`.
   - dependencies: `paimon-hive-connector-3.1` (`${paimon.version}`, exclude hadoop-common/hdfs,
     hive-metastore [we pin 2.3.7 ourselves], jackson-yaml, httpclient5, RoaringBitmap — mirror
     the existing connector exclusions), `hive-metastore:2.3.7` (server-side exclusions as in the
     current connector pom lines 163-191), `hive-common:${hive.common.version}`,
     `libthrift:0.9.3`. Mark `hadoop-common`, `paimon-core`, slf4j/log4j as `provided`.
   - `maven-shade-plugin` execution copying the hive-catalog-shade pattern: `artifactSet`
     excludes (hadoop, paimon-core/common/format, guava, protobuf, slf4j, log4j, commons-*,
     gson, jackson), the `META-INF` filter, and the two relocations above.
2. **Register module** in `fe/fe-connector/pom.xml` `<modules>` *before* `fe-connector-paimon`.
   Add a `dependencyManagement` entry for `libthrift:0.9.3` and (if not present)
   `hive-metastore:2.3.7` near the paimon entries in `fe/pom.xml`, or pin versions inline in the
   shade module.
3. **Edit** `fe/fe-connector/fe-connector-paimon/pom.xml`: swap raw
   paimon-hive-connector/hive-metastore/hive-common for the shade dependency (above).
4. **No production Java change.** `PaimonCatalogFactory.buildHmsHiveConf/buildDlfHiveConf` use
   only `new HiveConf()` + `HiveConf.set(k,v)` (verified) — version-agnostic; the relocation is
   transparent to that code because paimon (`org.apache.paimon.*`) and hadoop/hive
   (`org.apache.hadoop.hive.conf.HiveConf`) packages are *not* relocated.
5. **Build + unzip verification** (see Test Plan).
6. **Docker external suite** (`enablePaimonTest=true`) is the real gate.

Files touched:
- NEW `fe/fe-connector/fe-connector-paimon-hive-shade/pom.xml`
- `fe/fe-connector/pom.xml` (`<modules>` + version mgmt)
- `fe/fe-connector/fe-connector-paimon/pom.xml` (dependency swap)
- possibly `fe/pom.xml` (dependencyManagement for libthrift 0.9.3 / hive-metastore 2.3.7)

---

# Risk Analysis

1. **Thrift 0.9.3 vs host 0.16.0 wire handshake (already flagged by RC-5).** The metastore
   client now speaks Thrift **0.9.3** to the CI docker HMS. HMS's TBinaryProtocol/TSocket wire
   format is stable across 0.9.x↔0.16 for the metastore RPCs in practice, and the legacy fe-core
   path already used a 2.3.x metastore client over an old thrift against the same docker HMS — so
   this is the same wire version legacy shipped, not a new risk introduced here. **Not statically
   provable; gated by the docker paimon suite.** (Identical caveat to the RC-5 comment.)

2. **Relocation must not break `RetryingMetaStoreClientFactory`'s reflection.** The factory
   reflects on hive classes by **original** name (`HiveMetaStoreClient`,
   `RetryingMetaStoreClient`, `HiveMetaHookLoader`, `HiveConf`) — these are **not** relocated, so
   `Class.forName`/`getMethod("getProxy", ...)` still match. The thrift classes it touches only
   transitively (via `HiveMetaStoreClient` constructor signatures) **are** relocated, **and** the
   shaded `HiveMetaStoreClient` bytecode references the **same** relocated names (verified in
   hive-catalog-shade that shade rewrites both consistently). Maven-shade rewrites bytecode
   references and signatures together, so the relocation is internally consistent. **Low risk**,
   backed by the working hive-catalog-shade precedent that ships the identical paimon 1.3.1 +
   metastore + relocated-thrift combination.

3. **DLF `ProxyMetaStoreClient` path** (`PaimonCatalogFactory:428` sets
   `metastore.client.class = com.aliyun.datalake.metastore.hive2.ProxyMetaStoreClient`). The DLF
   client is **not** in this shade module (it lives in `metastore-client-hive3` / DLF SDK, not
   bundled in the paimon plugin today either). DLF was already a cutover-gated unknown (pom NOTE
   lines 175-182). This fix does not regress DLF, but **does not add DLF either** — DLF remains
   gated by live-e2e and is out of scope for the HMS `NoClassDefFound` fix. Flag for the DLF
   ticket: when DLF is wired, its `ProxyMetaStoreClient` references original-package
   `org.apache.thrift.*`; it would need to be relocated together (added to this shade module's
   artifactSet) to stay consistent.

4. **Fastutil collision** — neutralized by the defensive `it.unimi.dsi.fastutil` relocation in
   this module (the reason we build a paimon-scoped shade instead of reusing
   hive-catalog-shade-3.1.1 which ships un-relocated fastutil).

5. **Two paimon-hive copies?** The shade jar contains `org.apache.paimon.hive.*` (not relocated).
   The connector plugin must NOT also carry a raw `paimon-hive-connector-3.1.jar` (we remove that
   dep in step 3). Verify post-build that `paimon-hive-connector-3.1-*.jar` is **gone** from
   `lib/` and only the shade jar provides `org.apache.paimon.hive.*` — otherwise a child-first
   duplicate-class hazard. (paimon-**core** stays as its own jar; the shade excludes it.)

6. **HiveConf class identity across the plugin.** The shade bundles hive-common 2.3.9 `HiveConf`;
   the connector's `buildHmsHiveConf` constructs `new HiveConf()` resolved child-first from the
   shade. Because both the `HiveConf` instance and the `getProxy(HiveConf,...)` signature come
   from the same (shaded) hive-2.3.9, identity matches. **Low risk**; verify no second
   `org.apache.hadoop.hive.conf.HiveConf` remains in `lib/` after removing the raw hive-common
   dep.

---

# Test Plan

## Unit Tests

- The existing `fe-connector-paimon` UTs (`PaimonCatalogFactoryTest`, the offline
  `PaimonTableSerdeRoundTripTest`, the 46-test suite referenced in CI 968880) must still pass
  unchanged. They exercise `buildHmsHiveConf`/`buildDlfHiveConf` (HiveConf assembly), flavor
  resolution, and the FE→BE serde round-trip — the last one transitively exercises the
  **doris-gen TSerializer (0.16.0) path** that RC-1 protects, so a green round-trip test is the
  unit-level guard that the shade change did not re-split `TBase`. No new UT is needed: the
  failure is a packaging/classloader fault that **cannot reproduce in a single-classloader UT**
  (the whole point of the RC-5/RC-1 lineage — these bugs only surface under the docker
  plugin-zip child-first loader).
- Run: `mvn -pl fe/fe-connector/fe-connector-paimon -am test` (the `-am` is required, per the
  repo's `${revision}` gotcha, to also build the new shade module).

## E2E Tests

**Static jar verification (proves the class is now reachable, before docker):**
1. `mvn -pl fe/fe-connector/fe-connector-paimon-hive-shade,fe/fe-connector/fe-connector-paimon
   -am package`
2. Assert the relocated class is present in the shade jar:
   `unzip -l .../fe-connector-paimon-hive-shade/target/*.jar | grep
   'org/apache/doris/paimon/shaded/thrift/transport/TFramedTransport.class'` → must be **1 hit**.
3. Assert the shaded `HiveMetaStoreClient` references the relocated name:
   `unzip -p .../shade.jar org/apache/hadoop/hive/metastore/HiveMetaStoreClient.class | strings |
   grep 'paimon/shaded/thrift/transport/TFramedTransport'` → must hit (and the original
   `org/apache/thrift/transport/TFramedTransport` must **not** appear).
4. Assert **no** original-package `org/apache/thrift/` class in the final plugin zip's `lib/`
   except none-at-all (libthrift still excluded):
   `unzip -l .../doris-fe-connector-paimon.zip | grep -E 'lib/.*(libthrift|paimon-hive-connector-3.1-)'`
   → **no** raw `paimon-hive-connector-3.1` jar, **no** libthrift jar; the shade jar present.
5. Assert paimon-core is still a single jar and `org.apache.paimon.hive.*` is provided only by
   the shade.

**Docker external suite (the real gate, `enablePaimonTest=true`):**
- `external_table_p0/paimon/test_paimon_table.groovy::test_create_paimon_table` (line 44) — the
  `metastore=hive` create that currently throws `NoClassDefFoundError`. Must create the catalog
  and pass.
- `external_table_p0/paimon/test_paimon_statistics.groovy` (line 33) — same HMS catalog +
  ANALYZE/statistics read.
- Regression-only sanity that the non-HMS flavors still work and RC-1 did not regress:
  `test_paimon_jdbc_catalog.groovy` (jdbc), and any filesystem/REST paimon read suite (exercises
  `PaimonScanPlanProvider` → the doris-gen 0.16.0 TSerializer path) must stay green.

This bug class is **docker-plugin-zip-only**; local UTs and a single-loader run cannot catch it,
so a green docker `enablePaimonTest=true` run on these two suites (plus an unbroken jdbc/scan
suite) is the acceptance gate.
