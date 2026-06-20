# FIX-1 — test_create_paimon_table: paimon-over-HMS `create database` classloader split

## Problem
CI 973411 `external_table_p0/paimon/test_paimon_table.groovy:44`: creating a paimon catalog with
`paimon.catalog.type=hms` then `create database if not exists test_db` fails:
`Failed to create Paimon catalog with HMS metastore (flavor=hms): Failed to create the desired metastore
client (HiveMetaStoreClient)`.

## Root Cause
`fe.log:423900` deepest cause: `class org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl not
org.apache.hadoop.hive.metastore.MetaStoreFilterHook` from `HiveMetaStoreClient.loadFilterHooks:252`.
`Configuration.getClass("metastore.filter.hook", DefaultMetaStoreFilterHookImpl.class, MetaStoreFilterHook.class)`
resolves the configured class NAME through the **`Configuration` object's own `classLoader` field**, NOT the
thread-context CL. `new HiveConf()` captures the TCCL active *at construction* into that field. In
`PaimonConnector.createCatalog` the HiveConf is built by `assembleHiveConf` BEFORE `createCatalogFromContext`
pins the TCCL to the plugin loader — and `getClass` ignores the TCCL anyway. Under child-first plugin loading,
`DefaultMetaStoreFilterHookImpl` (resolved by name via the parent app loader) ≠ the child-loaded
`MetaStoreFilterHook` interface → the cast check throws.

The filesystem/jdbc path is immune: `buildHadoopConfiguration` already calls
`conf.setClassLoader(PaimonCatalogFactory.class.getClassLoader())` (PaimonCatalogFactory.java:257).
`assembleHiveConf` (line 323-330) never does. Legacy master ran in a single app loader, so no split.
Classification: **SPI regression** (introduced by child-first plugin packaging). Also covers DLF (shares
`assembleHiveConf`).

## Design
Pin the HiveConf classloader to the paimon plugin loader in `assembleHiveConf`, exactly mirroring
`buildHadoopConfiguration:257`. This makes every by-name class lookup `HiveMetaStoreClient` performs resolve
through the same child loader that loaded `HiveMetaStoreClient`/`MetaStoreFilterHook`. Single chokepoint →
fixes both HMS and DLF. Entirely inside the paimon connector module (connector-agnostic rule respected).

## Implementation Plan
`PaimonCatalogFactory.assembleHiveConf`: add `hiveConf.setClassLoader(PaimonCatalogFactory.class.getClassLoader())`
immediately after `new HiveConf()`.

## Risk Analysis
Minimal. Identical idiom already in use one method up. Pinning to the plugin loader is strictly more correct
than the captured-TCCL default; cannot regress the FS path (separate builder). No behavior change for the
single-classpath UT environment.

## Test Plan
### Unit Tests
`PaimonCatalogFactoryTest.assembleHiveConfPinsPluginClassLoaderNotTccl`: set a *foreign* TCCL
(`new URLClassLoader(new URL[0], null)`) before calling `assembleHiveConf`, assert the returned HiveConf's
`getClassLoader()` is the plugin loader (`PaimonCatalogFactory.class.getClassLoader()`), not the foreign TCCL.
RED before fix (HiveConf captures the foreign TCCL), GREEN after. Encodes WHY: the conf must resolve by-name
classes through the plugin loader independent of whatever TCCL was active at construction.

### E2E Tests
Existing `test_paimon_table.groovy` / `test_paimon_catalog.groovy` under docker `enablePaimonTest=true` are the
real gate (a flat-classpath UT cannot reproduce the actual cross-loader cast). Currently RED; expected GREEN.
