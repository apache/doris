# FIX-1 Summary — paimon-over-HMS create-db classloader split

## Problem
CI 973411 `test_create_paimon_table:44`: `create database` on a `paimon.catalog.type=hms` catalog failed with
`Failed to create the desired metastore client (HiveMetaStoreClient)`.

## Root Cause
`HiveMetaStoreClient.loadFilterHooks` → `Configuration.getClass("metastore.filter.hook", ...)` resolves the
class by name through the `HiveConf` object's own `classLoader` field. `new HiveConf()` in `assembleHiveConf`
captured the TCCL active at construction (= parent app loader, since it runs before the plugin TCCL pin), so
under child-first plugin loading `DefaultMetaStoreFilterHookImpl` (parent) ≠ child-loaded `MetaStoreFilterHook`
→ "class … not …". The filesystem builder already pinned the conf loader (line 257); `assembleHiveConf` did not.

## Fix
`PaimonCatalogFactory.assembleHiveConf`: `hiveConf.setClassLoader(PaimonCatalogFactory.class.getClassLoader())`
right after `new HiveConf()`. Single chokepoint → covers both HMS and DLF. Connector-local.

## Tests
`PaimonCatalogFactoryTest.assembleHiveConfPinsPluginClassLoaderNotTccl`: installs a foreign TCCL, asserts the
returned HiveConf is pinned to the plugin loader. RED before / GREEN after. Full class: 16/16 pass; checkstyle clean.

## Result
Fixed (offline UT). Real gate: docker `enablePaimonTest=true` rerun of test_paimon_table / test_paimon_catalog.
