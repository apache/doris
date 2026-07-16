# FIX-text-write — LZ4FRAME→LZ4BLOCK read remap lost in plugin scan path

**Suite fixed:** `test_hive_text_write_insert` (the `lz4` compression iteration).
**Scope:** connector opt-in hook on the scan SPI; fe-core stays connector-agnostic.
**Status:** code DONE (`e1d48045bee`). e2e awaits user self-run.

---

## 1. Symptom

A hive TEXT/OpenCSV table written with LZ4 compression fails to READ under the plugin scan path — BE aborts
with `LZ4F_getFrameInfo ERROR_frameType_unknown`. The write path is fine; only the read is broken.
`test_hive_text_write_insert.groovy` loops `format_compressions = [..., "lz4"]`, writing a text table then
reading it back against a codec-invariant golden; the `lz4` iteration diverges/fails.

## 2. Root cause (HEAD-verified)

BE does **not** infer compression from the path — it honors the `compress_type` FE sets on each scan range.
Legacy `HiveScanNode.getFileCompressType` (`HiveScanNode.java:670-678`) overrode the base inference to remap
`LZ4FRAME → LZ4BLOCK`, because hadoop/hive write `.lz4` files with the LZ4 **block** codec, not the LZ4 **frame**
format the `.lz4` extension implies.

The SPI cutover replaced `HiveScanNode` with the connector-agnostic `PluginDrivenScanNode`, which does **not**
override `getFileCompressType` and inherits `FileQueryScanNode.getFileCompressType` (`:624`) →
`Util.inferFileCompressTypeByPath` → `.lz4` becomes `LZ4FRAME`. FE ships `LZ4FRAME` on the scan range
(`FileQueryScanNode.java:477-478`); BE's frame decoder then fails on block-format bytes. FE-side fix only.

## 3. Fix — connector opt-in hook (mirrors `supportsTableSample` / `supportsBatchScan` / `classifyColumn`)

Iron rule: fe-core generic node must not branch on source/serde. The remap is a connector opt-in via a default
hook; only hive (and hudi, see §4) override it.

**A. SPI default (identity)** — `ConnectorScanPlanProvider`:
```java
default TFileCompressType adjustFileCompressType(TFileCompressType inferred) { return inferred; }
```

**B. Generic node delegates** — `PluginDrivenScanNode` (byte-identical shape to `classifyColumnByConnector`):
```java
@Override
protected TFileCompressType getFileCompressType(FileSplit fileSplit) throws UserException {
    TFileCompressType inferred = super.getFileCompressType(fileSplit);   // base extension inference
    ConnectorScanPlanProvider scanProvider = resolveScanProvider();
    if (scanProvider == null) {
        return inferred;
    }
    return onPluginClassLoader(scanProvider, () -> scanProvider.adjustFileCompressType(inferred));
}
```
No source/serde branch; `onPluginClassLoader` keeps the TCCL pin convention (streaming split paths run on a
pool thread that doesn't inherit the caller's TCCL). Verified: the plugin's `setScanParams` override never
re-touches `compress_type`, so the value set here survives to BE (red-team confirmed).

**C. Hive override** — `HiveScanPlanProvider`:
```java
@Override
public TFileCompressType adjustFileCompressType(TFileCompressType inferred) {
    return inferred == TFileCompressType.LZ4FRAME ? TFileCompressType.LZ4BLOCK : inferred;
}
```
Exact legacy parity; the hadoop-specific fact stays inside the connector. Only `LZ4FRAME` flips — every other
codec (and a non-hive real frame file, which hive never produces) passes through.

## 4. Hudi strict-parity (red-team call)

Legacy `HudiScanNode extends HiveScanNode` (`HudiScanNode.java:94`) and **inherited** the remap. The new
`HudiScanPlanProvider` is independent, so under this design it would get only the identity default. Both
red-team lenses (correctness, iron-rule) flagged this: leaving it off silently drops a behavior legacy shipped
(Rule 3 / Rule 12), resting on the unverifiable-in-code assumption "hudi never produces a `.lz4` split." Adopted
the **strict-parity option**: `HudiScanPlanProvider` re-declares the identical override. Cost is a few lines of
possibly-unreachable code; benefit is provable parity. The other 6 providers (iceberg/paimon/trino/es/mc/jdbc)
keep the identity default — verified legacy had the remap only on `HiveScanNode`.

## 5. Tests — RED/GREEN

- `ConnectorScanPlanProviderCompressTypeTest` (fe-connector-api): the default is identity for **every**
  `TFileCompressType` — the zero-break guard for non-opting connectors, and it proves the default returns
  `LZ4FRAME` unchanged.
- `HiveScanBatchModeTest` +1: hive remaps only `LZ4FRAME→LZ4BLOCK`, passes `GZ/ZSTD/SNAPPYBLOCK/PLAIN` through.
- `HudiBackendDescriptorTest` +1: hudi does the same (inherited-parity guard).
- **RED/GREEN encoded by the pair**: the api test pins the default = identity (returns `LZ4FRAME`); the hive
  test pins hive = `LZ4BLOCK`. The delta is exactly the override — remove it and hive returns `LZ4FRAME` ≠
  expected `LZ4BLOCK`.
- **Regression**: fe-core `PluginDrivenScanNodeBatchModeTest` (12), `PluginDrivenScanNodeTableSampleTest` (6),
  `PluginDrivenExternalTableTest` (36) all green with the new override; 0 checkstyle.

## 6. Acceptance (e2e — user self-run)

`external_table_p0/hive/write/test_hive_text_write_insert` (hive3): the `lz4` iteration must read back
byte-identical rows to the other codecs against the codec-invariant golden. This is the true RED→GREEN gate.
