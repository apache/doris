# FIX-READ-SPLIT — 对抗 review 轮次记录

> 设计: `plan-doc/tasks/designs/P4-T06d-FIX-READ-SPLIT-design.md`。修复: `MaxComputeScanPlanProvider` byte_size 分支 `.length(splitByteSize)` → `.length(-1L)`(恢复 BE BYTE_SIZE/ROW_OFFSET sentinel)。

## Round 1 (2 clean-room reviewers + 修复期已折 critic 更正)

修复期已处理 parent 设计的 critic 更正:`getLength()` byte_size 实有 **3** 个消费者(非 2):setPath、setSize、`PluginDrivenSplit:42→FileSplit.length`(→`FederationBackendPolicy:499` 一致性哈希 + `FileQueryScanNode:430` totalFileSize)。已在 T06d 设计 + parent 设计登记。UT 取 **provider-level**(非 parent 设计的弱 range-level),mutation 自证。

**R-A — 正确性 / blast-radius / legacy parity: ✅ CLEAN**
- 改的是 byte_size 分支(:272);row_offset(`:290 .length(count)`)/ limit-opt(`:338 .length(rowsToRead)`)未动且仍发真实计数;连接器内仅此 3 个 split builder,无遗漏。
- 3 个 `getLength()=-1` 消费者全安全:① BE `split_size==-1`⇒BYTE_SIZE(`IndexedInputSplit` 只用 split index,忽略 size);② `FederationBackendPolicy:499` 哈希 -1 为常量分量,真正区分靠 `/byte_size` 路径 + 唯一 start,确定且与 legacy 逐字一致;③ `totalFileSize+=-1` 转负仅供 EXPLAIN/stats,且 `applyMaxFileSplitNumLimit:767` 有 `<=0` early-return 守卫(无负除);`getSplitWeight` 不用 length;`getLength()*selectedSplitNum`(:387)路径因 PluginDrivenScanNode 不 override isBatchMode 而不可达。
- legacy parity 精确恢复:`MaxComputeScanNode:658-659` byte_size = `MaxComputeSplit(BYTE_SIZE_PATH, splitIndex, -1, byteSize, ...)`(arg3 length=-1,真实字节进未读的 fileLength);新连接器 `populateRangeParams:120-122` 逐字复刻(path `"[ start , -1 ]"`/startOffset/size=-1)。
- scope:仅连接器 1 生产行 + 新 UT;BE/thrift/gensrc/legacy/fe-core 生产零改。

**R-B — 测试有效性 (Rule 9): ✅ CLEAN**
- UT 经反射调真实 private `buildSplitsFromSession`(含被改的 `.length(-1L)` 行),用离线 Serializable fakes 返真实 `IndexedInputSplit`,读回 `populateRangeParams` 产物 → 断 `getSize()==-1`/startOffset/path。非弱 range-level。
- mutation 独立复现:还原 `.length(splitByteSize)` → `byteSizeBranchEmitsMinusOneSizeSentinel` FAIL(`expected <-1> but was <268435456>`),仅 1/2 失败(row_offset 对照仍过 → 断言特异)。复原后生产 diff 干净。
- 反射 rename → `NoSuchMethodException` JUnit ERROR(fail loud,不会静默 vacuous);连接器无 fe-core/Mockito、`buildSplitsFromSession` 私有无公开 seam → 反射合理(minor)。
- 对照锁定:`rowOffsetBranchKeepsRealRowCount` 断 `getSize()==1000`,防"全置 -1"过广回归。
- gates: MVN_EXIT=0(Tests run: 5,4 跑 1 skip=OdpsLiveConnectivityTest)/CS_EXIT=0。

## 收敛结论

Round 1 两 reviewer 均 CLEAN,无 finding → **FIX-READ-SPLIT 收敛(1 轮),可 commit**。
跨轮无矛盾(单轮)。

**登记(非本 issue,供后续跟踪)**: PluginDrivenScanNode 未 override `isBatchMode()`(legacy MaxComputeScanNode 对分区表 return true)→ plugin 路径不走 batch/lazy split 生成。独立于 FIX-READ-SPLIT,属另一(性能向)差异,与 READ-P3 分区裁剪丢失同族,**本批外**。
