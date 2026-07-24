# 待决项 2 —— 线路上「删除文件列表 + 分区 JSON」被逐分片重复塞进每个范围

> **状态**：⏳ 待定（**协议演进：动 BE/thrift 前必须用户签字**）
> **改动层**：FE 发送端（iceberg 连接器侧）+ **thrift 协议（新字段）** + **BE 读取端** + **跨版本兼容矩阵**。
> **触碰铁律 D**（改 BE + 改协议 = 协议演进，非回归修复）。
> **仅对 v2+ MOR 表生效**，普通表零影响。
> **行号信 HEAD（55087e08d0c 附近，PERF-11 后）**，以 `grep` 为准。

---

## 背景

Iceberg 的 v2/v3 表支持"读时合并"（MOR）：数据文件旁边挂着**删除文件**（position delete / equality delete / deletion vector），BE 读数据时用它们过滤掉被删的行。FE 规划时把每个数据文件的删除文件列表放进发给 BE 的扫描计划。

两个放大点叠加：
- 一个大数据文件会被切成 **k 个字节切片**（每个切片是一个扫描范围 `TFileRangeDesc`），BE 并行读；
- 一个 equality delete 文件常被**很多数据文件共享**（它是分区级/表级的删除）。

> 上一步的提交（PERF-11 的 `10b7d29423f`）只优化了 **FE 内存侧**——让同一文件的 k 个切片在 FE 堆里**共享**同一份删除列表对象；**线路（发给 BE 的字节）上的重复没动**，就是本项。

---

## 当前代码的调用栈与问题

```
createFileRangeDesc(...)                                   FileScanNode (每个切片一次)
└─ setScanParams(rangeDesc, split)                         PluginDrivenScanNode.java:1693  [每范围]
     └─ scanRange.populateRangeParams(fmtDesc, rangeDesc)  :1705 → IcebergScanRange.java:330
          fileDesc.setPartitionDataJson(partitionJson)     :397   ← 分区 JSON 塞进本范围
          if (v2+):                                         :408
            deleteDescs = new ArrayList(deleteFiles.size())         :413
            for (delete : deleteFiles):                            :414   ← 每范围 × 每删除文件
                 deleteDescs.add(delete.toThrift())                 :415   ← 每次新建一个 TIcebergDeleteFileDesc
            fileDesc.setDeleteFiles(deleteDescs)                    :417   ← 整份删除列表塞进"本切片"的范围
     rangeDesc.setTableFormatParams(fmtDesc)               :1707
```

thrift 的形状（`gensrc/thrift/PlanNodes.thrift`）：
```
TFileRangeDesc          (每范围一个)  →  table_format_params: TTableFormatFileDesc      :565 / :464
TTableFormatFileDesc                  →  iceberg_params: TIcebergFileDesc
TIcebergFileDesc.delete_files : list<TIcebergDeleteFileDesc>   :339   ← 删除列表挂在"每范围"里
TIcebergDeleteFileDesc { path; bounds; field_ids; content; DV偏移; original_path; ... }  :315-331
                                                              ← 每条约 100~200 字节(主要是路径字符串)
```

BE 侧逐范围 inline 消费（无字典）：
```
IcebergReaderMixin ... 初始化删除文件                       be/src/format/table/iceberg_reader_mixin.h:435
  table_desc = get_scan_range().table_format_params.iceberg_params   [每范围]
  for (desc : table_desc.delete_files):                              :444  ← 逐范围内联遍历
      分桶到 position / equality / deletion_vector                    :445-451
```

**问题**：一个数据文件的完整删除列表 + 分区 JSON 被复制进它**每一个字节切片**的 `TFileRangeDesc`；一个被 M 个数据文件共享的删除文件，在计划里出现 **M×k 次**。大 MOR 扫描的计划体积因此多出 MB 级——FE 序列化付一次、BE 反序列化再付一次。CPU 侧还有 `delete.toThrift()`（`:415`；实现在 `IcebergScanRange.java:681`）每范围重转一遍（"第二次转换"）。

---

## 解决方案的调用栈与问题

方向：**扫描节点级的删除文件字典 + 每范围索引**（去重）。

```
FE 发送端：
  createScanRangeLocations()                              PluginDrivenScanNode.java:1724
    └─ scanProvider.populateScanLevelParams(params, props)  :1732  ← 已存在的"扫描节点级"通用挂钩
         └─ (iceberg 新 override) 把整次扫描的去重删除文件表
            写进 params.iceberg_delete_dict : list<TIcebergDeleteFileDesc>   ← 每个删除文件只序列化 1 次
  setScanParams(rangeDesc, split)  →  populateRangeParams(...)  [每范围]
    └─ fileDesc.setDeleteFileIndices([3, 7, 12])          ← 每范围只带几个 int 索引，不带完整列表

BE 读取端：
  iceberg_reader_mixin.h
    dict = get_params().iceberg_delete_dict                ← 扫描级读一次
    for (i : table_desc.delete_file_indices):              ← 索引 → 字典解析
        分桶(dict[i])
```

关键先例（证明形状可行）：`TFileScanRangeParams` 里 paimon 的字段 27/30 就是"扫描节点级、避免每分片重复序列化"（`PlanNodes.thrift:549-556` 注释原话 *"Set at ScanNode level to avoid redundant serialization in each split"*），FE 侧正是通过 `ConnectorScanPlanProvider.populateScanLevelParams`（`:474`；`PaimonScanPlanProvider.java:1355` 已用同法）挂上去的。

**方案自身的问题（正是它需要用户签字的原因）**：

1. **这是协议演进，不是干净的回归修复**。要动**三层**：FE 发送端 + thrift schema（新字段）+ **BE 读取端**（`iceberg_reader_mixin.h` 改成"先按索引查字典再分桶"）。触碰"改 BE + 改协议"铁律 D → **必须用户先签字**。

2. **跨版本兼容矩阵是最难、也是承载性的一环**。集群里 FE/BE 可能版本不一：
   - **老 BE 配新 FE**：老 BE 不认新字典字段，只读 `delete_files`——新 FE 要么继续内联发一份（那就没省字节）、要么按"BE 能力位"判断对方支持才发字典；
   - **新 BE 配老 FE**：老 FE 只发 `delete_files`，新 BE 要能回退到内联读。
   - 这套"双发/能力位门控 + 双向回退"是真正的工作量，不能跳过。

3. **验证最重**：FE 单测（字典+索引与内联语义等价）+ BE reader 单测（索引解析 + 内联回退）+ 真实 MOR 表 e2e（position/equality/deletion vector 三类删除结果不变）+ **混版本兼容矩阵**（老BE↔新FE、新BE↔老FE 都要正确读到删除行）。

4. **一个澄清**：FE 发送端其实**不需要动 fe-core**（`populateScanLevelParams` 这个连接器无关挂钩已存在、paimon 已用同法），所以这块不碰"fe-core 加面"。难点全在 **BE + 兼容**。

5. **有个 FE-only「半赢」但基本没用**：因为上一步已让同文件的 k 个切片在 FE 堆里共享同一份删除列表，可顺手把 `toThrift()` 也缓存复用（k→1 次转换、省点 FE CPU/堆）。但 thrift 序列化每遇一次引用**仍会把整个结构完整写一遍**，所以**线路字节一个都不少**——只省 FE 的 CPU/堆，还要给不可变可序列化类加可变缓存，多半不值得单做。

---

## 示例

设一个 MOR 表：一个 equality delete 文件 `del-a.parquet`（路径 + 边界 + field_ids 序列化后约 200 字节），被 1000 个数据文件共享；每个数据文件平均切成 4 个字节切片。
- **当前**：这一个删除文件被序列化 1000 × 4 = **4000 次** ≈ 800 KB，全是重复的同一份；表里若有几十个这样的共享删除文件 → **几 MB 的重复**塞进发给每台 BE 的计划，FE 建 + BE 解析双向付费。
- **字典方案后**：`del-a.parquet` 在扫描级字典里**只序列化 1 次**（200 字节）；4000 个范围各带一个 4 字节索引 ≈ 16 KB。这一个删除文件从 800 KB 降到 ~16 KB。
- **代价对照**：换来的是要改 thrift + BE reader + 扛住"新旧 FE/BE 混跑都要正确读到删除行"的兼容矩阵——删错或读漏删除文件 = 查询结果错，所以兼容那一环必须做扎实。

---

## 立项前须确认

- [ ] 用户签字：**同意做协议演进**（改 thrift + 改 BE 读取端）。
- [ ] 定兼容策略：双发 vs BE 能力位门控；确认老BE↔新FE、新BE↔老FE 双向都能正确读删除。
- [ ] 定新字段布局：字典挂 `TFileScanRangeParams`（仿 paimon 27/30）+ 每范围索引挂 `TIcebergFileDesc`。
- [ ] 全套验证：FE 等价单测 + BE reader/回退单测 + 真 MOR e2e（三类删除）+ 混版本矩阵。

## 关联

审计原始证据 / 逐项调用链：`plan-doc/reviews/perf-audit-fe-connector-iceberg-2026-07-17*`（该簇 findings，自述"协议演进非回归修复"）；任务空间 `plan-doc/perf-hotpath-iceberg/`。
