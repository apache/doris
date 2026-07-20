# 问题类总结：热路径上的重操作放大（由 DORIS-27138 问题一泛化）

日期：2026-07-17
来源案例：JIRA DORIS-27138 问题一 / PR apache/doris#64134（commit `2366edffcc6`）

## 1. 具体案例回放（用于定义问题类，非本文重点）

PR #64134 为修复"迁移到 Iceberg 的表推断不出文件格式"，在
`IcebergUtils.getFileFormat(table)` 里加了第三级兜底
`inferFileFormatFromDataFiles()` → `table.newScan().planFiles()`
（= 读整表 manifest-list + 所有 manifest 的远程 IO）。

而这个方法的既有调用链是：

```
FileQueryScanNode.createScanRangeLocations()
  └─ for (Split split : splits)                        ← per-split 循环
       └─ splitToScanRange()
            └─ rangeDesc.setFormatType(getFileFormatType())   ← 每个 split 调一次
                 └─ IcebergScanNode.getFileFormatType()
                      └─ source.getFileFormat()
                           └─ IcebergUtils.getFileFormat(table)
                                └─ [兜底] planFiles() ← 整表元数据扫描
```

结果：N 个 split ⇒ N 次整表 planFiles()。对迁移表（无 format 属性），规划耗时
从 O(planFiles) 放大到 O(N × planFiles)，split 越多越灾难。

## 2. 问题类定义

**一个重操作，被放在高乘数调用位上，且没有任何一层做 hoist / memoize。**
三个要素缺一不可：

1. **重操作**——真实成本不小的操作：
   - 远程 IO：manifest / metadata / schema / 文件列表拉取，metastore RPC；
   - 大计算：整表/整分区遍历、大批量字符串处理（正则、JSON 序列化/反序列化）；
   - 大对象构建：整 schema 转换、大 thrift 结构序列化。
2. **高乘数调用位**——热路径上的循环体或高频入口：
   - per-split / per-file / per-partition / per-manifest 循环（乘数动辄 10³~10⁵）；
   - 每次查询规划都必经的路径（乘数 = QPS）。
3. **无 hoist / 无 memoize**——结果对循环不变（loop-invariant）或对
   同一次规划不变，却既没提到循环外，也没有任何缓存层兜住。

### 典型成因模式："伪装成轻访问器"

方法名像 O(1) 属性读（`getFileFormat`、`getSchema`、`getXxx`），调用方从签名
感知不到成本，于是随手放进循环。后来修 bug 的人往方法体里塞重兜底
（#64134 就是），**所有既有调用点被静默放大**——写兜底的人不知道有多少人
在循环里调它，循环作者也不知道 getter 变重了。本质是**接口签名不携带成本
契约**，两个各自合理的改动叠加成事故。

## 3. 常见变体

| 变体 | 描述 | 例子 |
|---|---|---|
| A. 循环内放大 | 重操作在 per-split/per-file 循环体内被逐次调用 | 本案例：per-split × planFiles() |
| B. 单链路重复 | 一次规划链路上同一信息被多次远端获取（无循环，但串行重复 k 次） | 一次查询里反复 loadTable / 反复拉 schema |
| C. 缓存旁路 | 缓存存在，但热路径没走它，或 key 设计导致必 miss | 新建 scan 对象绕过已有 table 级缓存 |
| D. 循环不变量未提出 | 每次迭代重复计算同一个值（解析 properties、拼字符串、构建相同的 thrift 子结构） | 循环内反复 `properties.get`+解析、反复序列化同一 schema |

A 是 B 的循环形态；C/D 是 A/B 的具体成因，单独列出便于按模式排查。

## 4. 审计方法（可复用清单）

1. **枚举热路径入口**：查询规划（split 枚举、scan range 序列化）、谓词下推、
   统计信息（row count / selectedPartitionNum）、MTMV 新鲜度、SHOW PARTITIONS、
   写入规划与提交。
2. **对每个循环体列出被调方法**，逐个**穿透 getter 追到底**，标注真实成本
   （内存读 / 本地计算 / 远程 IO），并判断返回值是否 loop-invariant。
3. **对每条链路数"同一信息获取次数"**：table 加载、schema 获取、snapshot 解析、
   properties 解析各发生几次？重复的是否有缓存兜住（并确认真的命中）？
4. **对每个缓存问 key**：热路径构造的 key 与缓存 key 是否一致？是否存在
   每次都 miss 的 key（如含时间戳/随机量/新建对象标识）。
5. 区分**固有成本**与**放大成本**：planFiles() 本身每次查询一次是规划的固有
   成本，不算问题；问题是同一信息被算了 N 次或在更高乘数的位置被算。
