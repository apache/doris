# Doris Lance Blob：简版设计与使用示例

本文介绍当前实现的设计思路和使用方式，供方案交流使用。内容依据 `branch-4.1-pk-blob` 工作区源码整理，基于 `a4a24f22f3c` 及未提交改动，更新于 2026-11-20。

## 1. 设计思路

**扫描时生成轻量 FILE 引用，执行 `fl_get_binary()` 时再通过 Lance SDK 读取二进制内容。**

图片、音频、视频等 Blob 可能很大。如果扫描时就读取全部内容，后续算子丢弃这些行时，会浪费网络、内存和复制开销。本方案将“查询 Blob 引用”和“读取 Blob 内容”分开，为减少无效读取提供基础。

SQL 提供两个入口：

```sql
-- 返回 FILE 描述符。
SELECT image FROM lance_table;

-- 显式读取内容，返回可空的 VARBINARY。
SELECT fl_get_binary(image) FROM lance_table;
```

当前不提供 `lance_blob_as_descriptor` 输出模式开关。即使 Blob 在 Lance 中采用 Inline 存储，扫描时也统一生成 FILE 引用，不自动填充 `FILE.inline`。

## 2. 查询流程

```text
FE: Blob v2 -> FILE, prepare source configuration
                         |
                         v
Lance Scan: ordinary columns + Blob descriptors + row IDs
                         |
                         v
FILE: public fields + binding ID + row ID
                         |
                         v
Execution plan: copy / filter / reorder / Exchange
                         |
             +-----------+-----------+
             |                       |
             v                       v
       Return FILE            fl_get_binary(FILE)
       (descriptor)                  |
                                     v
                           Group + deduplicate rows
                                     |
                                     v
                           Resolve Lance SDK handles
                                     |
                                     v
                           Check sizes -> read payloads
                                     |
                                     v
                              Return VARBINARY
```

各阶段的职责如下：

1. **FE 规划**：识别顶层 `lance.blob.v2` 列，将其映射为 FILE，并下发来源标识、存储配置和大小限制。
2. **扫描**：复用 Lance 已返回的 Arrow 描述符，生成逻辑 URI 和内部引用；不为构造 FILE 再逐行调用 `take_blobs()`、解析完整物理地址或读取 payload。
3. **引用传递**：FILE 经复制、过滤、重排或 Exchange 传输时，一并保留对应行的读取上下文。
4. **内容读取**：函数按来源、dataset、固定版本和列分组，在一次调用内对 row ID 去重，调用 `lance_dataset_take_blobs()` 批量取得 handle，再逐个同步读取内容。

“扫描不读取 payload”不等于没有存储 I/O：扫描仍需读取 manifest、普通列和 Blob 描述符等信息。

## 3. FILE 中保存什么

FILE 的公开字段仍为六个：

| 字段 | 当前 Lance Blob 输出含义 |
| --- | --- |
| `uri` | `lance-blob://...` 逻辑定位符，不是底层对象的物理下载地址 |
| `offset` | NULL，不暴露底层文件偏移 |
| `size` | 已知时为内容长度；未知时为 NULL |
| `content_type` | 当前不自动推断，保持 NULL |
| `checksum` | 当前不自动计算，保持 NULL |
| `inline` | 扫描输出保持 NULL；读取内容使用 `fl_get_binary()` |

例如，下面是一个已知长度 FILE 的展示示意：

```json
{
  "uri": "lance-blob://...",
  "offset": null,
  "size": 4096,
  "content_type": null,
  "checksum": null,
  "inline": null
}
```

其中，external Blob 描述符的 `size=0` 可能表示需要查询对象长度，因此扫描输出为 NULL；managed Blob 的有效空值则保留 `size=0`。真正读取时，SDK 解析实际长度，Doris 在分配和读取 payload 前校验长度一致性及 VARBINARY 长度表示范围。

### 3.1 FileReadBinding：描述“从哪里读”

公开的六个字段用于表达 FILE 值；`FileReadBinding` 是附加的内部读取上下文，用来回答：**这个 Blob 属于哪个查询、哪个来源、哪个 dataset 快照、哪一列，以及是否只能在特定 BE 上读取。**

当前字段如下：

| 字段 | 类型 | 作用 |
| --- | --- | --- |
| `query_id_hi`、`query_id_lo` | 两个 UInt64 | 标识所属查询，消费时与当前查询校验，阻止跨查询复用内部引用 |
| `source_node_id` | Int32 | 来源 ScanNode ID，用于在当前 BE 的查询配置表中查找对应来源的存储配置 |
| `dataset_uri` | String | 实际用于 SDK 打开 dataset 的 URI，独立于公开的 `lance-blob://...` 展示定位符 |
| `dataset_version` | UInt64 | 固定的正数版本，保证回读使用扫描时的 Lance 快照，而不是重新读取 latest |
| `column_name` | String | Blob 列名，供 `take_blobs()` 等接口定位列 |
| `local_backend_id` | UInt64 | 0 表示没有来源 BE 限制；非 0 时，消费 BE 必须与该 ID 一致 |

`row_id` 不放在 binding 中，而是逐行单独保存。二者的组合确定要读取的值：

```text
FileReadBinding + native row ID
      |
      +-- query / source / dataset / version / column / BE constraint
                         |
                         v
                 One Blob in one snapshot
```

这里的 row ID 是 Lance 返回的原生 `_rowid`，不是 Doris Block 内的行号，也不是 `0..N-1` 的逻辑行偏移。过滤或排序只改变它在 Block 中的位置，不改变其指向的 Blob。

binding 不额外保存 endpoint、access key 等 storage options，也不保存 SDK dataset 或 Blob handle。storage options 通过来源配置表恢复；SDK handle 在消费 BE 上创建。实际 dataset URI 可能包含原始 URI 参数，因此不能把内部 binding 当成对外导出格式。

预期长度也不放入 binding：已知时使用该行 FILE 的公开 `size` 字段，未知时等待 SDK 解析。相同 binding 下的各行可以有不同长度。

### 3.2 ColumnFile：共享 binding，逐行保存引用

同一个 dataset、版本、列产生的大量行通常具有相同 binding，因此 `ColumnFile` 使用字典共享这部分信息：

```text
Binding dictionary                       Per-row reference arrays
------------------                       ------------------------
0 -> unbound                             row   binding_id   row_id
1 -> dataset A, version 42, image         0         1          101
2 -> dataset B, version 18, image         1         1          103
                                         2         2            9
                                         3         1          101
```

上例中，第 0、1、3 行共享 binding 1，第 2 行来自另一个 dataset。第 0、3 行指向同一个 Blob，在同一次 `fl_get_binary()` 调用中可以去重读取。

- 每行增加一个 UInt32 binding ID 和一个 UInt64 row ID，即 12 字节的引用数组数据；这个数字不包含六个公开字段、字典、容器容量和其他对象开销。
- binding ID 只在所属字典中有效，不是全局来源 ID，也不是内存地址。ID 0 表示没有绑定读取上下文，不能单凭 row ID 判断能否回读。
- binding 通过 `shared_ptr<const FileReadBinding>` 共享。列克隆可共享字典；追加新的 binding 时按需复制字典，避免改动其他列的映射。
- 过滤、重排和复制同步处理公开字段、binding ID、row ID，保证逐行对齐。跨列复制时，如果两列字典不同，会合并等价 binding 并重映射 ID。
- 单值 `Field::File` 保存共享 binding 指针和 row ID，在回到列表示时重新登记到目标字典。

因此，不要求整个 FILE 列只有一个来源。来自不同来源的 FILE 合并后，每行仍保留自己的读取身份。

## 4. Scan 与函数位于不同 BE 时

### 4.1 两条独立的上下文路径

假设 BE-A 执行 Scan，BE-B 执行 `fl_get_binary()`：

```text
                         FE
              source_node_id -> scan parameters
                       /    \
                      v      v
                    BE-A    BE-B
                      |      |
                    Scan     |  QueryContext stores source configuration
                      |      |
                      +------>  Exchange: FILE fields + dictionary + ID arrays
                             |
                             v
                       Restore bindings
                             |
                             v
                       fl_get_binary()
                             |
                             v
                  Source configuration + binding + row ID
                             |
                             v
                  Open pinned dataset -> read Blob
```

| 路径 | 传递内容 |
| --- | --- |
| FE → 参与查询的 BE | `ScanNode ID -> TFileScanRangeParams` 来源配置表，包含访问存储所需的配置，以及 `blob_source_id` 等参数 |
| Scan BE → 消费 BE，经 Exchange | FILE 六个公开字段、编码后的 binding 字典、每行 binding ID 和 row ID |

FE 汇集查询的文件扫描参数，并向参与 BE 下发。BE 将配置表安装到 `QueryContext::file_scan_range_params_map`；只执行内容消费的 BE 也需要这份表。访问配置不需要随每行 FILE 重复发送，binding 中的 `source_node_id` 就是连接这两条路径的键。

### 4.2 FILE 如何序列化与恢复

Block 序列化使用以下布局示意：

```text
FILE column in a serialized Block
  |
  +-- six public child columns
  +-- encoded binding dictionary
  +-- UInt32 binding IDs, one per row
  +-- UInt64 native row IDs, one per row
```

每个 binding 通过 `FileReadBinding::encode()` 编码一次，包含 `LFB1` 标识及其字段值。接收端调用 `decode()` 恢复字段并校验格式，再重建列字典、binding ID 数组和 row ID 数组。`LFB1` 是内部 binding 编码标识，不是此前已移除的全局 BE 执行版本分界。

传输不复制源 BE 的 C++ 指针或 SDK handle。接收端产生新的本地对象，保留的是相同的来源身份和快照定位信息。

其他内部路径也保留相同语义：

| 路径 | 上下文保存方式 |
| --- | --- |
| Block / Exchange | 列内 binding 字典加两组逐行 ID 数组 |
| 内部 PValues | 有读取引用时，附加 binding 字典、binding ID 和 row ID 三部分 |
| 独立 arena 行编码 | 逐值保存 binding 编码和 row ID，恢复时重新登记并去重；不依赖外部字典 |

### 4.3 消费 BE 如何恢复读取能力

BE-B 执行 `fl_get_binary()` 时，针对需要外部读取的行执行以下步骤：

1. 取得该行的 binding、row ID 和可选预期长度，检查 binding 所属 query ID 与当前查询一致。
2. 检查 `local_backend_id`。非 0 且与当前 BE 不一致时直接报错。
3. 按完整 binding 分组并对 row ID 去重；对每组，通过 `source_node_id` 查找本地来源配置表，并检查其中的 `blob_source_id` 与引用一致。来源缺失时不猜测凭证或使用其他来源替代。
4. 使用来源配置中的 storage options、binding 的 `dataset_uri` 和 `dataset_version` 打开固定版本 dataset。
5. 使用该组 row ID 和 binding 中的列名取得 SDK Blob handle；校验长度一致性及 VARBINARY 长度表示范围，再读取内容并回填到原输入位置。

每个 BE 的 `QueryContext` 持有自己的 `FileContentReader`。它按 `(source_node_id, dataset_uri, dataset_version)` 缓存 dataset，因此同一来源和快照的不同 Blob 列可以复用打开结果。缓存生命周期独立于原 Scan，随消费 BE 的查询上下文清理。

共享远程存储可使用这条跨 BE 回读路径。本地 dataset、本地 external 对象以及含注册本地 base 的 dataset 当前要求在来源 BE 消费，不提供代理回读。

### 4.4 公开 URI 与内部上下文的边界

`lance-blob://...` 用于表达逻辑定位信息，`fl_get_binary()` 实际使用的是内部 binding 和来源配置，并不解析这个展示 URI 来恢复凭证或决定来源。query ID 校验也只是查询归属校验，不能代替存储访问配置。

公开 JSON 只包含六个 FILE 字段，不导出 binding。把 FILE 转成 JSON 后重新导入，即使 URI 相同，也不会恢复内部读取上下文。没有 inline 内容且没有有效 binding 的 FILE，当前执行 `fl_get_binary()` 会报错。

## 5. 使用示例

以下假设已配置可访问的 Lance Catalog，表 `lance_catalog.demo.assets` 包含 `id`、`category` 和 Blob v2 列 `image`。请替换为实际表名和字段名；本文 SQL 未在本轮执行。

### 5.1 启用读取路径

```sql
SET enable_file_scanner_v2 = true;

-- 查看 image 映射为 FILE。
DESC lance_catalog.demo.assets;
```

### 5.2 查询引用和元数据

```sql
-- 返回 FILE 描述符，不请求 Blob 内容。
SELECT id, image
FROM lance_catalog.demo.assets
WHERE category = 'cat'
LIMIT 10;

-- 只查看逻辑 URI 和已知长度。
SELECT id,
       element_at(image, 'uri') AS blob_ref,
       element_at(image, 'size') AS blob_size
FROM lance_catalog.demo.assets
WHERE category = 'cat'
LIMIT 10;
```

`blob_size` 为 NULL 不能单独用来判断 Blob 是否为 SQL NULL；对于非 NULL FILE，它表示扫描阶段没有确定长度。`element_at(image, 'inline')` 也不会触发内容读取。

### 5.3 显式读取内容

```sql
SELECT id, fl_get_binary(image) AS image_bytes
FROM lance_catalog.demo.assets
WHERE id IN (1, 2, 3);
```

返回的 `image_bytes` 是 VARBINARY，内容展示方式取决于 SQL 客户端。SQL NULL Blob 返回 NULL；有效空 Blob 返回非 NULL 的空二进制。

当前不设置 Lance Blob 专属的单值读取限额。内容读取仍受 VARBINARY 的长度表示范围和查询内存限制约束。

## 6. 当前范围与后续工作

- 当前支持顶层 Blob v2 列；嵌套在 ARRAY、MAP、STRUCT 中的 Blob 尚不支持。
- 当前保证扫描生成引用、函数执行时读取内容。具体读取哪些行取决于函数在物理执行计划中的位置；不能仅凭 SQL 中有 LIMIT、ORDER BY 或子查询，就保证只读取最终保留行。
- CASE/IF 只读取命中分支、进一步将函数后移到 TopN/LIMIT 之后，以及异步读取仍是后续工作。
- 当前使用 `take_blobs()` 取得 handle 后同步读取，尚未接入新版 SDK 的 planned read 流式接口。
- 固定 dataset 版本保持 Lance 快照一致性；对被外部覆盖的对象，当前长度校验无法检测同长度内容替换。
- FILE 上下文采用统一内部编码，不单独提高全局 BE 执行版本；通信节点需理解相同的 FILE 编码。

这是一份当前源码的方案说明，不代表已完成 SDK、FE/BE 的完整构建和跨 BE 联调。此前已做相关语法及补丁检查，新增单测和本文 SQL 仍需运行验证。当前固定版本的 lance-c 还需应用通用 base-path 元数据补丁并重建，以保留本地存储的放置约束。

## 7. 主要实现位置

| 模块 | 入口 |
| --- | --- |
| 类型映射与来源参数 | `LanceTypeConverter.java`、`FileQueryScanNode.java` |
| 扫描生成逻辑引用 | `lance_reader.cpp`、`lance_blob_column_reader.cpp` |
| FILE 上下文与传输 | `file_read_reference.h`、`column_file.cpp`、`data_type_file.cpp` |
| 内容读取入口与缓存 | `function_fl_get_binary.cpp`、`file_content_reader.cpp` |

详细方案见 [Lance Blob 延迟读取设计讨论稿](lance-blob-lazy-read-design.md)，实现说明见 [Lance Blob v1](lance-blob-v1.md)。
