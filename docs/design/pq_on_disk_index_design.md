# PQ_ON_DISK 索引设计文档（用户过滤后小候选集向量 TopN）

## 1. 问题背景

### 1.1 业务查询模式

```sql
SELECT photo_id
FROM tbl
WHERE user_id = ?
ORDER BY l2_distance_approximate(embedding, [query_vec])
LIMIT N;
```

### 1.2 数据特征

- 总用户规模约 100W。
- 每个 `user_id` 下向量上限约 1W。
- 表按 `user_id` 排序键存储，同一 `user_id` 的数据物理上连续。
- 向量列 `embedding` 类型为 `ARRAY<FLOAT>`，`NOT NULL`。
- 典型查询：先按 `user_id` 强过滤（倒排索引），再做向量 TopN。

### 1.3 现有痛点

1. **准入逻辑误判**：`segment_iterator.cpp:852-854` 中 `has_column_predicate` 检查会在 `WHERE user_id = ?` 存在时强制 fallback 到暴力搜索，即使 `user_id` 谓词已被倒排索引完全消费。
2. **30% 阈值误伤**：`segment_iterator.cpp:900-912` 中 `_row_bitmap.cardinality() < rows_of_segment * 0.3` 的检查会在小候选集场景下触发 fallback——而 PQ_ON_DISK 恰恰是为小候选集设计的。
3. **IVF 不适配**：现有 `ivf_on_disk` 依赖 coarse quantizer + inverted list 的全局 ANN 召回模型，对于已知精确 rowid 候选集的场景不是最佳匹配。

### 1.4 目标

- 引入 `PQ_ON_DISK` 索引类型（非 IVF，无 coarse quantizer）。
- 在倒排过滤后的精确 rowid 候选集上执行 PQ ADC TopN / Range Search。
- 性能优先：保证排序质量，不追求近似距离数值与精确函数完全一致。

### 1.5 非目标

- 不改 SQL 类型（对外仍是 `ARRAY<FLOAT>`）。
- 不改 scan 执行大时序。
- 不修改 FAISS 核心代码（复用其 `ProductQuantizer` 能力）。

---

## 2. 总体方案

### 2.1 设计原则

- 复用 Doris 现有 ANN 框架（索引元数据、加载、查询入口、profile/metrics）。
- 复用 FAISS `ProductQuantizer` 做训练与编码，不依赖 FAISS `IndexPQ` 内存结构。
- 新增独立 `PqOnDiskVectorIndex` 类实现 `VectorIndex` 接口，**不继承** `FaissVectorIndex`。
- 新增自定义序列化格式（`ann.pqmeta` + `ann.pqdata`），不使用 `faiss::write_index()`。
- PQ codes 按 rowid 顺序连续存储，利用表按 `user_id` 排序键的物理布局。

### 2.2 查询链路总览

```
┌────────────────────────────────────────────────────────────────┐
│                     SQL Query                                  │
│  WHERE user_id = ? ORDER BY l2_distance_approximate(...) LIMIT N│
└──────────────────────┬─────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  1. 倒排索引执行 user_id 谓词 → 收敛 _row_bitmap            │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  2. _apply_ann_topn_predicate()                              │
│     识别 index_type = PQ_ON_DISK                             │
│     ✓ 跳过 has_column_predicate 检查                         │
│     ✓ 跳过 30% 阈值检查                                     │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  3. AnnIndexReader::query() → PqOnDiskVectorIndex            │
│     3a. 加载 codebook (ann.pqmeta, 常驻内存)                 │
│     3b. 构建 query LUT (M × ksub 距离表)                     │
│     3c. 遍历 _row_bitmap → 批量读取 PQ codes (ann.pqdata)    │
│     3d. ADC 累加 → TopN 堆                                   │
│     3e. 输出 rowid + score                                   │
└──────────────────────┬───────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────┐
│  4. 设置 index-only scan, materialized result column         │
└──────────────────────────────────────────────────────────────┘
```

**关键点**：该路径本质是「过滤后小候选集上的 PQ 暴力近似重排」，不依赖 IVF list 探测，不做全局 ANN 召回。

---

## 3. FAISS 能力复用

本方案不修改 FAISS 核心，仅使用以下 `ProductQuantizer` API：

| API | 用途 |
|-----|------|
| `ProductQuantizer(d, M, nbits)` | 构造 PQ 对象 |
| `pq.train(n, x)` | 训练 codebook |
| `pq.compute_codes(x, codes, n)` | 向量编码为 PQ codes |
| `pq.compute_distance_table(x, dis_table)` | 构建 L2 LUT（M × ksub floats）|
| `pq.compute_inner_prod_table(x, dis_table)` | 构建 IP LUT（M × ksub floats）|

**不使用**：FAISS `IndexPQ` 的内存常驻搜索路径、IVF/HNSW 图或倒排列表结构、`faiss::write_index()` / `faiss::read_index()`。

**收益**：降低维护成本，避免后续 FAISS 升级冲突；保持实现可控，针对 Doris 小候选场景做定制优化。

### 3.1 L2 与 Inner Product 分开处理

虽然框架可复用，但必须分 metric 实现：

| 维度 | L2 | IP |
|------|----|----|
| LUT 构建 | `compute_distance_table()` | `compute_inner_prod_table()` |
| 排序方向 | ASC（越小越好）| DESC（越大越好）|
| TopN 堆 | max-heap（淘汰最大值）| min-heap（淘汰最小值）|
| FAISS 距离语义 | 返回 L2²，需 `sqrt` 转换 | 直接可用 |

---

## 4. PqOnDiskVectorIndex 类设计

### 4.1 类继承关系

```
VectorIndex (abstract)
├── FaissVectorIndex        (HNSW / IVF / IVF_ON_DISK)
└── PqOnDiskVectorIndex     (PQ_ON_DISK) ← 新增
```

### 4.2 头文件设计

```cpp
// be/src/storage/index/ann/pq_on_disk_vector_index.h

#pragma once

#include <faiss/impl/ProductQuantizer.h>
#include <roaring/roaring.hh>

#include "common/status.h"
#include "storage/index/ann/ann_index.h"

namespace doris::segment_v2 {

struct PqOnDiskBuildParameter {
    int dim = 0;
    int pq_m = 0;
    int pq_nbits = 8;
    AnnIndexMetric metric = AnnIndexMetric::L2;
};

class PqOnDiskVectorIndex : public VectorIndex {
public:
    PqOnDiskVectorIndex();
    ~PqOnDiskVectorIndex() override;

    /// 配置构建参数，初始化 FAISS ProductQuantizer
    void build(const PqOnDiskBuildParameter& params);

    // ---- VectorIndex 接口实现 ----

    Status train(Int64 n, const float* x) override;
    Status add(Int64 n, const float* x) override;
    Int64 get_min_train_rows() const override;
    Status ann_topn_search(const float* query_vec, int k,
                           const IndexSearchParameters& params,
                           IndexSearchResult& result) override;
    Status range_search(const float* query_vec, const float& radius,
                        const IndexSearchParameters& params,
                        IndexSearchResult& result) override;
    Status save(lucene::store::Directory*) override;
    Status load(lucene::store::Directory*) override;

    /// 设置 pqdata 缓存 key 前缀（load 前调用）
    void set_pqdata_cache_key_prefix(std::string prefix) {
        _pqdata_cache_key_prefix = std::move(prefix);
    }

private:
    // ---- ADC 搜索核心 ----

    /// L2 ADC TopN：遍历 bitmap 中的 rowid，查 LUT 累加距离，维护 max-heap
    Status _adc_topn_l2(const float* dis_table, int k,
                        const roaring::Roaring& bitmap,
                        IndexSearchResult& result);

    /// IP ADC TopN：遍历 bitmap 中的 rowid，查 LUT 累加内积，维护 min-heap
    Status _adc_topn_ip(const float* dis_table, int k,
                        const roaring::Roaring& bitmap,
                        IndexSearchResult& result);

    /// L2 Range Search：遍历 bitmap，ADC 累加后与 radius 比较
    Status _adc_range_l2(const float* dis_table, float radius,
                         const roaring::Roaring& bitmap,
                         IndexSearchResult& result);

    /// IP Range Search：遍历 bitmap，ADC 累加后与 radius 比较
    Status _adc_range_ip(const float* dis_table, float radius,
                         const roaring::Roaring& bitmap,
                         IndexSearchResult& result);

    /// 从 ann.pqdata 读取指定 rowid 范围的 PQ codes
    /// 利用 roaring bitmap 的连续区间做批量顺序读取
    Status _read_codes_for_bitmap(const roaring::Roaring& bitmap,
                                  std::vector<uint8_t>& codes,
                                  std::vector<uint32_t>& rowid_map);

    // ---- 成员变量 ----

    PqOnDiskBuildParameter _params;
    std::unique_ptr<faiss::ProductQuantizer> _pq;

    // 写入阶段：内存中累积的 PQ codes（flush 后写入 ann.pqdata）
    std::vector<uint8_t> _codes_buffer;
    int64_t _ntotal = 0;  // 已编码的向量总数

    // 读取阶段：ann.pqdata 的 CLucene IndexInput（通过 CachedRandomAccessReader 包装）
    std::string _pqdata_cache_key_prefix;
    lucene::store::IndexInput* _pqdata_input = nullptr;  // owned, clone from Directory
    size_t _pqdata_file_size = 0;
};

} // namespace doris::segment_v2
```

### 4.3 关键设计决策

| 决策 | 理由 |
|------|------|
| 不继承 `FaissVectorIndex` | `FaissVectorIndex` 强绑定 `faiss::Index*`，save/load 依赖 `faiss::write_index()`；PQ_ON_DISK 使用裸 `ProductQuantizer` + 自定义格式 |
| codes 写入阶段在内存中累积 | 写入是 segment flush 的一部分，整个 segment 的 codes 在 finish() 时一次性落盘 |
| 读取阶段不全量加载 codes | codes 按 rowid 顺序存储，查询时只读取 bitmap 覆盖的子集 |
| `_pqdata_input` 使用 CLucene IndexInput | 与 IVF_ON_DISK 一致的 I/O 层抽象，可复用 `CachedRandomAccessReader` |

---

## 5. 索引文件格式

### 5.1 文件概览

PQ_ON_DISK 索引存储在 CLucene compound `.idx` 文件内，包含两个逻辑文件：

| 文件名 | 内容 | 大小估算 |
|--------|------|----------|
| `ann.pqmeta` | 头部 + codebook | ~(32 + dim × ksub × 4) bytes |
| `ann.pqdata` | PQ codes，按 rowid 顺序 | ntotal × code_size bytes |

### 5.2 ann.pqmeta 格式

```
┌──────────────────────────────────────────────────────────────┐
│                    ann.pqmeta 文件布局                        │
├──────────────────────────────────────────────────────────────┤
│  Offset   │ Size    │ Field           │ Description          │
├───────────┼─────────┼─────────────────┼──────────────────────┤
│  0        │ 4       │ magic           │ 0x50514F44 ("PQOD") │
│  4        │ 4       │ version         │ 1 (uint32)           │
│  8        │ 4       │ dim             │ 向量维度 (uint32)     │
│  12       │ 4       │ pq_m            │ 子量化器数量 (uint32) │
│  16       │ 4       │ pq_nbits        │ 每个子量化器比特数    │
│  20       │ 4       │ metric          │ 0=L2, 1=IP (uint32) │
│  24       │ 8       │ ntotal          │ 向量总数 (uint64)    │
│  32       │ 4       │ codebook_size   │ codebook 字节数       │
│  36       │ var     │ codebook        │ M×ksub×dsub floats   │
│  36+var   │ 4       │ checksum        │ CRC32 of above       │
└──────────────────────────────────────────────────────────────┘
```

**codebook 布局**：与 FAISS `ProductQuantizer::centroids` 一致，`M × ksub × dsub` 个 float，共 `M × ksub × dsub × 4` 字节。其中 `dsub = dim / M`，`ksub = 2^pq_nbits`（8-bit 时为 256）。

**codebook_size 计算**：`M × ksub × dsub × sizeof(float)` = `M × 256 × (dim/M) × 4` = `dim × 256 × 4` = `dim × 1024` 字节。

### 5.3 ann.pqdata 格式

```
┌──────────────────────────────────────────────────────────────┐
│                    ann.pqdata 文件布局                        │
├──────────────────────────────────────────────────────────────┤
│  PQ code for rowid 0    │ code_size bytes                    │
│  PQ code for rowid 1    │ code_size bytes                    │
│  PQ code for rowid 2    │ code_size bytes                    │
│  ...                     │                                    │
│  PQ code for rowid N-1  │ code_size bytes                    │
└──────────────────────────────────────────────────────────────┘

code_size = pq_m × ceil(pq_nbits / 8)
          = pq_m (当 pq_nbits = 8 时)

总大小 = ntotal × code_size
```

**按 rowid 顺序存储的优势**：表按 `user_id` 排序键存储，同一 `user_id` 的行 rowid 连续。`_row_bitmap` 中同一用户的 rowid 也是连续区间。这意味着从 `ann.pqdata` 读取这些 codes 是**顺序 I/O**，而非随机 I/O。

### 5.4 文件名常量

```cpp
// be/src/storage/index/ann/ann_index_files.h 中新增：
inline constexpr char pq_meta_file_name[] = "ann.pqmeta";
inline constexpr char pq_data_file_name[] = "ann.pqdata";
```

---

## 6. 写入路径

### 6.1 写入流程图

```
                    AnnIndexColumnWriter
                           │
        ┌──────────────────┴───────────────────┐
        │ init(): 解析 properties               │
        │   index_type == "pq_on_disk" ?        │
        │   ├─ Yes → 创建 PqOnDiskVectorIndex  │
        │   └─ No  → 创建 FaissVectorIndex     │
        └──────────────────┬───────────────────┘
                           │
        ┌──────────────────┴───────────────────┐
        │ add_array_values():                   │
        │   累积 float 数据到 _float_array      │
        │   每满 chunk_size 行:                 │
        │     ├─ train(chunk_size, data)        │
        │     └─ add(chunk_size, data)          │
        └──────────────────┬───────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │ PqOnDiskVectorIndex::train()             │
        │   首次调用: _pq->train(n, x)            │
        │   后续调用: no-op (codebook 已训练)      │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │ PqOnDiskVectorIndex::add()               │
        │   _pq->compute_codes(x, codes, n)        │
        │   追加到 _codes_buffer                    │
        │   _ntotal += n                            │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │ finish() → save(dir)                      │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │ PqOnDiskVectorIndex::save()              │
        │   1. 创建 ann.pqmeta:                    │
        │      写入 header (magic/ver/dim/M/...)   │
        │      写入 _pq->centroids                 │
        │      写入 CRC32 checksum                 │
        │   2. 创建 ann.pqdata:                    │
        │      写入 _codes_buffer                  │
        └──────────────────────────────────────────┘
```

### 6.2 训练策略

- **最小训练行数**：`(2^pq_nbits) × 100`。对于 8-bit PQ，最小 25,600 行。
- **首次 train 调用**执行实际训练；后续 chunk 的 train 调用为 no-op（codebook 在首次训练后固定）。
- **小 segment 降级**：若 segment 行数不满足最小训练阈值且没有之前训练过的 codebook，跳过索引构建，调用 `_index_file_writer->delete_index(_index_meta)` 清理目录条目。

### 6.3 save() 伪代码

```cpp
Status PqOnDiskVectorIndex::save(lucene::store::Directory* dir) {
    // 1. 写 ann.pqmeta
    auto* meta_output = dir->createOutput(pq_meta_file_name);
    // header
    write_uint32(meta_output, 0x50514F44);       // magic "PQOD"
    write_uint32(meta_output, 1);                 // version
    write_uint32(meta_output, _params.dim);
    write_uint32(meta_output, _params.pq_m);
    write_uint32(meta_output, _params.pq_nbits);
    write_uint32(meta_output, static_cast<uint32_t>(_metric));  // 0=L2, 1=IP
    write_uint64(meta_output, _ntotal);
    // codebook
    size_t codebook_bytes = _pq->centroids.size() * sizeof(float);
    write_uint32(meta_output, codebook_bytes);
    meta_output->writeBytes(reinterpret_cast<const uint8_t*>(_pq->centroids.data()),
                            codebook_bytes);
    // checksum
    uint32_t crc = compute_crc32(/* all bytes above */);
    write_uint32(meta_output, crc);
    meta_output->close();
    delete meta_output;

    // 2. 写 ann.pqdata
    auto* data_output = dir->createOutput(pq_data_file_name);
    data_output->writeBytes(_codes_buffer.data(), _codes_buffer.size());
    data_output->close();
    delete data_output;

    return Status::OK();
}
```

---

## 7. 读取路径

### 7.1 加载流程图

```
           AnnIndexReader::load_index()
                       │
                       ▼
        ┌──────────────────────────────────────────┐
        │ 识别 _index_type == PQ_ON_DISK           │
        │                                          │
        │ 创建 PqOnDiskVectorIndex                 │
        │ 设置 pqdata_cache_key_prefix             │
        │ 调用 load(compound_dir)                  │
        └──────────────────┬───────────────────────┘
                           │
                           ▼
        ┌──────────────────────────────────────────┐
        │ PqOnDiskVectorIndex::load()              │
        │                                          │
        │ 1. 打开 ann.pqmeta:                     │
        │    读取 header, 校验 magic/version       │
        │    读取 codebook → _pq->centroids        │
        │    调用 _pq->set_derived_values()        │
        │    校验 CRC32 checksum                   │
        │                                          │
        │ 2. 打开 ann.pqdata:                     │
        │    _pqdata_input = dir->openInput(...)   │
        │    _pqdata_file_size = input->length()   │
        │    (不全量加载，按需读取)                 │
        └──────────────────────────────────────────┘
```

**codebook 常驻内存**：codebook 大小 = `dim × ksub × sizeof(float)` = `dim × 1024` 字节。对于 dim=768 的场景，codebook 仅 768 KB，可以安全常驻内存。

**ann.pqdata 不全量加载**：文件通过 CLucene IndexInput 打开后保持 handle，查询时按需读取。

### 7.2 查询流程图（TopN）

```
     AnnIndexReader::query()
              │
              ▼
     PqOnDiskVectorIndex::ann_topn_search(query_vec, k, params, result)
              │
     ┌────────┴────────────────────────────────────────────────────┐
     │  Step 1: 构建 LUT                                          │
     │    dis_table[M × ksub]                                     │
     │    if L2: _pq->compute_distance_table(query_vec, dis_table)│
     │    if IP: _pq->compute_inner_prod_table(query_vec, dis_table)│
     └────────┬────────────────────────────────────────────────────┘
              │
     ┌────────┴────────────────────────────────────────────────────┐
     │  Step 2: 从 ann.pqdata 读取候选 rowid 的 PQ codes          │
     │    遍历 _row_bitmap 的连续区间:                             │
     │      for each run [start, start+len):                       │
     │        offset = start × code_size                           │
     │        nbytes = len × code_size                             │
     │        seek(offset); readBytes(buf, nbytes)                 │
     │    合并为连续 codes 数组 + rowid_map                        │
     └────────┬────────────────────────────────────────────────────┘
              │
     ┌────────┴────────────────────────────────────────────────────┐
     │  Step 3: ADC 累加 + TopN 堆                                │
     │    初始化 heap (size k)                                     │
     │    for i in 0..num_candidates:                              │
     │      float dist = 0;                                        │
     │      const uint8_t* code = &codes[i × code_size];           │
     │      for m in 0..M:                                         │
     │        dist += dis_table[m × ksub + code[m]];               │
     │      heap_push_if_better(dist, rowid_map[i])                │
     │    heap_finalize → 输出 distances + row_ids + roaring       │
     └────────┬────────────────────────────────────────────────────┘
              │
              ▼
          返回 IndexSearchResult
```

### 7.3 I/O 策略：Roaring 区间批量读取

`roaring::Roaring` 内部使用 Run-Length Encoding (RLE) 存储连续区间。我们利用这一特性避免逐行随机 I/O：

```cpp
Status PqOnDiskVectorIndex::_read_codes_for_bitmap(
        const roaring::Roaring& bitmap,
        std::vector<uint8_t>& codes,
        std::vector<uint32_t>& rowid_map) {

    const size_t code_size = _pq->code_size;
    const size_t n_candidates = bitmap.cardinality();
    codes.resize(n_candidates * code_size);
    rowid_map.resize(n_candidates);

    size_t out_idx = 0;

    // 使用 roaring 迭代器遍历连续区间
    roaring::Roaring::const_iterator it(bitmap);
    while (it != bitmap.end()) {
        // 收集当前连续区间
        uint32_t run_start = *it;
        uint32_t run_end = run_start;
        ++it;
        while (it != bitmap.end() && *it == run_end + 1) {
            run_end = *it;
            ++it;
        }
        uint32_t run_len = run_end - run_start + 1;

        // 单次顺序读取整个区间
        size_t offset = static_cast<size_t>(run_start) * code_size;
        size_t nbytes = static_cast<size_t>(run_len) * code_size;
        _pqdata_input->seek(offset);
        _pqdata_input->readBytes(&codes[out_idx * code_size], nbytes);

        // 填充 rowid 映射
        for (uint32_t r = run_start; r <= run_end; ++r) {
            rowid_map[out_idx++] = r;
        }
    }

    DCHECK(out_idx == n_candidates);
    return Status::OK();
}
```

**性能分析**：
- 同一 `user_id` 的行在 segment 内 rowid 连续 → 通常只需 1~2 次顺序读取。
- 每次读取大小 = `run_len × code_size`。对于 dim=768, M=96 的 8-bit PQ，`code_size = 96 bytes`，1W 个向量 = 960 KB，单次读取即可完成。

### 7.4 ADC 搜索核心

```cpp
// L2 ADC TopN 核心循环
Status PqOnDiskVectorIndex::_adc_topn_l2(
        const float* dis_table, int k,
        const roaring::Roaring& bitmap,
        IndexSearchResult& result) {

    std::vector<uint8_t> codes;
    std::vector<uint32_t> rowid_map;
    RETURN_IF_ERROR(_read_codes_for_bitmap(bitmap, codes, rowid_map));

    const size_t n = rowid_map.size();
    const size_t code_size = _pq->code_size;
    const size_t M = _pq->M;
    const size_t ksub = _pq->ksub;

    // max-heap: 保持最小的 k 个距离
    // heap_top() 是当前第 k 小距离（阈值）
    std::vector<float> heap_dis(k, std::numeric_limits<float>::max());
    std::vector<uint32_t> heap_ids(k, 0);

    for (size_t i = 0; i < n; ++i) {
        const uint8_t* code = &codes[i * code_size];
        float dist = 0;
        for (size_t m = 0; m < M; ++m) {
            dist += dis_table[m * ksub + code[m]];
        }
        // L2: 距离越小越好 → max-heap 淘汰最大值
        if (dist < heap_dis[0]) {
            // 替换堆顶并下沉
            faiss::maxheap_replace_top(k, heap_dis.data(), heap_ids.data(), dist, rowid_map[i]);
        }
    }

    // 堆排序，输出按距离从小到大
    faiss::maxheap_reorder(k, heap_dis.data(), heap_ids.data());

    // 构建结果（跳过无效的 sentinel 条目）
    result.roaring = std::make_shared<roaring::Roaring>();
    size_t valid_count = 0;
    for (int i = 0; i < k; ++i) {
        if (heap_dis[i] < std::numeric_limits<float>::max()) {
            valid_count++;
        }
    }
    result.distances = std::make_unique<float[]>(valid_count);
    result.row_ids = std::make_unique<std::vector<uint64_t>>(valid_count);
    for (size_t i = 0; i < valid_count; ++i) {
        result.distances[i] = std::sqrt(heap_dis[i]);  // L2²→L2
        (*result.row_ids)[i] = heap_ids[i];
        result.roaring->add(heap_ids[i]);
    }

    return Status::OK();
}
```

---

## 8. Range Search 实现

### 8.1 设计

Range Search 与 TopN 共享 LUT 构建和 codes 读取逻辑，区别在于匹配条件：

- **L2 Range**：`dist < radius²`（FAISS 语义用平方距离）
- **IP Range**：`inner_product >= radius`（FAISS 语义）

### 8.2 流程图

```
     PqOnDiskVectorIndex::range_search(query_vec, radius, params, result)
              │
     ┌────────┴────────────────────────────────────────────────────┐
     │  Step 1: 构建 LUT（同 TopN）                               │
     └────────┬────────────────────────────────────────────────────┘
              │
     ┌────────┴────────────────────────────────────────────────────┐
     │  Step 2: 读取候选 PQ codes（同 TopN）                      │
     └────────┬────────────────────────────────────────────────────┘
              │
     ┌────────┴────────────────────────────────────────────────────┐
     │  Step 3: ADC 累加 + 阈值判断                               │
     │    result_roaring = new Roaring()                           │
     │    for i in 0..num_candidates:                              │
     │      float dist = adc_compute(dis_table, code_i)            │
     │      if is_le_or_lt:                                        │
     │        L2:  dist < radius² → add to result                  │
     │        IP:  不适用（反转语义，同 FaissVectorIndex）          │
     │      else:                                                  │
     │        L2:  不适用（反转语义）                               │
     │        IP:  dist >= radius → add to result                  │
     └────────┬────────────────────────────────────────────────────┘
              │
              ▼
     返回 IndexSearchResult (roaring + optional distances + row_ids)
```

### 8.3 与 FaissVectorIndex range_search 的一致性

结果格式与现有 `FaissVectorIndex::range_search()` 完全一致：

| 条件 | Metric | 返回 |
|------|--------|------|
| `is_le_or_lt=true` | L2 | roaring + distances + row_ids |
| `is_le_or_lt=true` | IP | roaring = origin - matched（差集）, distances=nullptr |
| `is_le_or_lt=false` | L2 | roaring = origin - matched（差集）, distances=nullptr |
| `is_le_or_lt=false` | IP | roaring + distances + row_ids |

---

## 9. SegmentIterator 准入逻辑修改

### 9.1 当前问题

`segment_iterator.cpp` 中 `_apply_ann_topn_predicate()` 的准入检查：

```cpp
// Line 852-854: 任何列有谓词就 fallback
bool has_column_predicate = std::any_of(_is_pred_column.begin(), _is_pred_column.end(),
                                        [](bool is_pred) { return is_pred; });
if (!has_ann_index || has_common_expr_push_down || has_column_predicate) {
    // fallback to brute force
}

// Line 900-912: 候选集 < 30% segment → fallback
if (static_cast<double>(pre_size) < static_cast<double>(rows_of_segment) * 0.3) {
    // fallback to brute force
}
```

**问题 1**：`has_column_predicate` 检查的是 `_is_pred_column` 数组中是否存在任何 `true`。`user_id = ?` 会将 `user_id` 列标记为 `true`。即使 `user_id` 谓词已被倒排索引完全消费（`_row_bitmap` 已收敛），ANN 路径仍然被阻断。

**问题 2**：30% 阈值假设 ANN 索引（如 HNSW）在小候选集上收益不高。但 PQ_ON_DISK 的设计目标恰恰就是小候选集上的高效重排。

### 9.2 修改方案

```cpp
Status SegmentIterator::_apply_ann_topn_predicate() {
    // ... existing code ...

    bool has_column_predicate = std::any_of(_is_pred_column.begin(), _is_pred_column.end(),
                                            [](bool is_pred) { return is_pred; });

    // ---- 新增：PQ_ON_DISK 跳过 has_column_predicate 和 30% 阈值检查 ----
    bool is_pq_on_disk = (ann_index_reader->get_index_type() == AnnIndexType::PQ_ON_DISK);

    if (!has_ann_index || has_common_expr_push_down ||
        (has_column_predicate && !is_pq_on_disk)) {
        // fallback to brute force
        // ...
    }

    // ... metric/direction checks (unchanged) ...

    // ---- 修改：PQ_ON_DISK 跳过 30% 阈值 ----
    size_t pre_size = _row_bitmap.cardinality();
    size_t rows_of_segment = _segment->num_rows();
    if (!is_pq_on_disk &&
        static_cast<double>(pre_size) < static_cast<double>(rows_of_segment) * 0.3) {
        // fallback to brute force
        // ...
    }

    // ... rest of the function (unchanged) ...
}
```

### 9.3 准入逻辑流程图（修改后）

```
     _apply_ann_topn_predicate()
              │
              ▼
     has_ann_index ?
     ├─ No → fallback (brute force)
     └─ Yes ↓
              │
     has_common_expr_push_down ?
     ├─ Yes → fallback
     └─ No ↓
              │
     has_column_predicate ?
     ├─ Yes ──→ is_pq_on_disk ?
     │          ├─ Yes → CONTINUE (允许有谓词)
     │          └─ No  → fallback
     └─ No ↓
              │
     metric/direction 检查
     ├─ mismatch → fallback
     └─ OK ↓
              │
     is_pq_on_disk ?
     ├─ Yes → SKIP 30% 检查，直接进入搜索
     └─ No ↓
              │
     _row_bitmap.cardinality() < 30% segment ?
     ├─ Yes → fallback
     └─ No ↓
              │
     加载索引 → 执行搜索 → 输出结果
```

### 9.4 AnnIndexReader 修改

`AnnIndexReader` 需要新增对 `PQ_ON_DISK` 的分发：

```cpp
// ann_index_reader.cpp::load_index()
if (_index_type == AnnIndexType::PQ_ON_DISK) {
    auto pq_index = std::make_unique<PqOnDiskVectorIndex>();
    pq_index->set_metric(_metric_type);
    pq_index->set_type(_index_type);
    pq_index->set_pqdata_cache_key_prefix(
            _index_file_reader->get_index_file_cache_key(&_index_meta));
    RETURN_IF_ERROR(pq_index->load(compound_dir->get()));
    _vector_index = std::move(pq_index);
} else {
    // existing FaissVectorIndex path
}

// ann_index_reader.cpp::query()
if (_index_type == AnnIndexType::PQ_ON_DISK) {
    // PQ_ON_DISK 不需要特殊的 SearchParameters
    // 直接传递基础 IndexSearchParameters
    IndexSearchParameters pq_search_params;
    pq_search_params.roaring = param->roaring;
    pq_search_params.rows_of_segment = param->rows_of_segment;
    pq_search_params.io_ctx = io_ctx;
    RETURN_IF_ERROR(_vector_index->ann_topn_search(query_vec, limit, pq_search_params,
                                                    index_search_result));
    stats->engine_search_ns.update(index_search_result.engine_search_ns);
    stats->engine_convert_ns.update(index_search_result.engine_convert_ns);
    stats->engine_prepare_ns.update(index_search_result.engine_prepare_ns);
}
```

---

## 10. SIMD 优化路线

### 10.1 Phase 1：标量 ADC（初始实现）

```cpp
// 标量版本 — 简单、正确、可测试
float dist = 0;
for (size_t m = 0; m < M; ++m) {
    dist += dis_table[m * ksub + code[m]];
}
```

性能基线：对于 M=96, ksub=256, 1W 候选向量，ADC 时间约 1~2ms（现代 CPU 单核）。

### 10.2 Phase 2：FAISS AVX2 Gather（后续优化）

FAISS 内部的 ADC 实现（`code_distance-avx2.h`）使用 `_mm256_i32gather_ps` 进行 8 路并行查表：

```cpp
// FAISS 风格的 AVX2 ADC（参考实现思路）
// 每次处理 8 个子量化器
__m256 sum = _mm256_setzero_ps();
for (size_t m = 0; m < M; m += 8) {
    // 加载 8 个 code byte，扩展为 32-bit index
    __m256i indices = _mm256_cvtepu8_epi32(_mm_loadl_epi64((__m128i*)(code + m)));
    // 对每个子量化器的 LUT 进行 gather
    // base_addr = &dis_table[m * ksub]
    // gather: 从 base_addr[indices[i]] 加载 8 个 float
    __m256 partial = _mm256_i32gather_ps(dis_table + m * ksub, indices, 4);
    sum = _mm256_add_ps(sum, partial);
}
// 水平求和
float dist = horizontal_sum(sum);
```

**预期加速**：标量版本每个候选做 M 次 table lookup + M 次 add。AVX2 版本减少到 M/8 次 gather + M/8 次 add，理论加速约 4~6x（受 gather 延迟限制）。

### 10.3 Phase 3：批量 ADC + 预取（远期）

- 同时处理 4~8 个候选向量的 ADC，利用指令级并行隐藏 gather 延迟。
- 对下一批 codes 进行 `_mm_prefetch`，隐藏 L2/L3 cache miss。

---

## 11. 内存估算

### 11.1 常驻内存（索引加载后）

| 组件 | 大小公式 | dim=768, M=96, nbits=8 示例 |
|------|----------|---------------------------|
| codebook | `dim × ksub × sizeof(float)` | 768 × 256 × 4 = **768 KB** |
| ProductQuantizer 对象 | ~几百字节 | ~0.5 KB |
| CLucene IndexInput handle | ~几百字节 | ~0.5 KB |
| **合计** | | **~769 KB / segment** |

**对比**：现有 HNSW 索引常驻内存 = `ntotal × (dim × 4 + graph_edges)`，对于 1M 向量 dim=768 约 3~4 GB。PQ_ON_DISK 常驻内存减少 **4000x+**。

### 11.2 查询时临时内存

| 组件 | 大小公式 | dim=768, M=96, nbits=8, k=100, candidates=10K 示例 |
|------|----------|-----------------------------------------------------|
| LUT (dis_table) | `M × ksub × sizeof(float)` | 96 × 256 × 4 = **96 KB** |
| codes buffer | `candidates × code_size` | 10000 × 96 = **960 KB** |
| rowid_map | `candidates × sizeof(uint32_t)` | 10000 × 4 = **40 KB** |
| TopN heap | `k × (sizeof(float) + sizeof(uint32_t))` | 100 × 8 = **0.8 KB** |
| **合计** | | **~1.1 MB / query** |

### 11.3 写入时内存

| 组件 | 大小公式 | dim=768, M=96, chunk=100K 示例 |
|------|----------|-------------------------------|
| _float_array (chunk buffer) | `chunk_size × dim × sizeof(float)` | 100K × 768 × 4 = **300 MB** |
| _codes_buffer (全 segment) | `ntotal × code_size` | 1M × 96 = **96 MB** |
| ProductQuantizer (训练时) | ~codebook + 训练临时 | ~5 MB |
| **峰值合计** | | **~401 MB** |

注：`_float_array` 是 `AnnIndexColumnWriter` 的 chunk buffer，与 `FaissVectorIndex` 共用相同的 chunking 机制，不额外增加内存。

### 11.4 磁盘空间

| 组件 | 大小公式 | dim=768, M=96, 1M rows 示例 |
|------|----------|---------------------------|
| ann.pqmeta | ~`36 + dim × 1024 + 4` | ~769 KB |
| ann.pqdata | `ntotal × code_size` | 1M × 96 = **96 MB** |
| 原始向量 | `ntotal × dim × 4` | 1M × 768 × 4 = **3072 MB** |
| **PQ 存储比** | `code_size / (dim × 4)` | 96 / 3072 = **3.1%** |

---

## 12. 关键改动点汇总

### 12.1 FE

- **文件**：`fe/fe-core/src/main/java/org/apache/doris/analysis/AnnIndexPropertiesChecker.java`
- **改动**：支持 `index_type = pq_on_disk`
- **约束**：
  - `quantizer` 固定为 `pq`（或可省略，由 index_type 隐含）
  - 必须提供 `dim`, `pq_m`, `pq_nbits`
  - `dim % pq_m == 0`

### 12.2 BE 索引类型与路由

- **文件**：`be/src/storage/index/ann/ann_index.h`
  - `AnnIndexType` 枚举新增 `PQ_ON_DISK`
- **文件**：`be/src/storage/index/ann/ann_index.cpp`
  - 字符串映射新增 `"pq_on_disk"`
- **文件**：`be/src/storage/index/ann/ann_index_files.h`
  - 新增 `pq_meta_file_name` 和 `pq_data_file_name` 常量

### 12.3 BE 新增文件

- `be/src/storage/index/ann/pq_on_disk_vector_index.h` — 类定义
- `be/src/storage/index/ann/pq_on_disk_vector_index.cpp` — 实现

### 12.4 BE 修改文件

- **`ann_index_writer.cpp`**：`init()` 中根据 `index_type == "pq_on_disk"` 创建 `PqOnDiskVectorIndex` 替代 `FaissVectorIndex`
- **`ann_index_reader.cpp`**：`load_index()` 和 `query()` / `range_search()` 中新增 PQ_ON_DISK 分支
- **`segment_iterator.cpp`**：`_apply_ann_topn_predicate()` 中 PQ_ON_DISK 跳过 `has_column_predicate` 和 30% 阈值检查

### 12.5 BE 搜索参数

- **文件**：`be/src/storage/index/ann/ann_search_params.h`
  - `AnnIndexStats` 可新增 PQ_ON_DISK 特有的 metrics（LUT 构建耗时、codes 读取耗时、ADC 耗时）

---

## 13. 实施清单（按 PR 拆分）

### PR1：类型与属性接入

- [ ] BE 新增 `AnnIndexType::PQ_ON_DISK` 及字符串映射
- [ ] BE 新增 `pq_meta_file_name` / `pq_data_file_name` 常量
- [ ] FE 属性校验支持 `index_type=pq_on_disk`
- [ ] 回归测试：建表建索引 DDL 校验通过/失败用例

### PR2：PQ_ON_DISK 写入链路

- [ ] 实现 `PqOnDiskVectorIndex` 类（build/train/add/save）
- [ ] `ann_index_writer.cpp` 支持创建 `PqOnDiskVectorIndex`
- [ ] 训练 + 编码 + 落盘 `ann.pqmeta` + `ann.pqdata`
- [ ] 小 segment 不满足训练条件时降级策略
- [ ] UT：编码结果可加载且行数一致

### PR3：PQ_ON_DISK 查询链路（L2/IP TopN）

- [ ] 实现 `load()` 读取 codebook + 打开 pqdata
- [ ] 实现 L2 LUT + ADC + TopN
- [ ] 实现 IP LUT + ADC + TopN
- [ ] `ann_index_reader.cpp` 支持 PQ_ON_DISK 分发
- [ ] 与 `_row_bitmap` 联动，仅扫描候选 rowid
- [ ] UT：L2/IP 排序正确性

### PR4：Range Search

- [ ] 实现 L2/IP range search（阈值判断模式）
- [ ] 结果格式与 `FaissVectorIndex::range_search()` 一致
- [ ] UT：range search 正确性

### PR5：执行准入修正

- [ ] 对 `pq_on_disk` 跳过 `has_column_predicate` fallback
- [ ] 对 `pq_on_disk` 跳过 30% 阈值 fallback
- [ ] 回归测试：`user_id` 倒排过滤后能够命中 `pq_on_disk` 路径

### PR6：观测与压测

- [ ] 增加 metrics/profile：candidate rows、LUT time、ADC time、I/O time
- [ ] 基准测试：1K/5K/10K 候选集，N=10/50/100
- [ ] 输出性能报告：P50/P95/P99、QPS、内存占用、Recall@N

---

## 14. 测试与验收标准

### 功能验收

- [ ] `WHERE user_id = ? ORDER BY l2_distance_approximate(...) LIMIT N` 命中 `pq_on_disk`
- [ ] `inner_product_approximate` 路径可用且排序方向正确
- [ ] Range search（L2/IP × le_or_lt/ge_or_gt）四种组合正确

### 性能验收（建议目标）

- [ ] 在 1W 候选集下，P95 延迟优于 float 暴力路径
- [ ] 索引常驻内存显著低于全量原始向量加载（~769 KB vs 数 GB）
- [ ] 查询临时内存不超过 2 MB

### 质量验收

- [ ] Recall@N 达到业务可接受阈值（由业务定义）
- [ ] 结果稳定，无崩溃、越界、空结果异常
- [ ] Compaction 后索引自动重建，结果正确

---

## 15. 风险与应对

| 风险 | 应对 |
|------|------|
| PQ 近似误差导致召回下降 | 调 `pq_m/pq_nbits`，必要时增加候选过采样 |
| rowid 稀疏导致随机 I/O | 按 roaring 连续区间批读；表按 user_id 排序保证物理连续 |
| 小 segment 训练质量差 | 设置最小训练样本阈值（25,600），不达标则降级跳过 |
| codebook 不同 segment 间不一致 | 每个 segment 独立训练（与现有 IVF 行为一致），compaction 自动重建 |
| 准入逻辑修改影响其他索引类型 | `is_pq_on_disk` 条件精确限制范围，不影响 HNSW/IVF |

---

## 16. 当前决策结论

1. 采用 `PQ_ON_DISK`（非 IVF）方案。
2. 新增 `PqOnDiskVectorIndex` 类实现 `VectorIndex` 接口，不继承 `FaissVectorIndex`。
3. 自定义序列化格式（`ann.pqmeta` + `ann.pqdata`），不使用 `faiss::write_index()`。
4. 不修改 FAISS 核心，复用 `ProductQuantizer` 的训练、编码、LUT 计算能力。
5. 准入逻辑对 PQ_ON_DISK 跳过 `has_column_predicate` 和 30% 阈值检查。
6. 优先支持 `l2_distance_approximate` / `inner_product_approximate`。
7. SIMD 优化分阶段：先标量 ADC 保证正确性，后 AVX2 gather 优化性能。
8. 性能优先，先达成小候选集重排收益，再迭代质量与工程细节。
