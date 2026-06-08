# PQ_ON_DISK Chunk Cache 设计与实施计划

## 1. 背景与问题

### 1.1 现状

当前 `PQ_ON_DISK` 索引的查询路径中，PQ codes 的读取 **没有任何应用层缓存**：

- 每次查询都通过 `_read_codes_for_bitmap()` 从 CLucene `IndexInput` 读取 `ann.pqdata`。
- 唯一的"缓存"是 OS page cache，不受 Doris 控制。
- `_pqdata_cache_key_prefix` 成员已预埋但未使用。
- 所有并发查询通过 `_io_mutex` 串行化 seek+read，形成 I/O 瓶颈。

对比：`IVF_ON_DISK` 已有专用的 `AnnIndexIVFListCache`（LRU，按 list region 粒度缓存），cache hit 时完全跳过磁盘 I/O。

### 1.2 典型场景开销

以 `dim=768, pq_m=96, nbits=8` 为例：

| 组件 | 大小 | 说明 |
|------|------|------|
| Codebook | 768 KB | 已在 `load()` 时常驻内存 |
| 每次查询 I/O | `n_candidates × 96 bytes` | 1W 候选 = 960 KB |
| `_io_mutex` 竞争 | 每次查询必抢 | 高并发下串行化所有读取 |

**核心问题**：同一 `user_id` 的高频重复查询反复读取相同的磁盘数据，且 `_io_mutex` 导致并发查询无法并行。

---

## 2. 业界参考

### 2.1 Milvus

- **缓存粒度**：per-segment（IVF_PQ 整体 load/unload）。
- **DiskANN**：PQ codes 按预算（`PQCodeBugetGBRatio=12.5%`）常驻内存；图结构在 NVMe + Linux AIO。
- **MMap**：依赖 OS 页面管理，无自定义 block cache。
- **关键洞察**：Milvus 不做 PQ codes 的部分缓存——要么全量加载，要么全量 mmap。

### 2.2 Qdrant

- **缓存粒度**：per-segment，二元决策（`always_ram: true/false`）。
- **Codebook**：始终常驻内存（体积小）。
- **PQ codes**：`always_ram=true` 时全部 pin 在内存；`false` 时 mmap + OS page cache。
- **关键洞察**：无自定义 LRU cache；简单的全有/全无策略。

### 2.3 两者共同点

1. **Codebook 始终在内存**。
2. **PQ codes 的缓存单元不是 per-vector，而是较大的连续块**。
3. **不做 per-vector 级别的细粒度缓存**（管理开销过大）。

---

## 3. Doris 场景特殊性

Doris 的 PQ_ON_DISK 与 Milvus/Qdrant 存在本质区别：

| 维度 | Milvus/Qdrant | Doris PQ_ON_DISK |
|------|--------------|------------------|
| 使用场景 | 全局 ANN 召回 | **过滤后小候选集重排** |
| 候选集大小 | 全 segment (100K~1M) | 1K~10K (单个 user_id) |
| 访问模式 | 随机（图遍历）或全扫 | **按 user_id 连续区间** |
| 数据局部性 | 较差 | **极好**（sort key = user_id） |
| 并发特征 | 不同 query 访问不同 segment | **同 user_id 高频重复查询** |

**关键洞察**：Doris 的访问模式天然是 rowid-range 级别的连续读，同一 `user_id` 的 PQ codes 在 `ann.pqdata` 中是一段物理连续的字节区间。高频查询场景下同一 `user_id` 的 codes 会被反复读取。

---

## 4. Cache 粒度设计

### 4.1 方案对比

| 方案 | 粒度 | Cache Key | 优点 | 缺点 |
|------|------|-----------|------|------|
| A | Per-vector (rowid) | `(file, rowid)` | 最精确 | 条目数爆炸（千万级），管理开销大 |
| B | **Fixed-size chunk** | `(file, chunk_offset)` | 对齐 I/O，跨查询复用好 | chunk 边界需对齐 code_size |
| C | Rowid-range run | `(file, run_start, run_len)` | 完美匹配单次访问模式 | 不同 bitmap 产生不同 key，跨查询命中率低 |
| D | Segment 全量 | `(file)` | 最简单 | 大 segment (1M×96=96MB) 内存浪费 |

### 4.2 最终选择：Fixed-size chunk（方案 B）

**核心思路**：将 `_read_codes_for_bitmap()` 的 I/O 粒度从"精确 run 大小"改为"固定 chunk 边界对齐的读取"，使 I/O 单元 = cache 单元，两层自洽统一。

**chunk_size 计算**：

```
chunk_size = floor(TARGET_SIZE / code_size) * code_size
```

其中 `TARGET_SIZE = 64KB`。保证每个 chunk 包含 **整数个** vector 的 PQ codes，避免跨 chunk 边界拆分。

**典型 chunk_size**：

| pq_m | nbits | code_size | chunk_size | vectors/chunk |
|------|-------|-----------|------------|---------------|
| 96 | 8 | 96 bytes | 65,472 bytes | 682 |
| 48 | 8 | 48 bytes | 65,520 bytes | 1,365 |
| 32 | 8 | 32 bytes | 65,536 bytes | 2,048 |

**选择 Fixed chunk 的理由**：

1. **Cache key 稳定**：`chunk_idx` 是纯位置决定的，不依赖查询 bitmap。不同查询只要 rowid 落在同一 chunk 就能命中缓存。
2. **跨查询命中率高**：同 user_id 不同过滤条件（如 `AND tag='photo'`）的查询，只要 rowid 区间有 chunk 重叠就能复用。对比 run 方案需要精确匹配 `(start, len)`。
3. **与 IVF cache 完全同构**：可直接复用 `StoragePageCache::CacheKey` 格式 `(fname, fsize, offset)`，代码模式一致。
4. **多余读取开销很小**：chunk 内额外读取的其他行的 codes 也会被缓存，对后续查询有潜在收益。

---

## 5. 整体架构

### 5.1 数据流

```
┌─────────────────────────────────────────────────────────────────┐
│  PqOnDiskVectorIndex::_read_codes_for_bitmap()                  │
│                                                                 │
│  for each contiguous run in bitmap:                             │
│    for each chunk overlapping the run:                          │
│      ┌────────────────────┐                                     │
│      │ Cache lookup        │                                     │
│      │ key = (file_prefix, │                                     │
│      │  file_size,         │                                     │
│      │  chunk_byte_offset) │                                     │
│      └──────┬──────────────┘                                     │
│         hit │       miss                                         │
│      ┌──────┘       └────────────────────┐                       │
│      │ memcpy from              lock _io_mutex                   │
│      │ cached chunk             seek(chunk_offset)               │
│      │                          readBytes(chunk_size)            │
│      │                          cache insert                     │
│      └──────┬────────────────────────────┘                       │
│             │                                                    │
│      extract codes for overlapping rowids from chunk             │
│      append to output codes[] + rowid_map[]                     │
└─────────────────────────────────────────────────────────────────┘
                            │
                            ▼
              ADC TopN / Range Search（unchanged）
```

### 5.2 Cache Key 设计

复用 `StoragePageCache::CacheKey` 格式，与 `AnnIndexIVFListCache` 完全一致：

```cpp
StoragePageCache::CacheKey key(
    _pqdata_cache_key_prefix,   // segment 级别唯一标识
    _pqdata_file_size,          // 防止 stale 文件冲突
    chunk_byte_offset            // = chunk_idx * _chunk_size
);
```

### 5.3 Cache Value

直接复用 `DataPage`（即 `MemoryTrackedPageWithPageEntity`）：

- 容量 = `actual_chunk_bytes`（末尾 chunk 可能不足 `_chunk_size`）
- 自带 `Allocator<false>` + `MemTrackerLimiter` 追踪

---

## 6. 类设计

### 6.1 `AnnIndexPqChunkCache`

遵循 `AnnIndexIVFListCache` 的设计模式：

```cpp
// be/src/storage/cache/ann_index_pq_chunk_cache.h

class AnnIndexPqChunkCache {
public:
    static constexpr uint32_t kDefaultNumShards = 16;

    using CacheKey = StoragePageCache::CacheKey;

    class CacheImpl : public LRUCachePolicy {
    public:
        CacheImpl(size_t capacity, uint32_t num_shards)
            : LRUCachePolicy(
                  CachePolicy::CacheType::ANN_INDEX_PQ_CHUNK_CACHE,
                  capacity, LRUCacheType::SIZE,
                  config::ann_index_pq_chunk_cache_stale_sweep_time_sec,
                  num_shards, 0, true, false) {}
    };

    // Singleton
    static AnnIndexPqChunkCache* instance() { return _s_instance; }
    static AnnIndexPqChunkCache* create_global_cache(
            size_t capacity, uint32_t num_shards = kDefaultNumShards);
    static void destroy_global_cache();

    AnnIndexPqChunkCache(size_t capacity, uint32_t num_shards);
    ~AnnIndexPqChunkCache() = default;

    // Operations
    bool lookup(const CacheKey& key, PageCacheHandle* handle);
    void insert(const CacheKey& key, DataPage* page, PageCacheHandle* handle);

    std::shared_ptr<MemTrackerLimiter> mem_tracker();

private:
    static AnnIndexPqChunkCache* _s_instance;
    std::unique_ptr<CacheImpl> _cache;
};
```

### 6.2 `PqOnDiskVectorIndex` 新增成员

```cpp
// pq_on_disk_vector_index.h 新增 private 成员

static constexpr size_t kTargetChunkSize = 64 * 1024;  // 64KB target
size_t _chunk_size = 0;        // actual chunk size (code_size aligned)
size_t _vecs_per_chunk = 0;    // = _chunk_size / code_size
```

在 `load()` 中计算：

```cpp
const size_t code_size = _pq->code_size;
_vecs_per_chunk = kTargetChunkSize / code_size;
if (_vecs_per_chunk == 0) _vecs_per_chunk = 1;
_chunk_size = _vecs_per_chunk * code_size;
```

---

## 7. 核心实现：零拷贝 ADC + Chunk 遍历模板

### 7.1 设计思路

原设计是在 `_read_codes_for_bitmap()` 中将 chunk 中需要的 PQ codes memcpy 到中间 `codes[]` 缓冲区，然后在四个 ADC 函数中遍历 `codes[]` 做距离计算。零拷贝方案将两步合并：**ADC 距离计算直接在 cached chunk 内存上执行**，消除中间 `codes[]` 和 `rowid_map[]` 的分配与拷贝。

### 7.2 `_for_each_code_in_bitmap` 模板

四个 ADC 函数（`_adc_topn_l2`、`_adc_topn_ip`、`_adc_range_l2`、`_adc_range_ip`）共用同一个 chunk 遍历模板，chunk 遍历和 cache 逻辑只写一次：

```cpp
// Visitor signature: void(uint32_t rowid, const uint8_t* code)
// The visitor receives the raw PQ code pointer directly into the cached chunk,
// no intermediate buffer or memcpy involved.
template <typename Visitor>
Status PqOnDiskVectorIndex::_for_each_code_in_bitmap(
        const roaring::Roaring& bitmap, Visitor&& visitor) {

    auto* cache = AnnIndexPqChunkCache::instance();
    const size_t code_size = _pq->code_size;

    roaring::Roaring::const_iterator it(bitmap);
    while (it != bitmap.end()) {
        // Collect contiguous run
        uint32_t run_start = *it;
        uint32_t run_end = run_start;
        ++it;
        while (it != bitmap.end() && *it == run_end + 1) {
            run_end = *it; ++it;
        }

        // Iterate chunks overlapping this run
        uint32_t chunk_idx_start = run_start / _vecs_per_chunk;
        uint32_t chunk_idx_end   = run_end / _vecs_per_chunk;

        for (uint32_t ci = chunk_idx_start; ci <= chunk_idx_end; ++ci) {
            int64_t chunk_byte_offset = (int64_t)ci * _chunk_size;
            size_t actual_chunk_bytes = std::min(
                    _chunk_size, _pqdata_file_size - (size_t)chunk_byte_offset);

            // Cache lookup (handle pins the entry for this scope)
            StoragePageCache::CacheKey key(
                    _pqdata_cache_key_prefix, _pqdata_file_size, chunk_byte_offset);
            PageCacheHandle handle;
            bool hit = (cache != nullptr) && cache->lookup(key, &handle);

            // Temporary page for cache-miss or no-cache fallback
            std::unique_ptr<DataPage> tmp_page;

            if (!hit) {
                auto tracker = cache ? cache->mem_tracker()
                                     : /* thread tracker fallback */;
                tmp_page = std::make_unique<DataPage>(actual_chunk_bytes, tracker);
                {
                    std::lock_guard<std::mutex> lock(_io_mutex);
                    _pqdata_input->seek(chunk_byte_offset);
                    _pqdata_input->readBytes(
                            (uint8_t*)tmp_page->data(), cast_set<Int32>(actual_chunk_bytes));
                }
                if (cache) {
                    // Transfer ownership to cache; handle now pins it
                    cache->insert(key, tmp_page.release(), &handle);
                }
            }

            // Pointer to chunk data: either from cache handle or local page
            const uint8_t* chunk_data = hit
                    ? reinterpret_cast<const uint8_t*>(handle.data())
                    : reinterpret_cast<const uint8_t*>(tmp_page->data());

            // Compute overlap between run and chunk
            uint32_t chunk_rowid_start = ci * _vecs_per_chunk;
            uint32_t chunk_rowid_end   = chunk_rowid_start
                    + actual_chunk_bytes / code_size - 1;
            uint32_t overlap_start = std::max(run_start, chunk_rowid_start);
            uint32_t overlap_end   = std::min(run_end, chunk_rowid_end);

            // Zero-copy: visitor reads directly from cached chunk memory
            for (uint32_t r = overlap_start; r <= overlap_end; ++r) {
                const uint8_t* code = chunk_data
                        + (size_t)(r - chunk_rowid_start) * code_size;
                visitor(r, code);
            }
            // PageCacheHandle released here (end of loop iteration)
        }
    }
    return Status::OK();
}
```

### 7.3 ADC 函数示例（零拷贝 L2 TopN）

```cpp
Status PqOnDiskVectorIndex::_adc_topn_l2(
        const float* dis_table, int k,
        const roaring::Roaring& bitmap, IndexSearchResult& result) {

    const size_t M = _pq->M;
    const size_t ksub = _pq->ksub;
    std::vector<uint8_t> unpacked(M);

    std::vector<float> heap_dis(k, std::numeric_limits<float>::max());
    std::vector<faiss::idx_t> heap_ids(k, -1);

    RETURN_IF_ERROR(_for_each_code_in_bitmap(bitmap,
            [&](uint32_t rowid, const uint8_t* code) {
                _decode_pq_code(code, unpacked);
                float dist = 0;
                for (size_t m = 0; m < M; ++m) {
                    dist += dis_table[m * ksub + unpacked[m]];
                }
                if (dist < heap_dis[0]) {
                    faiss::maxheap_replace_top(k, heap_dis.data(), heap_ids.data(),
                                               dist, (faiss::idx_t)rowid);
                }
            }));

    faiss::maxheap_reorder(k, heap_dis.data(), heap_ids.data());
    // ... build result ...
    return Status::OK();
}
```

### 7.4 关键变化

| 维度 | 原设计 | 零拷贝设计 |
|------|--------|-----------|
| 中间缓冲区 | `codes[]` + `rowid_map[]`（n × code_size + n × 4 bytes） | **无**，直接在 chunk 内存上计算 |
| memcpy 次数 | chunk → codes[]（per vector），再由 ADC 读取 codes[] | **零**（visitor 直接读 chunk） |
| 代码重复 | chunk 逻辑在 `_read_codes_for_bitmap`，ADC 逻辑在 4 个函数 | chunk 逻辑在 `_for_each_code_in_bitmap` **写一次**，visitor lambda 不同 |
| PageCacheHandle 生命周期 | 不适用（原方案先 memcpy 再释放） | handle 在 chunk 循环体内，ADC 计算完成后自动释放 |
| `_read_codes_for_bitmap` | 保留 | **移除**，被模板替代 |

---

## 8. 配置

```cpp
// be/src/common/config.h
DECLARE_mString(ann_index_pq_chunk_cache_limit);
DECLARE_mInt32(ann_index_pq_chunk_cache_stale_sweep_time_sec);

// be/src/common/config.cpp
DEFINE_mString(ann_index_pq_chunk_cache_limit, "2%");
DEFINE_mInt32(ann_index_pq_chunk_cache_stale_sweep_time_sec, "1800");
```

**默认 2% 物理内存的推理**：

- PQ codes 已是压缩数据（dim=768 时 96 bytes/vec vs 原始 3072 bytes）。
- 64GB 机器 × 2% = 1.3 GB，可缓存约 1400 万个 vector 的 PQ codes。
- 覆盖数千热点 user（假设每 user 1W vectors × 96 bytes = 960 KB）。
- 远小于 IVF cache 的 70% 默认值，因 PQ codes 本身体积小得多。

---

## 9. ExecEnv 生命周期管理

### 9.1 初始化

在 `exec_env_init.cpp` 中，IVF cache 初始化之后插入：

```cpp
// Init ANN index PQ chunk cache
{
    int64_t pq_cache_limit = ParseUtil::parse_mem_spec(
            config::ann_index_pq_chunk_cache_limit,
            MemInfo::mem_limit(), MemInfo::physical_mem(), &is_percent);
    while (!is_percent && pq_cache_limit > MemInfo::mem_limit() / 2) {
        pq_cache_limit = pq_cache_limit / 2;
    }
    AnnIndexPqChunkCache::create_global_cache(pq_cache_limit);
    LOG(INFO) << "ANN index PQ chunk cache memory limit: "
              << PrettyPrinter::print(pq_cache_limit, TUnit::BYTES)
              << ", origin config value: " << config::ann_index_pq_chunk_cache_limit;
}
```

### 9.2 销毁

在 IVF cache 销毁旁边：

```cpp
AnnIndexPqChunkCache::destroy_global_cache();
```

---

## 10. CacheManager 集成

PQ chunk cache **参与动态容量调整**（不 skip），因为：

- PQ chunk miss 的代价（一次 64KB 顺序读，约 0.05ms on SSD）远小于 IVF list miss（涉及复杂的 coarse quantizer + list 读取）。
- 内存紧张时应优先释放 PQ cache，保留更关键的 IVF / data page cache。
- 无需修改 `cache_manager.cpp`（默认行为即参与调整）。

---

## 11. Cache 失效策略

| 场景 | 失效方式 |
|------|----------|
| Segment 删除（compaction/drop） | LRU 自然淘汰（key 不再被访问），stale sweep 清理 |
| Segment 重建（compaction） | 新 segment 有不同 `_pqdata_cache_key_prefix`，旧条目自然不命中 |
| 内存压力 | `CacheManager` 通过 `adjust_capacity_weighted` 动态缩容 |
| 长时间无访问 | stale sweep（默认 1800s）清理 |
| 显式清理 | `prune_all()` / `prune_stale()` |

**不需要主动 invalidate**：Cache key 包含 file prefix（含 segment rowset 信息），compaction 产生新 segment 自动有新 key。

---

## 12. 性能预期

### 12.1 Cache Hit 收益

| 指标 | 无 Cache（当前） | 有 Cache（命中） | 改善 |
|------|-----------------|----------------|------|
| I/O 延迟 (1W候选, 960KB) | ~0.5~2ms (SSD) | ~0.05ms (memcpy) | **10x~40x** |
| `_io_mutex` 竞争 | 每次查询必抢 | 仅 miss 时抢 | 高并发下显著降低 |
| 同 user_id 第二次查询 | 重读 960KB | ~15 次 cache lookup + memcpy | **接近零 I/O** |
| 同 user_id 不同过滤条件 | 重读子集 | chunk 级别复用 | **部分 I/O 消除** |

### 12.2 内存开销

| 热点规模 | 缓存大小 |
|----------|----------|
| 100 user × 1W vec × 96B/vec | ~96 MB |
| 1000 user × 1W vec × 96B/vec | ~960 MB |
| 默认 2% × 64GB 机器 | 1.3 GB 上限 |

### 12.3 对比 Run 粒度方案

| 维度 | Fixed chunk | Run |
|------|-------------|-----|
| 同 user_id 精确重复查询 | 命中 | 命中 |
| 同 user_id 不同过滤条件 | **部分 chunk 命中** | miss |
| 不同 user_id 共享 chunk | **可能命中** | 不可能 |
| Cache key 复杂度 | 简单 (file, fsize, offset) | 需要 (file, fsize, start, len) |
| 与 IVF cache 架构一致性 | **完全一致** | 不一致 |

---

## 13. 修改文件清单

### 13.1 新增文件（2个）

| 文件 | 内容 |
|------|------|
| `be/src/storage/cache/ann_index_pq_chunk_cache.h` | `AnnIndexPqChunkCache` 类声明 |
| `be/src/storage/cache/ann_index_pq_chunk_cache.cpp` | singleton / lookup / insert 实现 |

### 13.2 修改文件（6个）

| 文件 | 改动 |
|------|------|
| `be/src/runtime/memory/cache_policy.h` | 枚举新增 `ANN_INDEX_PQ_CHUNK_CACHE = 25`；`type_string()` 新增 case；`StringToType` 新增条目 |
| `be/src/common/config.h` | 新增 `DECLARE_mString(ann_index_pq_chunk_cache_limit)` 和 `DECLARE_mInt32(ann_index_pq_chunk_cache_stale_sweep_time_sec)` |
| `be/src/common/config.cpp` | 新增 `DEFINE_mString(ann_index_pq_chunk_cache_limit, "2%")` 和 `DEFINE_mInt32(ann_index_pq_chunk_cache_stale_sweep_time_sec, "1800")` |
| `be/src/runtime/exec_env_init.cpp` | 初始化和销毁 `AnnIndexPqChunkCache` |
| `be/src/storage/index/ann/pq_on_disk_vector_index.h` | 新增 `kTargetChunkSize`、`_chunk_size`、`_vecs_per_chunk` 成员；新增 `_for_each_code_in_bitmap` 模板；移除 `_read_codes_for_bitmap` |
| `be/src/storage/index/ann/pq_on_disk_vector_index.cpp` | 移除 `_read_codes_for_bitmap()`，实现 `_for_each_code_in_bitmap` 模板；重写四个 ADC 函数为零拷贝模式；`load()` 中计算 chunk 参数 |

### 13.3 可选修改

| 文件 | 改动 | 必要性 |
|------|------|--------|
| `be/src/runtime/exec_env.h` | 新增 getter/setter（如果不走 singleton 模式） | 可选（走 singleton 则无需改） |

---

## 14. 实施步骤

| 步骤 | 内容 | 涉及文件 |
|------|------|----------|
| 1 | 注册 `ANN_INDEX_PQ_CHUNK_CACHE` CacheType | `cache_policy.h` |
| 2 | 添加配置项 | `config.h`、`config.cpp` |
| 3 | 实现 `AnnIndexPqChunkCache` 类 | 新增 `ann_index_pq_chunk_cache.h/cpp` |
| 4 | 初始化/销毁注册到 ExecEnv | `exec_env_init.cpp` |
| 5 | `pq_on_disk_vector_index.h` 新增 chunk 相关成员 | `pq_on_disk_vector_index.h` |
| 6 | `load()` 中计算 `_chunk_size` / `_vecs_per_chunk` | `pq_on_disk_vector_index.cpp` |
| 7 | 实现 `_for_each_code_in_bitmap` 模板 + 零拷贝 ADC 重写 | `pq_on_disk_vector_index.cpp` |
| 8 | 单元测试 | 新增 UT |
| 9 | 回归测试验证功能正确性 | 已有 `ann_pq_on_disk_basic.groovy` |

---

## 15. 测试计划

| 测试 | 内容 |
|------|------|
| UT: cache hit | 两次相同 bitmap 查询，第二次验证 I/O 为 0 |
| UT: cache miss | 首次查询，验证结果正确且条目被 insert |
| UT: eviction | 填满 cache 后插入新条目，验证旧条目被淘汰 |
| UT: cross-query hit | 两个 bitmap 覆盖同一 chunk 的不同子集，验证 chunk 复用 |
| UT: chunk boundary | run 跨多个 chunk，验证结果正确拼接 |
| UT: last chunk | 最后一个 chunk 的 `actual_chunk_bytes < _chunk_size` |
| UT: no cache | `AnnIndexPqChunkCache::instance()` 为 nullptr 时退化为直接 I/O |
| 回归测试 | 已有 `ann_pq_on_disk_basic.groovy` 覆盖 L2/IP TopN/Range |

---

## 16. 后续优化方向

1. **Async prefetch**：在 `_read_codes_for_bitmap()` 中，对下一个 chunk 发起异步预读，隐藏 I/O 延迟。
2. **Segment 级全量 cache**：对于小 segment（如 `ann.pqdata < 某阈值`），一次性缓存整个文件，省去 chunk 分割开销。
3. **MMap 替代选项**：对于支持 mmap 的部署环境，提供 mmap 模式替代 LRU cache（类似 Milvus/Qdrant）。
4. **自适应 chunk size**：根据运行时统计的候选集大小自动调整 chunk_size。
5. **Cache warmup**：在 segment 加载时主动预热热点 chunk（例如根据历史查询频率）。
