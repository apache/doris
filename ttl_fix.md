# Doris BE FileCache TTL/Expiration 临时修复方案（切换到 Tablet Creation Time）

本文基于当前临时修复分支上的实现与 review 结论，重新整理 FileCache TTL 的短期修复方案。目标不是恢复“理想 TTL 语义”，而是在正式 TTL 重构上线前，优先解决 **expiration_time 在 query / write / warmup / sync_meta 间不一致** 导致的 cache 漂移、rename 抖动和残留目录问题。

> 约定：
> - `ttl_seconds` 是 duration（秒数）
> - `expiration_time` 是绝对时间戳（Unix epoch seconds）
> - `base_timestamp` 是本次临时修复中用于计算 file cache expiration 的稳定时间基准
> - “磁盘 TTL”指 `FSFileCacheStorage` 目录名中编码的 `.../<hash>_<expiration_time>/...`

## 0. 结论先行

临时修复不再尝试“前移并复用 `newest_write_timestamp`”，而是：

1. 保留统一的 `expiration_time` helper，统一 clamp / overflow / invalid-base 规则；
2. 将 file cache TTL 的 `base_timestamp` 统一切换为 `tablet_meta->creation_time()`；
3. 明确 `newest_write_timestamp` 继续只承担 rowset freshness 语义，不再作为 file cache TTL 基准；
4. `query / write / warmup / sync_meta` 四条链路全部使用同一套 `creation_time + ttl_seconds` 计算；
5. `sync_meta` 在 TTL 属性变化时，不仅更新 segment data cache，也要同步更新 inverted index cache，避免只改一半。

## 1. 为什么放弃 `newest_write_timestamp` 方案

上一版思路是：

- 保持 `expiration_time = newest_write_timestamp + ttl_seconds`
- 为了让 write/read/warmup 使用同一个时间基准，把 pending rowset 的 `newest_write_timestamp` 提前写入 rowset meta

这个方向虽然能缓解 `t0/t1` 分裂，但 review 后确认有两个问题：

1. **它改变了 `newest_write_timestamp` 的既有语义**
   - cooldown、warmup_delta_data 的“最近写入”判定、cloud freshness fallback 等逻辑，都默认它表达 rowset 的写入新鲜度；
   - 把它前移到 writer 初始化时刻，本质上会让 rowset“看起来更老”。

2. **它修的是 file cache TTL，不该顺带改其他功能的时间语义**
   - 临时修复应该把影响面收敛在 TTL 自身；
   - 如果为了修 TTL 而重定义 `newest_write_timestamp`，副作用范围会超出本次修复目标。

所以这次方案明确回退这部分改动：**撤掉 pending rowset 提前持久化 `newest_write_timestamp` 的实现。**

## 2. 为什么接受 `tablet_meta->creation_time()` 作为 base

TTL 当前的问题，本质上不是 “+ ttl” 这一步，而是 `base` 不稳定。

使用 `tablet_meta->creation_time()` 的理由：

1. **稳定**
   - tablet 创建后该值天然固定，不会在 import/build/commit/query/warmup 间漂移。

2. **天然是 tablet 级别**
   - file cache TTL 的竞争对象实际是 tablet 下同一远端对象对应的 cache hash；
   - 用 tablet 级时间基准比 rowset 级时间基准更容易保证全链路一致。

3. **不影响 `newest_write_timestamp` 现有语义**
   - cooldown、warmup_delta_data 等依赖“最近写入时间”的逻辑可以保持不变；
   - TTL 修复只影响 file cache expiration 的计算。

4. **符合当前业务接受范围**
   - 这个临时方案主要面向 cloud + dynamic partition 场景；
   - partition/tablet 会持续新建，TTL 按 tablet 创建时间计算通常是可接受的。

## 3. 语义变化与接受范围

切到 `tablet_meta->creation_time()` 后，TTL 语义会从：

- “相对最新写入时间过期”

变为：

- “相对 tablet 创建时间过期”

这意味着：

1. **晚到写入不会刷新 TTL**
   - 老 tablet 在 TTL 窗口过去之后，即使有新写入，新的 cache block 也会按 `expiration_time = 0` 进入 normal queue。

2. **TTL 不再表达数据新鲜度，而表达 tablet 生命周期窗口**
   - 这不是最终理想语义，但它是本次临时修复有意接受的 tradeoff。

3. **creation_time 非法时统一降级为非 TTL**
   - 若 `creation_time <= 0`，则 `expiration_time = 0`；
   - 这样不会制造新的多值竞争。

本次临时修复明确接受上述语义变化，优先保证稳定性、一致性和不影响其他逻辑。

## 4. 统一 helper：只保留一套计算规则

统一 helper 形态：

```c++
int64_t calc_file_cache_expiration_time(int64_t base_timestamp, int64_t ttl_seconds) {
    if (ttl_seconds <= 0 || base_timestamp <= 0) {
        return 0;
    }
    if (base_timestamp > std::numeric_limits<int64_t>::max() - ttl_seconds) {
        return 0;
    }
    int64_t expiration_time = base_timestamp + ttl_seconds;
    return expiration_time > UnixSeconds() ? expiration_time : 0;
}
```

规则统一为：

1. `ttl_seconds <= 0` -> `0`
2. `base_timestamp <= 0` -> `0`
3. 溢出 -> `0`
4. 已过期 -> clamp 到 `0`

这样可以避免 query / write / warmup / sync_meta 因为“是否 clamp”或“边界值不同”再次分叉。

## 5. 代码落点

### 5.1 Write Path

写路径不再使用 `newest_write_timestamp` 算 file-cache expiration，而是显式传递：

- `RowsetWriterContext.file_cache_base_timestamp = tablet_meta->creation_time()`

然后在 `RowsetWriterContext::get_file_writer_options()` 中统一调用 helper：

- `calc_file_cache_expiration_time(file_cache_base_timestamp, file_cache_ttl_sec)`

这样导入写 cache 时固化下来的 expiration，与后续 query/warmup/sync_meta 的稳定基准一致。

### 5.2 Read Path

读路径通过 tablet reader 将稳定基准下传到 rowset reader：

- `RowsetReaderContext.file_cache_base_timestamp = tablet->tablet_meta()->creation_time()`

`BetaRowsetReader` 不再读取 rowset 的 `newest_write_timestamp` 来算 TTL。

### 5.3 Warmup Path

三类 warmup 入口统一使用：

- `calc_file_cache_expiration_time(tablet_meta->creation_time(), tablet_meta->ttl_seconds())`

覆盖：

- `cloud_tablet.cpp`（sync rowset warmup）
- `cloud_internal_service.cpp`（event-driven warmup）
- `cloud_warm_up_manager.cpp`（job warmup）

### 5.4 Sync Meta

当 `sync_meta` 发现 `tablet_meta->creation_time()` 或 `tablet_meta->ttl_seconds()` 发生变化时：

1. 重新计算目标 expiration：
   - `calc_file_cache_expiration_time(tablet_meta->creation_time(), new_ttl_seconds)`
2. 对当前 tablet 下所有 cached segment data 调 `modify_expiration_time`
3. **对当前 tablet 下所有 cached inverted index 也调 `modify_expiration_time`**

最后一点很重要。若只改 segment，不改 index，会留下同一 rowset 内 data/index TTL 分叉的问题。

## 6. 不做的事情

本次临时修复不做：

1. 不恢复“写入刷新 TTL”的旧语义
2. 不尝试兼容频繁 `ALTER TABLE TTL`
3. 不做跨 clone / restore / rebuild 的更复杂时间语义修正
4. 不保证这是最终最优解，正式 TTL 重构仍然是长期方案

## 7. 验收标准

### 7.1 行为一致性

同一个 tablet 内同一个远端对象，在以下路径中使用相同的 `base_timestamp`：

- query
- write
- warmup
- sync_meta

### 7.2 语义隔离

- file cache TTL 不再依赖 `newest_write_timestamp`
- `newest_write_timestamp` 语义保持不变，不影响 cooldown、warmup_delta_data 等逻辑

### 7.3 边界统一

- 非法 base、已过期、overflow 都统一得到 `expiration_time = 0`

### 7.4 TTL 属性变更后的完整性

- `sync_meta` 修改 expiration 时，segment data 和 inverted index 都被覆盖

## 8. 建议验证

1. **UT**
   - helper 对 invalid / expired / overflow 的 clamp 行为
   - writer-side `file_cache_base_timestamp` 传播

2. **Cloud docker regression**
   - 新创建 tablet 立即写入，TTL queue 仍生效
   - 创建 table 后等待超过 TTL 再写入，cache 不再进入 TTL queue，而进入 normal queue

3. **Code inspection**
   - 确认没有剩余路径仍使用 `newest_write_timestamp` 计算 file-cache expiration

## 9. 适用分支

- `branch-4.0`
- `branch-3.1`

`master / branch-4.1 / cloud-26.1` 已有正式修复路径，和本临时方案方向不同，不应直接 pick 本补丁。
