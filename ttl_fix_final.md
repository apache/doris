目标说明
TTL 重构已经可以解决所有 TTL 已知问题，但是由于重构上线周期很长，在这段时间里需要一个范围受控的临时修复方案。

我们要做的：
- 使用 ttl 后系统稳定，没有 cache 空间泄漏问题，没有 NOT FOUND miss 问题
- 保证 query / write / warmup / sync_meta 对同一对象得到一致的 expiration_time
- 不改变 `newest_write_timestamp` 在 cooldown、warmup_delta_data 等现有逻辑里的语义
在临时修复方案里，我们不做：
- 完美优雅的修复（重构已经做了这部分工作）
- 允许用户频繁 alter table ttl 属性
- 保持“新写入会刷新 TTL”这类旧语义

问题
内存中的 FileCacheKey.meta.expiration_time 和 磁盘目录路径里的 .../<hash>_<expiration_time>/... 两者不一致。

这类不一致会表现为：
1. 同一个 hash 在不同入口（query/warmup/import）下反复触发 modify_expiration_time / rename；
2. 磁盘目录中出现“旧 expiration 目录残留”，但内存里已是“新 expiration”；
3. 导入后立即查询命中同 hash 时，出现 TTL 行为漂移（是否进入 TTL queue、是否被视为过期） 。

- ttl_seconds：持续时长（duration，秒）。
- expiration_time：绝对时间戳（Unix seconds）。
- “磁盘 TTL”指 FSFileCacheStorage 的目录名后缀：<hash>_<expiration_time>。
- base_timestamp：本次临时修复里用于计算 file cache TTL 的稳定基准时间，统一取 `tablet_meta->creation_time()`。

TTL 端到端传递路径分析
1. 查询路径
1. 元数据来源
  - TabletMeta::ttl_seconds()
  - TabletMeta::creation_time()
2. 读取时计算 expiration
  - be/src/olap/rowset/beta_rowset_reader.cpp
  - 写入 _read_options.io_ctx.expiration_time
3. 进入 file cache
  - CachedRemoteFileReader / BlockFileCache::get_or_set
  - CacheContext.expiration_time -> FileCacheKey.meta.expiration_time
4. 落磁盘
  - FSFileCacheStorage::get_path_in_local_cache_v2(hash, expiration_time)
  - 目录编码为 <hash>_<expiration_time>
结论：query 侧是“内存 key + 磁盘目录”共享同一个 `io_ctx.expiration_time` 的主路径。

2. Warmup 路径（Sync Rowset / Job / Event-driven）

入口三类：
- be/src/cloud/cloud_tablet.cpp（add_rowsets(... warmup_delta_data)）
- be/src/cloud/cloud_warm_up_manager.cpp（周期/一次性 warmup）
- be/src/cloud/cloud_internal_service.cpp（事件驱动 warmup）
流程：

1. 构造 DownloadFileMeta.ctx.expiration_time
2. BlockFileCacheDownloader 下载并写 cache
3. 进入 BlockFileCache、最终落盘到 <hash>_<expiration_time>
结论：warmup 是第二条“主动写 cache”的主路径，如果计算规则与 query/import 不一致，会直接制造同 hash 不同 expiration。

3. 导入写路径
核心链路：
1. Rowset writer context 初始化
  - context.file_cache_base_timestamp
  - context.file_cache_ttl_sec = tablet->ttl_seconds()
2. 生成 file writer options
  - be/src/olap/rowset/rowset_writer_context.h
  - FileWriterOptions.file_cache_expiration
3. 远端写 + 本地 cache
  - S3FileWriter::appendv
  - UploadFileBuffer::on_upload
  - upload_to_local_file_cache
4. cache builder 把 expiration 带入 block key
  - FileWriter::init_cache_builder
  - FileCacheAllocatorBuilder::allocate_cache_holder
  - CacheContext.expiration_time -> FileCacheKey.meta.expiration_time
5. 落盘路径
  - FSFileCacheStorage 目录名 <hash>_<expiration_time>
结论：导入路径会在“写入当下”把 expiration 固化到 cache block；如果 base_timestamp 不稳定，就会和 query/warmup 产生长期分叉。

可能导致“不一致”的主要场景
以下按影响与复现概率排序。
A. 读写 clamp 规则不一致（过期归零 vs 不归零）
触发条件：
- query 侧对 newest_write_timestamp + ttl <= now 做 clamp（变 0）
- import/warmup 某些路径仍写非 0 绝对时间
结果：
- 同 hash 在 query 得到 expiration=0，在导入/预热得到 expiration>0
- modify_expiration_time/rename 高频触发
- 短期内出现“磁盘目录还是旧 expiration，内存已切新 expiration”

B. 以 rowset write time 为基准不稳定（pending rowset t0/t1 分裂只是其中一种表现）
触发条件：
- 不同入口依赖 rowset 级别时间戳（尤其是 newest_write_timestamp）
- 同一对象在导入、build/commit、query、warmup 看到的“写入时间”并不天然一致
结果：
- 导入写 cache 可能使用 t0 + ttl
- 查询/预热读取 rowset_meta 可能使用 t1 + ttl
- 未改 TTL property 也会出现一致性问题
这是你观察到“没有 alter ttl 也不一致”的关键来源之一。

C. Warmup 路径自成一套计算规则
触发条件：
- warmup 入口（sync_rowset/job/event-driven）未完全复用 query/write 的统一计算
- 或某个入口未做 clamp
结果：
- warmup 成为第三个 expiration_time 来源
- 同 hash 在 query/import/warmup 三端形成三值竞争

修复方案
A. 读侧 clamp（过期归零） vs 写侧不 clamp（仍写非 0 绝对时间）
抽象统一的 expiration 计算函数，并在写路径补齐 clamp
int64_t calc_file_cache_expiration(int64_t base_timestamp, int64_t ttl_seconds) {
    if (ttl_seconds <= 0 || base_timestamp <= 0) return 0;
    int64_t exp = base_timestamp + ttl_seconds; // 需溢出保护
    return exp > UnixSeconds() ? exp : 0;
}
- Query：be/src/olap/rowset/beta_rowset_reader.cpp
- Write：be/src/olap/rowset/rowset_writer_context.h
- Warmup：be/src/cloud/cloud_internal_service.cpp、be/src/cloud/cloud_tablet.cpp、be/src/cloud/cloud_warm_up_manager.cpp
- SyncMeta：be/src/cloud/cloud_tablet.cpp
修完后，写入 <hash>_<expiration_time> 与后续 query/warmup 的规则一致，消除“写非0、读0”的系统性差异。

B. 使用稳定的 tablet 级基准，避免复用 `newest_write_timestamp`
以 TabletMeta/TabletMetaSharedPtr 的 creation_time() 作为 file cache expiration 基准
思路：
- 不再用 newest_write_timestamp + ttl；
- 改为 tablet_meta->creation_time() + ttl（并做 clamp）；
- query/write/warmup/sync_meta 统一显式传递这个稳定基准。
核心收益：
1. 规避 B 类 t0/t1 漂移；
2. 基准稳定，减少同 hash 的反复改 expiration；
3. 不需要修改 newest_write_timestamp 语义，避免影响 cooldown、warmup_delta_data 等其他逻辑。
风险/语义变化（很重要）：
1. TTL 语义从“相对最新写入”变为“相对 tablet 创建时间”；
2. 新写入不会延长缓存寿命；
3. clone/restore/rebuild 场景下 creation_time 语义可能不等价于“数据新鲜度”；
4. 历史 meta 若 creation_time 缺失（0）需定义 fallback，本次临时修复统一按非 TTL（expiration=0）处理；
5. 若目标是“随写入刷新 TTL”，该方案有意放弃这种语义，优先保证稳定性与一致性。

C. Warmup 路径的 expiration 计算规则不一致（尤其缺少 clamp）

warmup 与 query/write 完全复用同一套 calc + clamp
把 warmup 入口统一改成：
- expiration_time = calc_file_cache_expiration(tablet_meta->creation_time(), ttl_seconds)
至少覆盖：
- be/src/cloud/cloud_internal_service.cpp
- be/src/cloud/cloud_tablet.cpp
- be/src/cloud/cloud_warm_up_manager.cpp

D. `sync_meta` 发现 TTL anchor 相关元数据变化时，data/index 都要一起更新

当 `sync_meta` 发现 `tablet_meta->creation_time()` 或 `tablet_meta->ttl_seconds()` 变化时：
- 重新计算 `calc_file_cache_expiration(tablet_meta->creation_time(), new_ttl_seconds)`
- 用这个统一结果更新 segment data cache
- 同时更新 inverted index cache

如果只更新 segment 而漏掉 index，仍会残留同一 rowset 内部的 TTL 视图不一致问题。

验收标准
1. query/write/warmup/sync_meta 四条链路对同一 tablet 内对象使用相同的 base_timestamp。
2. file cache TTL 计算不再依赖 newest_write_timestamp，因此不改变 cooldown、warmup_delta_data 等现有逻辑语义。
3. 对于 old tablet 的 late write，TTL 不会被“刷新”；这属于本次临时修复引入且接受的语义变化。
4. 已过期或非法 base_timestamp 统一落到 expiration_time=0，避免同 hash 多值竞争。
5. `sync_meta` 在 TTL anchor 相关元数据变化时，segment data 和 inverted index 都会一起迁移到新的 expiration。

版本/分支
master/branch-4.1/cloud-26.1 带了正式的修复，与临时修复冲突，所以不用也不能 pick
branch-4.0 + branch-3.1 版本可以 pick
