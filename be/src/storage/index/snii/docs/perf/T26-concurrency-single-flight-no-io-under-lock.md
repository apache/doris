> **优先级：低（暂定，用户 2026-06-28 决定）。** 当前实现无锁内 IO；searcher cache 锁有界且分片。
> 本任务的并发加固（reader-open single-flight、`_section_ranges` per-read 去锁、共享块缓存分片）暂不优先。
> 但请注意：若实现 **T04** 的 DICT block 缓存，其并发安全（默认 **request-scoped**、锁外解压）仍是必须遵守的设计约束，
> 属 T04 自身范围、不依赖本任务先行。

＜T26 并发安全契约：reader 打开 single-flight + 锁内禁 IO 结构不变量 + 共享块缓存请求级化/分片规则＞

# 1. 目标与背景

## 问题与 finding 映射
本任务不是单点性能优化，而是**为整个 SNII 读路径确立并固化并发契约**，并修掉一个现存隐患。依据 `findings/CONCURRENCY.md`：

- **现状（无需改）**：锁面极小，当前**没有锁内 IO、也没有粗锁串行化查询**。
  - `DorisSniiFileReader::_classify_section` 持 `std::shared_lock` 仅扫 ≤5 个 `SectionRange`，函数返回即释放，真正 IO（`_read_at`→`_reader->read_at`）在锁外（`snii_doris_adapter.cpp:141-155`、`:166-180` read_at、`:209-283` read_batch；证据见 CONCURRENCY.md 一节-1）。
  - `s3_object_store.cpp` 的 `g_api_mu` 仅护 `Aws::InitAPI/ShutdownAPI` 引用计数（`s3_object_store.cpp` 内 `#ifdef SNII_WITH_S3`），非 Doris 生产路径、不护 GetObject（CONCURRENCY.md 一节-2）。
  - 跨查询共享的 `LogicalIndexReader`（经 `InvertedIndexSearcherCache` 缓存，`snii_index_reader.cpp:343-346`）读路径 const 无锁（`logical_index_reader.h:54` lookup() const；on-demand dict block 解码进**栈局部** `OnDemandDictBlock`，`logical_index_reader.h:124-130`）。

- **隐患 H2（本任务修复，现存）**：`_get_logical_reader`（`snii_index_reader.cpp:299-352`）在 searcher cache miss 时**直接** `open_snii_index`（:333-334，做 meta + resident dict + BSBF 的 IO）再 insert，**无 in-flight 去重**。已读码确认 `InvertedIndexSearcherCache::lookup/insert`（`inverted_index_cache.cpp:88-115`）底层是 `ShardedLRUCache`（`lru_cache_policy.h:43-62`），分片锁保证 map 操作线程安全，但**对"同 key 并发 open"无任何去重**：N 个并发 miss 同一 index ⇒ N 次完整 open IO（thundering herd），N-1 次 insert 浪费。高 QPS + 冷缓存/淘汰风暴拖并发。

- **隐患 H1（本任务确立契约、提供测试基建，实现归 T04/T07）**：给跨查询共享的 `LogicalIndexReader` 加可变 dict-block 缓存 = 引入共享可变状态；naive 实现（一把 `std::mutex` 包整个缓存且锁内做 ~64KB zstd 解压 + CRC）会同时犯"锁粒度过粗 + 锁内重活"，串行化最热的 term lookup（CONCURRENCY.md 二节）。

## 预期收益
- **H2**：N 并发冷查询同 index 的底层 `open`/meta 读次数从 N 降到 1（确定性可验证）。
- **H1**：把"NO-IO-UNDER-LOCK / NO-DECOMPRESS-UNDER-LOCK / 共享块缓存必须 request-scoped 或分片且解压在锁外"写成**结构性、单测可验**的不变量与可复用测试基建，给 T04/T07 当硬约束护栏，避免将来挖坑。

# 2. 影响的文件/函数（当前签名）

- `be/src/storage/index/snii/snii_doris_adapter.h` / `.cpp`
  - `uint8_t DorisSniiFileReader::_classify_section(uint64_t offset, size_t len) const`（`.cpp:134-155`，持 `std::shared_lock lock(_section_ranges_mutex)`）。
  - `::doris::snii::Status DorisSniiFileReader::_read_at(...) const`（`.cpp:182-207`，真正 IO，**不**持锁）。
  - `read_at`（:166-180）、`read_batch`（:209-283）：调用序 `_classify_section`（持锁）→ 锁释放 → `_read_at`（IO）。
  - 成员 `mutable std::shared_mutex _section_ranges_mutex;`（`.h:98`）。
- `be/src/storage/index/snii/snii_index_reader.cpp`
  - `Status SniiIndexReader::_get_logical_reader(context, searcher_cache_handle, uncached_reader, logical_reader)`（:299-352）——cache miss 分支 :329-351 无 single-flight。
- `be/src/storage/index/inverted/inverted_index_cache.{h,cpp}`（只读确认语义，不改）：`lookup`/`insert`/`_insert`（:88-115）走 `ShardedLRUCache`。
- 新增：`be/src/storage/index/snii/common/single_flight.h`、`be/src/storage/index/snii/common/lock_witness.h`。
- 新增测试：`be/test/storage/index/snii_concurrency_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）。

# 3. 变更设计

## 3.1 NO-IO-UNDER-LOCK：把"锁内禁 IO"做成结构性、可单测的不变量
当前代码已经"分类持锁 / IO 锁外"，本任务把它**固化为可机验的不变量**，而非靠人读码保证。

新增 `be/src/storage/index/snii/common/lock_witness.h`（header-only，零成本 thread_local 计数）：
```cpp
namespace doris::snii::testing {
// 每个受护临界区一个 thread_local 深度计数；进入临界区 ++、退出 --。
// 业务侧用 LockWitnessGuard 在持锁作用域内自增；测试侧在 IO/解压入口断言对应计数 == 0。
int& classify_lock_depth();      // DorisSniiFileReader::_section_ranges_mutex
int& dict_cache_lock_depth();    // 预留给 T04/T07 的 per-reader 块缓存分片锁
struct LockWitnessGuard {
    explicit LockWitnessGuard(int& d) : d_(d) { ++d_; }
    ~LockWitnessGuard() { --d_; }
    int& d_;
};
}
```
改 `_classify_section`：在 `std::shared_lock` 同作用域内放 `doris::snii::testing::LockWitnessGuard w(doris::snii::testing::classify_lock_depth());`（仅一次 thread_local 自增/自减，release 编译可忽略成本；不引入额外锁）。
不变量测试：用扩展版 `RecordingFileReader`，在其 `read_at_impl` 回调里 `EXPECT_EQ(doris::snii::testing::classify_lock_depth(), 0)`——证明物理 IO 发生时分类锁未被本线程持有（结构性证明，无需 race）。read_batch 同理。

> 说明：选 witness 计数而非"让 mock 去 try_lock 私有 mutex"，因为 `_section_ranges_mutex` 私有不可达；witness 是确定性、无竞态、可永久保留的护栏。

## 3.2 reader-open single-flight（修 H2）
新增 header-only 原语 `be/src/storage/index/snii/common/single_flight.h`：
```cpp
namespace doris::snii {
// 同 key 并发只执行一次 loader；其余等待复用结果。loader 在 in-flight 占位之后、
// 全局小锁之外执行；等待用 condition_variable。仅护一张 per-key 小 map，绝不在锁内做 IO。
template <class Key, class Value>
class SingleFlight {
public:
    // loader: () -> std::pair<Status, std::shared_ptr<Value>>
    template <class Loader>
    Status do_once(const Key& key, Loader&& loader, std::shared_ptr<Value>* out);
private:
    struct Call { std::mutex done_mu; std::condition_variable cv; bool done=false;
                  Status st; std::shared_ptr<Value> val; };
    std::mutex map_mu_;                                   // 仅护 calls_
    std::unordered_map<Key, std::shared_ptr<Call>> calls_;
};
```
语义（关键不变量，写进注释）：
1. 持 `map_mu_` 仅查/插 `calls_`；判定 leader/follower 后**立即释放** `map_mu_`。
2. leader 在**锁外**执行 `loader()`（真正 open IO），完成后取 `done_mu` 写结果、erase map、`cv.notify_all()`。
3. follower 在 `done_mu`/`cv` 上等结果，**绝不在持有 `map_mu_` 期间阻塞或做 IO**。
4. 任意 key 任意时刻最多一个在飞 loader。

在 `snii_index_reader.cpp` 引入进程级单例协调器（keyed by `searcher_cache_key.index_file_path` 字符串）：
```cpp
SingleFlight<std::string, doris::snii::reader::LogicalIndexReader>& snii_reader_open_coordinator();
```
改写 `_get_logical_reader` cache-miss 分支（:329-351）为：
- 先 `lookup`（命中直接返回，:314-327 不变）。
- miss 且 `enable_searcher_cache` 时，调 `do_once(index_file_path, loader, &shared)`；loader 内部：再 `lookup` 一次（double-check，可能别的 leader 刚插好）→ 仍 miss 则 `init` + `open_snii_index`（:331-334）→ 构造 `CacheValue` + `insert`（:342-346）→ 返回 shared 句柄。follower 直接复用 leader 已 insert 的 cache 条目（loader 返回后再 `lookup` 取 handle，保证句柄计数正确）。
- `enable_searcher_cache==false`（:336-339）路径不变（无共享、无需去重）。
- 错误传播：leader open 失败 → loader 返回错误 Status，所有 follower 同样收到该 Status（不缓存失败，下次重试）。

> FORMAT-COMPATIBILITY：reader/writer-only，零在盘字节变更（本任务不碰任何编解码）。
> CONCURRENCY 结论：
> - `_classify_section` 仍是 shared_lock 下 ≤5 项扫描，IO 在锁外（强化为 witness 可验）。
> - SingleFlight 的 `map_mu_` 只护一张小 map；open IO 在锁外；NO-IO-UNDER-LOCK 维持。
> - 不向共享 `LogicalIndexReader` 新增任何可变状态（lookup 仍 const 无锁）。

## 3.3 共享块缓存契约（H1，给 T04/T07 的硬约束 + 测试基建）
本任务**不**实现 dict-block 缓存，但确立并提供：
- 契约（落总设计文档与代码注释）：任何 per-reader 块缓存**必须二选一**——(A) request-scoped（每查询、无共享可变状态、无锁，默认推荐）；或 (B) 分片 lock-striped，**锁只护 map 查/插，zstd 解压/CRC 在锁外局部缓冲完成后再插入**。**红线：任何持锁期间禁止 FileReader IO / zstd 解压 / CRC。**
- 测试基建：`dict_cache_lock_depth()` witness（同 3.1）+ `doris::snii::format::dict_decode_counter()`（解压计数 seam，测试间 reset）。T04/T07 必须用它们断言：并发 lookup 下 `dict_decode_counter() == unique_blocks`（request-scoped）或 `≤ unique_blocks×分片冗余上界`（分片），且解压入口 `dict_cache_lock_depth()==0`。
- 本任务提供一个 `DISABLED_` stub 测试 + TODO，挂接基建，待 T04 落地后启用（见 gaps）。

# 4. 依赖
- **提供给**：T04（per-reader dict-block 缓存）、T07（resident dict 缓存策略）——消费 `single_flight.h`、`lock_witness.h`、`dict_decode_counter()` 及本契约。
- **被依赖**：无（Batch 1，可独立先行）。`depends_on=[]`。
- 复用现有：`RecordingFileReader`（`snii_doris_adapter_test.cpp:53-97`）、`MemoryFile`/`build_reader()`（`snii_query_test.cpp:53-101,203-275`）、`IoMetrics`/`MeteredFileReader`（`io_metrics.h`、`metered_file_reader.h`）。

# 5. TDD 步骤（RED → GREEN → REFACTOR）

**步骤 A — NO-IO-UNDER-LOCK 不变量**
- RED：在 `snii_concurrency_test.cpp` 写 `TEST(SniiReaderConcurrencyTest, ClassifySectionHoldsNoLockDuringIo)`：扩展 RecordingFileReader 在 `read_at_impl` 内 `EXPECT_EQ(classify_lock_depth(),0)`；构造带 section refs 的 `DorisSniiFileReader`，跑 `read_at` + `read_batch`。此时 `lock_witness.h` 不存在 → 编译失败（RED）。
- GREEN：新增 `lock_witness.h`；`_classify_section` 内加 `LockWitnessGuard`。测试转 GREEN（IO 时计数=0）。
- REFACTOR：确认 guard 作用域恰好等于 `shared_lock` 作用域；release 路径零额外开销注释。

**步骤 B — SingleFlight 原语**
- RED：写 `TEST(SniiReaderConcurrencyTest, SingleFlightInvokesLoaderOnce)`：N=8 线程并发 `do_once(同 key, loader)`，loader 内 `atomic calls++` 且用 latch 卡住直到 8 线程全部进入，断言 `calls==1` 且各线程拿到同一 `shared_ptr`。`single_flight.h` 不存在 → 编译失败。
- GREEN：实现 `single_flight.h`（§3.2 语义）。测试转 GREEN。
- 追加 RED/GREEN：`TEST(..., SingleFlightDifferentKeysRunConcurrently)`（不同 key 各跑一次，calls==key 数）、`TEST(..., SingleFlightPropagatesLoaderError)`（leader 失败 → 全员收到错误、不缓存、下次重试 calls 再+1）。

**步骤 C — single-flight 锁外不变量**
- RED：`TEST(..., SingleFlightDoesNotHoldMapLockDuringLoad)`：loader 入口断言一个 witness（`single_flight` 内 `map_lock_depth` thread_local，进入临界区 ++、释放前 --）为 0 → 若实现误在持 map_mu_ 时调 loader 则失败。
- GREEN：确保实现先释放 `map_mu_` 再 loader。

**步骤 D — wiring 进 _get_logical_reader**
- 用 `snii_reader_open_coordinator()` 包住 miss 分支（§3.2）。该路径依赖 Doris 单例，不做纯单测（见 gaps）；以 B/C 的原语单测 + 现有 doris regression 覆盖。REFACTOR：抽 `_open_logical_reader_uncached()` 私有函数当 loader 体，保持 `_get_logical_reader` 可读。

**步骤 E — H1 契约 stub**
- 写 `TEST(SniiDictCacheConcurrencyTest, DISABLED_ConcurrentLookupDecodesEachBlockOnce)` + TODO，挂 `dict_decode_counter()`/`dict_cache_lock_depth()`，留给 T04 启用。

# 6. 功能验证（target：`doris_be_test`，filter `Snii*Concurrency*` / `DorisSniiFileReaderTest.*`）

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| F1 ClassifyNoLockDuringIo | RecordingFileReader + 注册 5 类 section refs | `read_at(off,len)` 命中某 section | 调 read_at | 回调内 `classify_lock_depth()==0`；返回字节正确（沿用 `DorisSniiFileReaderTest` 风格全量比对）| 锁内禁 IO 结构不变量（read_at）|
| F2 BatchNoLockDuringIo | 同上 | 3 个 range（合并成 1 物理读，复用 `snii_doris_adapter_test.cpp:158-160` 模式）| read_batch | 回调内 `classify_lock_depth()==0`；`reads().size()==1` 且 offset/len 与合并后一致；各 out 字节正确 | 锁内禁 IO（read_batch）+ 合并正确性等价 |
| F3 SingleFlightOnce | 计数 loader + 8 线程 latch | 同 key | 并发 do_once | `loader_calls==1`，8 个返回指针 `==` 同一对象 | single-flight 正确性 |
| F4 SingleFlightDistinctKeys | 计数 loader | 4 个不同 key×各 2 线程 | 并发 do_once | `loader_calls==4`，每 key 内 2 线程同对象 | 不同 key 不互相阻塞 |
| F5 SingleFlightError | loader 首次返回 `Status::IOError`，二次成功 | 同 key | do_once×N 再单独 do_once | 首轮全员收到错误且未缓存；后续成功 calls 再+1 | 错误路径（不缓存失败）|
| F6 SingleFlightNoMapLockInLoad | witness | 同 key 并发 | do_once | loader 入口 `map_lock_depth()==0` | 锁外执行 loader 不变量 |
| F7 (degenerate) SingleFlightSingleThread | 计数 loader | 同 key 串行 2 次 | do_once 两次（中间结果已 erase）| 第二次重新 load（calls==2，因成功结果不长缓存，仅 in-flight 去重）| 边界：单线程/无并发语义明确 |
| F8 (corrupt) ClassifyUnknownSection | 未注册任何 ref | read_at | 调用 | 返回正确字节且 `section_type` 回退 current_io_ctx（行为不变）；`classify_lock_depth()==0` | 退化输入不破坏不变量 |

> 等价性：F2 断言"加 witness 后"read_batch 的合并物理读次数/字节与改动前逐一相等（对照 `snii_doris_adapter_test.cpp` 既有断言），证明仅加护栏、行为零变化。

# 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 确定性 | 测试文件/target |
|---|---|---|---|---|---|
| reader open 次数（H2 核心）| `SingleFlight` 原语 + 计数 loader（模拟 open_snii_index）+ N 线程 latch | 改前 N 次 open | `loader_calls == 1`（N=8 并发同 key）| 是 | snii_concurrency_test.cpp / doris_be_test |
| 锁内 IO 计数（不变量）| RecordingFileReader 回调内 witness | n/a | IO 时 `classify_lock_depth()==0`（计数=0）| 是 | 同上 |
| 锁外 loader（single-flight）| `map_lock_depth` witness | n/a | loader 期间 `map_lock_depth()==0` | 是 | 同上 |
| read_batch 物理读合并不回归 | `RecordingFileReader::reads()` | 改前次数/offset/len | 加 witness 后逐字段相等 | 是 | snii_doris_adapter_test.cpp |
| （H1，预留）解压次数 | `dict_decode_counter()` + `dict_cache_lock_depth()` | 待 T04 | `== unique_blocks` 且解压入口锁深=0 | 是（DISABLED 占位）| snii_concurrency_test.cpp |
| TSAN 无竞态 | `BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'` | n/a | 无告警 | 工具确定 | doris_be_test(TSAN) |
| 并发 open QPS（report-only）| 微基准（`-DBUILD_BENCHMARK=ON` RELEASE）| 加 single-flight 前 | 不回归（report-only，**非 CI 门禁**）| 否 | benchmark_snii_*.hpp |

wall-clock 仅 report-only：理由是 open 吞吐受机器/IO 抖动影响，确定性的 open-count==1 已直接证明 thundering-herd 被消除，无需把计时入门禁。

# 8. 验收标准（可勾选、可机验）

- `[功能]` F1/F2：`classify_lock_depth()==0` 在每次 IO 回调内成立；read_batch 合并读次数/字节与改前位级一致（`RecordingFileReader::reads()`，`EXPECT_EQ`）。
- `[功能]` F3–F7：SingleFlight 正确去重、不同 key 并行、错误不缓存、单线程语义明确（计数与指针相等断言）。
- `[性能-确定性]` 8 线程并发同 key：`loader_calls == 1`（改前为 8）。
- `[性能-确定性]` loader 期间 `map_lock_depth()==0`；IO 期间 `classify_lock_depth()==0`（NO-IO-UNDER-LOCK）。
- `[并发]` `BUILD_TYPE_UT=TSAN ./run-be-ut.sh --run --filter='Snii*Concurrency*'` 无数据竞争告警。
- `[格式]` 无在盘字节变更（reader/writer-only）；现有 `SniiPhraseQueryTest.*`/`DorisSniiFileReaderTest.*` 全绿，证明零行为回归。
- `[契约]` `lock_witness.h`/`single_flight.h`/`dict_decode_counter()` seam 就位，T04/T07 可直接消费（DISABLED 占位测试可编译）。

# 9. 风险与回滚

- **正确性风险（single-flight follower 句柄计数）**：follower 必须在 loader 完成后**重新 `lookup`** 拿自己的 `InvertedIndexCacheHandle`（句柄引用计数语义，见 `InvertedIndexCacheHandle` :148-191），不能共享 leader 的 handle。设计已规定 loader 只负责 insert，调用方各自 lookup 取 handle。缓解：F3 之外加一条 wiring 层 regression（doris 侧）确认句柄释放无 double-free。
- **死锁风险**：SingleFlight 持 `map_mu_` 期间绝不调 loader、绝不嵌套取 `done_mu`（§3.2 不变量 1/3，F6 验证）。回滚：移除 coordinator 调用、恢复 :329-351 原直连 open（diff 局部，单点回退）。
- **线程安全风险（witness 误用）**：witness 是 thread_local，跨线程不串扰；只读断言，不参与业务逻辑；release 成本可忽略。
- **格式风险**：无（不碰编解码）。
- **上游语义风险**：依赖"`ShardedLRUCache` lookup/insert 分片线程安全"（已读 `lru_cache_policy.h:43-62` 确认）；若上游未来自带 open 去重，本层可降级为透传（保留原语供 T04 复用）。
- **H1 未闭环风险**：本任务只立契约+基建，真实缓存并发断言待 T04（gaps 已声明）；护栏 seam 先行，确保 T04 一落地即可被门禁拦截违例。
- **回滚总策略**：三块改动相互独立（lock_witness、single_flight+wiring、契约 stub），可分别 revert，互不影响在盘格式与现有查询路径。
