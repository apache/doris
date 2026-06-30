# T10 — select_covering_windows 改双指针单调游标

## 1. 目标与背景

`select_covering_windows`（`be/src/storage/index/snii/query/docid_conjunction.cpp:497-514`）对一个 windowed 高频词，要从 N 个窗口里挑出"覆盖当前候选集"的窗口子集。它**对每个升序候选 docid 调用一次** `prelude.locate_window(d)`（`:502/:505`）：

```cpp
for (uint32_t d : candidates) {
    bool found = false; uint32_t w = 0;
    SNII_RETURN_IF_ERROR(prelude.locate_window(d, &found, &w));   // :505
    if (!found) continue;
    if (w != last) { sel.push_back(w); last = w; }                // 仅折叠输出，不省探测
}
```

`locate_window`（`be/src/storage/index/snii/format/frq_prelude.cpp:445-468`）每次做：① 在 super-block 目录 `sb_last_docid_` 上 `std::lower_bound`（`:454`，~log n_super），② 从 `lo = sb*group_size`（`:458`，G 默认 64，`frq_prelude.h:124`）起**重新线性扫描**至多 G 个 `windows_[i].last_docid`（`:460-466`）。由于每次都从 super-block 块首重启扫描，映射到同一组的候选会反复重扫该组 → 整体 **O(C·(log n_super + G))**，且每次触碰 ~104–112B 的 `WindowMeta` 行（`last_docid` 在偏移 0）。候选与窗口**都已升序**，本质是一个被当成 C 次独立查找来执行的有序归并。

**Finding 映射 / 修订后严重度**：
- **F05 [MEDIUM]**（cross-cutting-systems，`需改格式: False`，`needs-nuance`）：算法低效真实存在，但验证器把"每窗一次 cache-miss"的表述下调——一个 super-block 组 = 64×~104B ≈ 6.6KB，**L1 常驻**，重扫主要烧 CPU 比较而非 cache-miss。被验证器从 high/medium 下调为 **MEDIUM**。
- **F42 [LOW]**（query-postings-ops，同位置，同类问题，重复指出"应为 two-pointer 归并"）：验证器评 **LOW**，因 `should_scan_all_windows`（`docid_conjunction.cpp:516-524`）已把候选数 C 上界钳到 `window_count*64`，最坏 ~10⁵ 次整数比较（数十 µs），且这是纯 CPU 选择步，其后的 **PFOR 解码 + （可能远程的）窗口字节抓取才是主导**（`collect_windowed_docids_only:650-675`）。

**真实热点形状（必须如实反映）**：仅在"多个 windowed 高频词（df≥512）+ 中等密度候选"的 phrase / MATCH_ALL 形状才显著——conjunction 按 `ascending_df_order` 处理，第一个词（k==0）走 `all_windows` 分支（`:685`），**只有非首位的 windowed 词**且候选数落在 `should_scan_all_windows` 阈值**之下**的中等密度带，才走 `select_covering_windows`（`:691`）。

**预期收益与口径**：把窗口定位从 O(C·(log n_super + G)) 降为 **O(C + N)** 的单调双指针，每次只比较 4B 的 `last_docid`（可选 packed 数组）。**收益主要是窗口比较次数（query CPU）的下降，不是 wall-clock 主导项**（解码/IO 主导）。因此本任务的**确定性单体证据**为两条不变量：
1. `probe_count`（窗口 `last_docid` 比较次数）从随 G 增长的 O(C·G) 降为 **`probe_count ≤ C + N`** 且与 G 无关；
2. 选出的覆盖窗口集合相对旧 `locate_window`-per-candidate 实现**逐元素相等（同序、同去重）**。

packed `win_last_docid_` 的 cache-locality 收益**无法确定性证明**（cache-miss 下降不可机验），其单测只能验证**位级等价 + 不改变窗口集合**，wall-clock 仅作 report-only 微基准旁证，**不作 CI 门禁**。

## 2. 影响的文件/函数

**头文件** `be/src/storage/index/snii/format/frq_prelude.h`
- 现有：`Status FrqPreludeReader::locate_window(uint32_t docid, bool* found, uint32_t* w) const;`（`:163`，返回"首个 `last_docid ≥ docid` 的窗口"；`docid > windows_.back().last_docid` 时 `*found=false`）。**保留**（公开 API + 等价性测试 oracle）。
- 现有：`Status FrqPreludeReader::window(uint32_t w, WindowMeta* out) const;`（`:157`，整结构体拷贝；不动）。
- 现有：`uint32_t n_windows() const`（`:144`）、`std::vector<uint64_t> sb_last_docid_;`（`:173`）、`std::vector<WindowMeta> windows_;`（`:175`，`open()` 后不可变）、`uint32_t group_size_`（`:168`）、`uint32_t n_super_`（`:169`）。
- 新增（本任务）：packed `std::vector<uint32_t> win_last_docid_;`（private，in-memory only）；inline 访问器 `uint32_t window_last_docid(uint32_t) const`；const 成员 `void select_covering_windows(const std::vector<uint32_t>&, std::vector<uint32_t>*) const`；自由函数 `select_covering_windows_cursor(...)`（隔离可测核心）；`namespace doris::snii::format::testing { uint64_t window_probe_count(); void reset_window_probe_count(); }`。

**实现** `be/src/storage/index/snii/format/frq_prelude.cpp`
- `FrqPreludeReader::open`（`:404-434`）：在 `decode_all_blocks`（`:432`）填好 `windows_` 后，紧随 `sb_last_docid_` 的构建（`:429-431`）**并行构建 `win_last_docid_`**。
- `FrqPreludeReader::locate_window`（`:445-468`）：仅在 level-2 扫描循环（`:460-466`）插入 `++g_window_probes` 计数 seam（行为不变，保留为 oracle）。

**调用方** `be/src/storage/index/snii/query/docid_conjunction.cpp`
- `Status select_covering_windows(const FrqPreludeReader& prelude, const std::vector<uint32_t>& candidates, std::vector<uint32_t>* windows)`（`:497-514`，匿名命名空间）：**删除**，逻辑迁入 `FrqPreludeReader`。
- `collect_docids_only`（`:679`）在 `:691` 处的调用 `SNII_RETURN_IF_ERROR(select_covering_windows(p.prelude, *candidates, &windows));` 改为 `p.prelude.select_covering_windows(*candidates, &windows);`（成员纯内存、不可失败，去掉 `SNII_RETURN_IF_ERROR`）。
- 下游 `collect_windowed_docids_only`（`:620-677`）消费 `windows`（`:629` 起以 `p.prelude.window(w,&meta)` 顺序遍历 + `find_candidate_range` 的单调 `candidate_search_begin`），**要求 windows 升序**——游标天然保证，契约不变。

**数据结构** `WindowMeta`（`frq_prelude.h:86-115`）：~104–112B；`last_docid`（`uint32_t`，偏移 0）是窗口定位**唯一**需要的字段；packed `win_last_docid_` 即把这 4B 抽出连续存放，复用 `sb_last_docid_` 的设计思路。**`WindowMeta` 布局不变。**

## 3. 变更设计

### 3.1 单调双指针游标（替代 per-candidate `locate_window`）

候选升序 + 窗口 `last_docid` 非降（构建期不变量见 §3.4），窗口在 docid 空间上**连续不重叠地分区**（窗口 w 覆盖 `(win_base(w), last_docid(w)]`，`win_base(w)=last_docid(w-1)`）。因此一个**只前进**的窗口游标即可复现 `locate_window` 的"首个 `last_docid ≥ d` 的窗口"。为同时兼顾**稀疏候选/超多窗口**的情形（纯线性游标在 C≪N 时退化为 O(N)，可能劣于旧的二分），保留 super-block 级单调游标做**边界跳跃**，得到严格 O(C + N)、且不劣于旧实现：

```cpp
// frq_prelude.cpp —— 隔离可测的纯函数核心（对数组操作，无 FrqPreludeReader/IO）
void select_covering_windows_cursor(const uint32_t* win_last_docid, uint32_t n_windows,
                                    const uint64_t* sb_last_docid, uint32_t n_super,
                                    uint32_t group_size,
                                    const std::vector<uint32_t>& candidates,
                                    std::vector<uint32_t>* windows) {
    windows->clear();
    if (n_windows == 0) return;                  // empty-windows guard (locate_window:450)
    uint32_t sb = 0;                             // monotonic super-block cursor
    uint32_t w = 0;                              // monotonic window cursor
    uint32_t last_emitted = UINT32_MAX;
    for (uint32_t d : candidates) {
        // Level-1: first super-block whose absolute last_docid >= d (monotone, total <= n_super).
        while (sb < n_super && static_cast<uint64_t>(d) > sb_last_docid[sb]) ++sb;
        if (sb == n_super) break;                // d past term's last docid -> all remaining miss
        // Boundary jump: never scan windows below the current super-block's first window.
        if (w < sb * group_size) w = sb * group_size;
        // Level-2: first window with last_docid >= d (monotone forward, total advances <= N).
        while (w < n_windows) {
            ++g_window_probes;                   // op-count seam (window last_docid comparison)
            if (d <= win_last_docid[w]) break;
            ++w;
        }
        if (w == n_windows) break;               // defensive; invariants guarantee a hit here
        if (w != last_emitted) { windows->push_back(w); last_emitted = w; }
    }
}
```

**复杂度**：窗口前进 `++w` 总数 ≤ N（游标全程 0→n 单向），每候选至多 1 次"命中比较"（≤ C），故 **窗口比较 `g_window_probes` ≤ C + N**；super-block 前进 ≤ n_super、每候选 1 次停查 ≤ C，均为低阶项。**与 G 无关**（旧实现的 O(C·G) 重扫被消除）。

**边界跳跃为何正确**：`sb` 是首个 `sb_last_docid[sb] ≥ d` 的 super-block，则所有 `< sb` 的 super-block 内窗口 `last_docid ≤ sb_last_docid[sb-1] < d`，对当前及更大候选都不可能覆盖；把 `w` 跳到 `sb*group_size` 只跳过这些不可能命中的窗口，不漏不重。

### 3.2 与旧实现逐元素等价的论证（不变量驱动）

- `locate_window(d)` 返回"首个 `last_docid ≥ d` 的窗口 w"（`frq_prelude.cpp:461`）；游标 level-2 在 `d ≤ win_last_docid[w]` 处停（`++w` 仅越过 `last_docid < d` 的窗口）→ 停位 == `locate_window(d)`。
- **miss 处理**：`locate_window` 在 `docid > windows_.back().last_docid` 时 `*found=false`（`:451`）；游标在 `sb==n_super`（即 `d > sb_last_docid.back() == windows_.back().last_docid`）或 `w==n_windows` 时 `break`，对当前及后续候选**均不再 emit**——与旧 `if(!found) continue` 的输出**逐元素相等**（验证器明确："break early -- matching locate_window line 451"）。
- **去重/顺序**：`if (w != last_emitted)` 与旧 `if (w != last)` 一致；游标单调 → 升序。
- 即便出现 `last_docid` 相等的退化窗口（实际不会：`first_docid_in_window` 在 `first > last_docid` 时报 Corruption，`docid_conjunction.cpp:109`，故每窗 ≥1 doc、`last_docid` 严格递增），游标停在"首个 `last_docid ≥ d`"仍与 `locate_window` 的 `<=` 语义一致。

### 3.3 packed `win_last_docid_`（可选 cache-locality polish）+ 成员桥接

在 `open()` 内随 `sb_last_docid_` 一并构建（in-memory only，**零在盘变更**）：

```cpp
// frq_prelude.cpp FrqPreludeReader::open，紧随 :429-432 sb_last_docid_ 构建之后
out->win_last_docid_.clear();
out->win_last_docid_.reserve(out->windows_.size());
for (const WindowMeta& m : out->windows_) out->win_last_docid_.push_back(m.last_docid);
```

```cpp
// frq_prelude.h —— inline 零拷贝标量访问器（DCHECK 边界，release 零开销）
uint32_t window_last_docid(uint32_t w) const { DCHECK_LT(w, win_last_docid_.size()); return win_last_docid_[w]; }

// 成员桥接：把私有目录数组喂给纯函数核心
void FrqPreludeReader::select_covering_windows(const std::vector<uint32_t>& candidates,
                                               std::vector<uint32_t>* windows) const {
    select_covering_windows_cursor(win_last_docid_.data(),
                                   static_cast<uint32_t>(win_last_docid_.size()),
                                   sb_last_docid_.data(),
                                   static_cast<uint32_t>(sb_last_docid_.size()),
                                   group_size_, candidates, windows);
}
```

游标扫描只触碰 4B/窗口的连续 `win_last_docid_`，不再触碰 ~104–112B 的 `WindowMeta`。验证器口径：这是**次要 polish**（组 L1 常驻，cache 收益小，dominant win 来自游标消除重扫）。即便不加 packed 数组，游标改用 `windows_[w].last_docid`（偏移 0）也能拿到绝大部分收益；本设计采用 packed 数组为更干净的扫描局部性，且其正确性由"位级等于 `windows_[w].last_docid`"覆盖（§6 FV-10）。

### 3.4 计数 seam（确定性性能断言）

```cpp
// frq_prelude.cpp 匿名命名空间
namespace { uint64_t g_window_probes = 0; }
namespace testing {
uint64_t window_probe_count() { return g_window_probes; }     // 唯一读出点
void reset_window_probe_count() { g_window_probes = 0; }      // 测试间 reset
} // namespace testing
```

唯一计数语义 = **窗口 `last_docid` 比较次数**，在**两条路径的同一比较点**自增：游标 level-2 的 `++g_window_probes`（§3.1）与 `locate_window` level-2 扫描（`frq_prelude.cpp:460-466`）的循环体首。如此可 apples-to-apples 对比：旧路径随 G 增长（dense 时 ≈ C·扫描深度），新游标 ≤ C + N 且与 G 无关。

### 3.5 FORMAT-COMPATIBILITY（格式影响）

**reader-only，零在盘字节变更**（非 T18）。`win_last_docid_` 纯 in-memory，于 `open()` 由已解码的 `windows_` 派生，不进入序列化/反序列化；`WindowMeta`/prelude 在盘布局、CRC、版本号均不变。已写盘索引无需重写、完全兼容。

### 3.6 CONCURRENCY（并发与锁影响）

**无新增共享可变状态，无锁、无锁内 IO 风险。** `win_last_docid_` 在 `open()` 中完全物化、之后不可变（与 `windows_`/`sb_last_docid_` 同生命周期，`frq_prelude.h:175` 契约）；`FrqPreludeReader` 随共享 `LogicalIndexReader` 被并发查询共享，但游标用到的 `sb`/`w`/`last_emitted` 均为 `select_covering_windows_cursor` 的**栈局部变量**，目录数组为只读 const 引用。符合规范 §5"共享 `LogicalIndexReader` 现为 const 无锁只读"。`g_window_probes` 为 **test-only** 计数（生产路径不读、测试单线程 reset/读），不引入并发语义。**无需 TSAN 门禁。**

## 4. 依赖

- `depends_on`：**无硬依赖**。本任务自包含（`frq_prelude.{h,cpp}` + `docid_conjunction.cpp` 调用点改一行）。
- **T20（FrqPreludeReader WindowMeta 零拷贝引用访问器 `window_at`）**：互补且独立。T20 给"读整 `WindowMeta`"的循环去拷贝；T10 给"窗口定位"加 4B 标量 packed 数组 + 游标。二者命名/职责不冲突，可任意先后合入；若 T20 已落，`collect_windowed_docids_only` 仍按各自访问器走，互不影响。
- **T23（prelude 按 super-block 惰性解码）**：**软关系，非硬依赖**。T23 若让 `windows_` 惰性化，则 T10 在 `open()` 中**急切**构建全量 `win_last_docid_` 会与惰性目标相抵。集成顺序上二选一：(a) 合入 T23 后令 `win_last_docid_` 同样按 super-block 惰性填充；或 (b) 游标降级为读已（惰性）解码的 `windows_[w].last_docid`，放弃 packed polish。两任务分属不同 batch（T10∈b2，T23∈b5），当前无强耦合，集成时按上述策略消解即可。
- **提供（shared-infra）**：`doris::snii::format::testing::window_probe_count()` 作为"窗口定位算法"的确定性 op-count 样板；`select_covering_windows_cursor(...)` 作为可被其他归并式选择复用的纯函数核心。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

纪律：先写失败测试跑出 FAIL，再最小实现转 GREEN，再重构；改实现不改测试；每批自闭环（业务代码 + UT + 断言同批）。

**Step 0（基线快照，GREEN 起点）**：在新建 `be/test/storage/index/snii_covering_windows_test.cpp`（GLOB 自动纳入 `doris_be_test`，无需改 CMake）写 oracle 参考 `oracle_select(win_last_docid, candidates)`：对每个候选线性求"首个 `last_docid ≥ d`"、按 `w != last` 去重——这是 `locate_window`-per-candidate 语义的可信复刻。再用既有 `SniiPhraseQueryTest` 跑 `phrase_query({"failed","order"})`（`build_reader`，`snii_query_test.cpp:203-279`）记录 golden 结果集（改动前 GREEN）。

**Step 1（RED — 等价性失败保护）**：写 `EquivalenceMatchesOracleOnRandomAscendingSets`：固定 RNG 生成多组（N、G、严格递增 `win_last_docid`、派生 `sb_last_docid`、升序候选），断言 `select_covering_windows_cursor(...) == oracle_select(...)`（`EXPECT_EQ` 全量）。函数尚未实现 → 编译/链接失败 → RED。

**Step 2（GREEN — 实现游标核心）**：实现 §3.1 `select_covering_windows_cursor` + §3.4 计数 seam（先不接 `FrqPreludeReader`）。Step1 转 GREEN。

**Step 3（RED — 探测复杂度不变量）**：写 `CursorProbeCountStaysLinearInCandidatesPlusWindows`：`reset_window_probe_count()` → 游标 → `EXPECT_LE(window_probe_count(), candidates.size() + n_windows)`，覆盖 dense（C≈64·W）与 sparse（C≪N）两形状。当前若误用某非游标实现会超界；用此锁定 O(C+N)。再写 `LegacyLocateWindowProbeGrowsWithGroupSize`：用真实 `make_test_prelude(last_docids, G=8/64)`（经 `build_frq_prelude`+`open`），对同一窗口集分别跑 per-candidate `locate_window` 计数，断言 `probes_old(G=64) > probes_old(G=8)` 且 `> C+N`；而游标在 G=8/64 上 `window_probe_count()` **相等且 ≤ C+N**。（旧 `locate_window` 尚未插 seam → RED。）

**Step 4（GREEN — 接通 reader 成员 + 计数 seam + 改调用点）**：在 `open()` 构建 `win_last_docid_`（§3.3）；加 `window_last_docid` 访问器、`FrqPreludeReader::select_covering_windows` 成员；在 `locate_window` level-2 循环插 `++g_window_probes`；删除 `docid_conjunction.cpp:497-514` 匿名函数、把 `:691` 改为成员调用。Step3 转 GREEN；Step0 的 `phrase_query` golden 结果集逐元素不变。

**Step 5（REFACTOR）**：清理；跑 `be-code-style`。复跑既有 `SniiPhraseQueryTest.*`、`SniiSegmentReaderTest.*`、`SniiTermQueryTest.*` 全 GREEN，证明窗口读路径无回归。`locate_window` 保留为公开 API + 测试 oracle（公开方法，无 `-Wunused`）。

## 6. 功能验证

gtest target：`doris_be_test`（GLOB 自动纳入 `be/test/storage/index/snii_*_test.cpp`）。隔离用例落 `snii_covering_windows_test.cpp`（套件 `SniiCoveringWindowsTest`）；wired 端到端复用 `snii_query_test.cpp` 的 `build_reader()`/`SniiPhraseQueryTest`。结果集断言一律 `EXPECT_EQ` 全量。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| FV-01 EquivalenceRandomAscending | 固定 RNG，多组：N∈{1,2,8,64,200,4000}，G∈{1,8,64}，严格递增 `win_last_docid`，派生 `sb_last_docid`，升序候选（含重复映射到同窗） | arrays+candidates | `cursor` vs `oracle_select` | 两者 `windows` 向量逐元素 `EXPECT_EQ`（同序、同去重） | 新游标 == 旧 locate 语义（随机化） |
| FV-02 EquivalenceVsRealLocateWindow | `make_test_prelude(last_docids,G=64)` 真实 prelude | candidates | `prelude.select_covering_windows` vs per-candidate `locate_window`+去重 | 结果向量 `EXPECT_EQ` | 与生产 `locate_window` oracle 对齐（真实 reader） |
| FV-03 SingleWindow | N=1，`win_last_docid={100}` | 候选含 `<100`、`==100`、`>100` | cursor | `<=100` 的候选 → `{0}`；全 `>100` → `{}` | 边界：单窗口 |
| FV-04 CandidateBeforeFirst | N≥2 | 候选含 0 与 `win_last_docid[0]` | cursor | emit `{0}`（不漏首窗） | 边界：候选在首窗内/起点 |
| FV-05 CandidateAfterLast | N≥2 | 候选含 `> win_last_docid.back()` 的尾部多个 | cursor | 这些候选不 emit；游标 `break` 后结果与"忽略尾部候选"相同 | 边界：候选超末窗（miss 提前终止） |
| FV-06 EmptyCandidates | N≥1，candidates 为空 | {} | cursor | `windows` 为空 | 边界：空候选 |
| FV-07 EmptyWindows | N=0（`win_last_docid` 空） | 任意候选 | cursor | `windows` 为空（不崩溃，命中 empty-guard） | 边界：零窗口（locate_window:450 对齐） |
| FV-08 SuperBlockBoundaryCrossing | N=10，G=4（3 个 super-block），候选跨多个 super-block 边界且稀疏 | candidates | `cursor` vs `oracle_select` 且 vs 真实 `locate_window` | 三者 `EXPECT_EQ`；游标做了 super-block 跳跃仍正确 | super-block 边界跨越 + 跳跃正确性 |
| FV-09 DenseEveryWindowCovered | 候选稠密到每窗都被命中 | candidates | cursor | `windows == {0,1,...,N-1}` | 全覆盖去重正确 |
| FV-10 PackedArrayBitIdentity | 真实 prelude（`build_reader` 的 "failed"/"order" windowed term，或 `make_test_prelude`） | — | 遍历 w 比较 `window_last_docid(w)` 与 `window(w).last_docid` | 对所有 w：`window_last_docid(w) == window(w).last_docid`（`EXPECT_EQ`） | packed 数组位级等同 `WindowMeta.last_docid` |
| FV-11 WiredPhraseUnchanged | `build_reader(include_phrase_bigrams=false)`，9000 docs | `phrase_query({"failed","order"})` | 端到端查询 | 结果集 == Step0 golden（如 `{5000,7000,8000}`），改前后逐元素 `EXPECT_EQ` | 接入 Doris 的查询路径无回归 |
| FV-12 WiredMatchAllUnchanged | `build_reader`，多 windowed 词 MATCH_ALL | 多词 conjunction（非首位词走 `select_covering_windows`） | 端到端 | 结果集改前后 `EXPECT_EQ` | 走 `:691` 分支的真实形状 |

补充：FV-02/FV-08/FV-10 的 `make_test_prelude(last_docids, G)` 经 `build_frq_prelude(FrqPreludeColumns)`+`FrqPreludeReader::open` 构造，`WindowMeta` 用 `doc_count=1`、`last_docid` 严格递增（每窗 ≥1 doc，满足 `frq_prelude.cpp:60-66` 宽度校验）、`dd_off` 累加 + `dd_disk_len=1`（满足 `validate_region_layout` 链式校验）。

## 7. 性能验证（单体）— 确定性优先

target：`doris_be_test`，套件 `SniiCoveringWindowsPerfTest`（确定性断言，可进 CI 门禁）。

| 指标 | 隔离手法 | 基线（改前） | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| 新游标窗口探测次数（O(C+N) 不变量） | `reset_window_probe_count()` → `select_covering_windows_cursor` → `window_probe_count()`；dense（C≈64·W）与 sparse（C≪N）两形状 | 旧 per-candidate ≈ O(C·G) | `EXPECT_LE(window_probe_count(), C + N)` | 是 | `SniiCoveringWindowsPerfTest.CursorProbeCountStaysLinearInCandidatesPlusWindows` |
| 旧路径随 G 增长、新路径与 G 无关 | 同一窗口集建 `make_test_prelude` G=8/64；旧=per-candidate `locate_window` 计数，新=游标计数；各自 `reset`/读 seam | — | `probes_old(G=64) > probes_old(G=8)` 且 `> C+N`；`probes_new(G=64) == probes_new(G=8) ≤ C+N`（**复杂度从 O(C·G) 降为 O(C+N)** 的直接证据） | 是 | `SniiCoveringWindowsPerfTest.LegacyLocateWindowProbeGrowsWithGroupSize` |
| 选出窗口集合位级一致（防优化跑偏） | 同输入下 `cursor` vs `oracle_select` / 真实 `locate_window` | 旧实现窗口集合 | `windows` 向量逐元素 `EXPECT_EQ`（FV-01/02/08 复用为门禁） | 是 | `SniiCoveringWindowsPerfTest.SelectedWindowSetMatchesLegacy` |
| packed 数组位级等同（cache polish 可验部分） | 遍历比较 `window_last_docid(w)` vs `window(w).last_docid`（FV-10） | `WindowMeta.last_docid` | 全 w `EXPECT_EQ`；不改变窗口集合（由上一行保证） | 是（仅位级；cache-miss 下降**不可机验**） | 复用 FV-10 |
| 窗口选择 wall-clock（report-only，非门禁） | Google Benchmark `benchmark_snii_covering_windows.hpp` + `#include` 进 `benchmark_main.cpp`，`-DBUILD_BENCHMARK=ON` 且 RELEASE | 旧 per-candidate `locate_window` | 仅报告 dense/medium 形状下 µs 下降 | 否（report-only） | `benchmark_test` |

确定性断言占主导（probe_count ≤ C+N 不变量 + 旧随 G 增长/新与 G 无关 + 窗口集合逐元素一致 + packed 位级等同）。wall-clock 仅 report-only：理由——验证器明确真实热点窄（仅多 windowed 高频词 + 中等密度候选的 phrase/MATCH_ALL），且 `should_scan_all_windows` 已钳 C≤64·W，绝对成本数十 µs，其后的 PFOR 解码 +（可能远程）字节抓取主导 wall-clock，故不可作门禁。

## 8. 验收标准

- `[功能]` FV-01..FV-12 全 GREEN：`./run-be-ut.sh --run --filter='SniiCoveringWindowsTest.*:SniiCoveringWindowsPerfTest.*:SniiPhraseQueryTest.*'`。重点：FV-01/FV-02/FV-08 `cursor`/成员 与 oracle/真实 `locate_window` 窗口集合逐元素 `EXPECT_EQ`；FV-06/FV-07 空候选/零窗口不崩溃且为空；FV-11/FV-12 wired `phrase_query`/MATCH_ALL 结果集改前后 `EXPECT_EQ`（如 `phrase_query({"failed","order"})` == `{5000,7000,8000}`）。
- `[性能-确定性]` `CursorProbeCountStaysLinearInCandidatesPlusWindows`：`window_probe_count() ≤ C + N`（改前 per-candidate 远超）。`LegacyLocateWindowProbeGrowsWithGroupSize`：`probes_old(G=64) > probes_old(G=8) > C+N` 且 `probes_new(G=64)==probes_new(G=8) ≤ C+N`。`SelectedWindowSetMatchesLegacy`：窗口集合逐元素一致。FV-10：`window_last_docid(w)==window(w).last_docid` 全 w 相等。
- `[格式]` 零在盘变更：`win_last_docid_` 纯 in-memory；prelude 序列化/CRC/版本未改；既有 `SniiSegmentReaderTest.*` round-trip GREEN 即证。
- `[并发]` N/A（无新增共享可变状态；`win_last_docid_` 于 `open()` 物化后不可变，游标用栈局部）；不触碰共享 reader const 路径，CONCURRENCY.md H1/H2 不受影响，无需 TSAN run。
- 提交前 `be-code-style` 通过。

## 9. 风险与回滚

- **稀疏候选退化（纯线性游标劣于旧二分）**：已用 super-block 级单调游标 + 边界跳跃（§3.1）规避，保证 O(C+N) 且不劣于旧 O(C·log)；FV-05/FV-08 + sparse 形状的 probe 断言守护。
- **miss 提前 `break` 漏窗**：候选升序 + 窗口非降保证一旦 `d > windows_.back().last_docid` 后续候选必 miss；FV-05（尾部超末窗）专测，且与 `locate_window:451` 语义对齐。
- **去重/顺序漂移导致下游错乱**：`collect_windowed_docids_only` 的单调 `candidate_search_begin`（`:636`）依赖 windows 升序——游标单调天然满足；FV-01/FV-02 全量窗口向量 `EXPECT_EQ` 是唯一可靠护栏。
- **packed 数组与 `WindowMeta.last_docid` 不一致**：构建点唯一（`open()` 内从 `windows_` 派生），FV-10 位级等同断言守护；若不放心可降级为游标直接读 `windows_[w].last_docid`（放弃 packed polish，收益略减、零正确性风险）。
- **构建期不变量被破坏**：游标正确性依赖窗口 `last_docid` 非降——构建期已由 `validate_input`（`frq_prelude.cpp:80-84`，"last_docid not monotonic" 报错）强制，reader 侧 `decode_all_blocks` 经非负 delta 累加亦保证非降；候选升序由 running conjunction（`intersect_sorted` 等）保证。三者任一被破坏，FV-01/FV-02 等价断言立即失配。
- **T23 集成冲突（急切 packed vs 惰性解码）**：见 §4，软关系，集成时令 `win_last_docid_` 同步惰性化或游标读惰性 `windows_`；当前 batch 无耦合。
- **回滚**：改动集中于 `frq_prelude.{h,cpp}` + `docid_conjunction.cpp` 一处调用点。`git revert` 即恢复匿名 `select_covering_windows` 与 per-candidate `locate_window` 调用；`win_last_docid_`/计数 seam 为新增 in-memory/test-only 设施，移除不影响生产；解码侧与在盘格式从未改动，已写盘索引完全兼容。
