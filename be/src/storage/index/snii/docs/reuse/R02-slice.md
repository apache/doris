# R02-slice — Slice 只读字节视图

## R02-slice 决策文档：doris::snii::Slice 只读字节视图

### 结论与依据
**Verdict: keep-snii-doris-suboptimal（保留 doris::snii::Slice，仅在 IO 边界使用 doris::Slice）。**

doris::snii::Slice（`be/src/storage/index/snii/common/slice.h:12-37`）是一个 40 行、零依赖的「只读」字节视图：`const uint8_t* data_ + size_t size_`，仅暴露 `data()/size()/empty()/operator[]/subslice()`，并对 `operator[]`、`subslice` 做 `assert` 边界检查（slice.h:25,30）。它是整个 SNII core 解码路径的基础抽象：byte_source/byte_sink/crc32c/section_framer/zstd_codec 以及全部 format 解码器（frq_pod、prx_pod、dict_block、per_index_meta、tail_meta_region 等）都以它承载待解码字节。该类型已完全 CLucene-free。

### Doris 等价物
唯一候选是 `be/src/util/slice.h` 的 `doris::Slice`（struct，`char* data + size_t size`，line 48-283），附带 `doris::OwnedSlice`（line 329-375）。它是一个「可变」的通用 slice。

### 是否最优（doris::Slice 次优，具体证据）
1. **可变 vs 只读语义冲突**：doris::Slice 暴露 `mutable_data()`(util/slice.h:93)、`remove_prefix/remove_suffix/clear/truncate/relocate`(line 108-251)，relocate 还会 memcpy；把它放进解码层会让只读路径具备改写底层指针/长度的能力，破坏 const 正确性。doris::snii::Slice 在类型层面就禁止写。
2. **char* vs uint8_t 字节语义**：doris::Slice 以 `char*` 存储，构造自 uint8_t 时 `const_cast`+`reinterpret_cast`(util/slice.h:66-67)，`get_data()` 返回 `char*`。SNII 的 varint/pfor/crc32c 依赖 uint8_t 无符号语义；改用 char* 需在每处 `data()/operator[]` reinterpret_cast，且本平台 char 有符号，移位与比较存在符号扩展隐患。
3. **依赖更重**：util/slice.h 引入 `core/allocator.h`、`faststring`、`OwnedSlice`(line 31-32,329)，远超纯视图所需。
4. **缺少一步式 bounds-checked subslice**：doris::snii::Slice::subslice(off,n) 一次 `assert(off+n<=size)`(slice.h:29)，doris 需 remove_prefix+truncate 两步组合且无该断言。

stdlib 的 `std::span<const uint8_t>`（C++20，`be/CMakeLists.txt:350` 已 `CMAKE_CXX_STANDARD 20`，snii 内已有 13 处 std::span 使用，如 format/prx_pod.h、frq_pod.h、query/docid_sink.h）在语义上才是最优形态：只读、const、uint8_t、有 subspan/operator[]/data/size/empty。但它属于 stdlib 标准化，而非本工作流的「reuse-Doris」目标；且 span 缺少 doris::snii::Slice 的 `string_view`/`vector<uint8_t>` 便利构造（slice.h:16-18），subspan 越界在 release 下是 UB（与 assert 在 release 被编译掉等价）。

### 字节兼容性结论
不适用（touches_on_disk=false）。Slice 只是「指针+长度」视图，不定义任何编码；无论保留 doris::snii::Slice、换 doris::Slice 还是换 std::span，被解码的 on-disk 字节不变，解码结果二进制一致。

### 迁移设计
- **本期（推荐）**：零改动保留 doris::snii::Slice。doris::Slice 继续只出现在 SNII 与 Doris IO 的交界（当前 io 层 read_at 以 `std::vector<uint8_t>* out` 出参、file_writer/local_file/s3_object_store 的 `append(Slice)` 用 doris::snii::Slice，core 内当前 0 处 doris::Slice 引用，边界清晰）。
- **可选未来路径（drop-for-stdlib，effort L）**：将 doris::snii::Slice 全量替换为 `std::span<const uint8_t>`。改动点：(1) 删除 common/slice.h，新增一个 `doris::snii::byte_view = std::span<const uint8_t>` 别名与两个 helper（`from(string_view)` 需 reinterpret_cast、`from(vector<uint8_t>)` 直接构造）以覆盖原便利构造；(2) `subslice(off,n)` → `subspan(off,n)`；(3) 约 277 处、约 50 个文件机械替换。风险/回滚：纯类型替换，编译期可全量验证；以 git 分支隔离，回滚即 revert。**鉴于收益有限（仅省一个 40 行无依赖头）而迁移面大，本期不执行。**

### 为何 KEEP（Doris 次优的明确理由）
唯一的 Doris 等价物 doris::Slice 是「可变 char* 通用 slice」，对 SNII 的「只读 uint8_t 解码」用途在语义、字节类型、依赖、边界检查四方面均次优（见上）；强行复用需在解码热路径遍布 reinterpret_cast 并丢失 const，反而劣化。stdlib 的 span 更优但非 Doris 复用目标且迁移不经济。故保留 doris::snii::Slice，doris::Slice 限定在 IO 边界。

---

## TDD

n/a（保留现状，无迁移）。

补充（仅当未来选择 drop-for-stdlib 迁移到 std::span<const uint8_t> 时启用，target: `doris_be_test`，目录 be/test/storage/index/）：
1. RED——先写等价性测试 `SniiSliceMigrationTest.DecodeResultsIdentical`：用同一份 on-disk 字节，分别经旧 doris::snii::Slice 路径与新 std::span 路径调用 dict_block / frq_pod / per_index_meta 的 open()/decode()，断言解出的 docids/freqs/positions 与各 reader 字段逐字段相等（确定性断言，非随机）。
2. RED——`Sfrom_string_view/from_vector` helper 单测：构造越界 subspan/subslice 的负路径在 debug 下触发断言、size()/data() 与原 Slice 行为一致。
3. GREEN——执行机械替换后跑全套既有 snii 解码用例（snii_query_test、snii_doris_adapter_test）必须全绿。
4. 黄金测试——非 on-disk 组件无需字节黄金测试；但保留一条「同字节缓冲 → 两实现 crc32c(Slice/span) 输出 uint32 相等」的确定性断言，证明视图替换不改变任何下游字节级计算。
5. REFACTOR——删除 common/slice.h 后确认无悬挂 include。