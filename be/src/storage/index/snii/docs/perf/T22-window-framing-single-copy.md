# T22 — 窗口 framing/region 单次拷贝（去临时 ByteSink/vector）

## 1. 目标与背景

SNII 的 SPIMI build（flush/compaction）路径在「封帧」(framing) 与「region 落盘」两处存在冗余的临时缓冲 + 双重拷贝，浪费每窗口一次 heap 分配与一次整 payload memcpy。本任务为 **纯写路径重构**，输出字节与 CRC 完全不变。

涉及两个 finding：

- **F32**（`prx_pod.cpp:207-243`，及 `section_framer.cpp:7-16`）：`write_pfor` / `write_raw` / `write_zstd_compressed` / `SectionFramer::write` 各自分配一个临时 `ByteSink framed`，`framed.put_bytes(payload)`（拷贝#1 + 一次 payload 大小的 heap alloc），对 `framed.view()` 算 crc，再 `sink->put_bytes(framed.view())`（拷贝#2）。`bitpack`（`pfor.cpp:63-81`）逐字节 `out->put_u8`（=`buf_.push_back`，`byte_sink.h:14`）追加，无 `reserve`。这些在 `build_prx_window_flat`（`prx_pod.cpp:666-685`）每个 ~256-doc prx 窗口（kDocsPositions/kDocsPositionsScoring tier）被调用。
- **F38**（`frq_pod.cpp:76-93`）：`emit_region` 的 raw 分支（dd/freq 在当前 writer 下**恒为 raw**，`kRawFrqRegion=0`）分配 `std::vector<uint8_t> disk`、`disk.assign(plain.data(), plain.data()+plain.size())`（alloc + memcpy）、`crc32c(Slice(disk))`、`out->put_bytes(Slice(disk))`（再拷一次到调用方 sink）。`build_dd_region`/`build_freq_region`（`frq_pod.cpp:125-151`）每窗口调用一次；高 df term 切成成千上万窗口时累积明显。

**预期收益**：每个 prx 窗口 / 每个 dd/freq region 消除一次临时 heap alloc + 一次整 payload 拷贝；`bitpack` 消除逐字节 push_back 的增长检查 / realloc 抖动。**纯 build 吞吐项，绝不触及查询读延迟**。验证器已将两个 finding 量级修正为 LOW（第二拷贝中「写入调用方流式 sink」那一份是流式设计内禀、本任务**不**消除；只消除临时缓冲的 alloc + 第一份拷贝）。

## 2. 影响的文件/函数（当前签名）

- `be/src/storage/index/snii/core/src/format/prx_pod.cpp`
  - `void write_pfor(Slice payload, ByteSink* sink)`（:207）
  - `void write_zstd_compressed(Slice plain, Slice compressed, ByteSink* sink)`（:235）
  - `void write_raw(Slice plain, ByteSink* sink)`（:592）
- `be/src/storage/index/snii/core/src/format/frq_pod.cpp`
  - `Status emit_region(Slice plain, int level, ByteSink* out, FrqRegionMeta* meta)`（:76）
- `be/src/storage/index/snii/core/src/encoding/section_framer.cpp`
  - `void SectionFramer::write(ByteSink& sink, uint8_t section_type, Slice payload)`（:7）
- `be/src/storage/index/snii/core/src/encoding/pfor.cpp`
  - `void bitpack(const uint32_t* v, size_t n, uint8_t w, ByteSink* out)`（:63）
- `be/src/snii/encoding/byte_sink.h`：新增 `void reserve(size_t additional)`（共享基础设施，本任务提供）。

辅助既有设施（无需改）：`Slice::subslice(off,n)`（`slice.h:29`）、`crc32c(Slice)`/`crc32c_extend`（`crc32c.h`）、`ByteSink::size()`/`view()`（`byte_sink.h:23,25`）。

## 3. 变更设计

### 3.1 framing 单次拷贝（F32 — prx 三处 + SectionFramer）

把 header（codec/type 字节 + varint 长度）与 payload **直接写入目标 `sink`**，记录写入前的起始偏移，待全部写完后取一次 `sink.view()`，对 `[start, written)` 子切片算 crc，再 `put_fixed32(crc)`。无临时 `ByteSink`，payload 只拷一次（即流式 sink 那次）。

`write_pfor` 改写为：
```cpp
void write_pfor(Slice payload, ByteSink* sink) {
    const size_t start = sink->size();
    sink->put_u8(static_cast<uint8_t>(PrxCodec::kPfor));
    sink->put_varint32(static_cast<uint32_t>(payload.size()));
    sink->put_bytes(payload);
    const size_t framed_len = sink->size() - start;
    const uint32_t crc = crc32c(sink->view().subslice(start, framed_len));
    sink->put_fixed32(crc);
}
```
`write_raw`、`write_zstd_compressed`、`SectionFramer::write` 同构改写（注意 SectionFramer 用 `put_varint64` 写长度、`section_type` 字节）。

**关键正确性点（验证器纠正）**：
- crc 的 `view()` 必须在写完 payload **之后、写 crc 之前**取——此刻 `buf_` 连续、无后续插入，`subslice(start, framed_len)` 指向的内存有效，无 realloc/aliasing 隐患。
- crc 覆盖的字节范围 = `[codec/type][varint len][payload]`，与 reader（`read_framed` `prx_pod.cpp:612-638` / `SectionFramer::read`）重新推导的 `slice_from(start, framed_len)` 完全一致，故 CRC 值不变。
- `put_fixed32` 写的是 4 字节小端（`byte_sink.cpp:11-13`），与原 `sink->put_fixed32(crc32c(framed.view()))` 字节一致。

### 3.2 emit_region raw 分支去临时 vector（F38）

raw 分支不再构造 `disk`，直接用 `plain`：
```cpp
Status emit_region(Slice plain, int level, ByteSink* out, FrqRegionMeta* meta) {
    if (out == nullptr || meta == nullptr) return Status::InvalidArgument("frq: null region out");
    meta->uncomp_len = plain.size();
    if (should_compress(level, plain.size())) {
        std::vector<uint8_t> disk;                 // zstd 仍需自有缓冲
        meta->zstd = true;
        SNII_RETURN_IF_ERROR(zstd_compress(plain, level > 0 ? level : kDefaultZstdLevel, &disk));
        meta->disk_len = disk.size();
        meta->crc = crc32c(Slice(disk));
        out->put_bytes(Slice(disk));
        return Status::OK();
    }
    meta->zstd = false;
    meta->disk_len = plain.size();                 // 必须严格 == plain.size()
    meta->crc = crc32c(plain);                     // raw 上盘字节 == plain，故 CRC 不变
    out->put_bytes(plain);
    return Status::OK();
}
```
**正确性点**：reader 侧 `open_region`（`frq_pod.cpp:97-121`）对 raw 区强校验 `uncomp_len == disk_len` 且 `crc32c(disk) == meta.crc`；因 raw 上盘字节逐字节等于 `plain`，`disk_len = plain.size()`、`crc = crc32c(plain)` 保持不变量成立。`plain`（来自 `ByteSink::view()`，连续）满足 `crc32c(Slice)`/`put_bytes(Slice)`。

### 3.3 bitpack 去逐字节 push_back（F32 子项）

`bitpack` 在循环前按精确字节数预留，消除 realloc 抖动（验证器纠正：`reserve` 取**绝对容量 size()+needed**，否则缓冲增长后再 reserve 同值即 no-op，因为 `encode_pfor_runs` 复用同一 `out` 跨多个 run）。两个 run 编码器（`prx_pod.cpp:104` / `frq_pod.cpp:36`）run 长度恒 `<= kFrqBaseUnit=256`，w<=32 → packed `<= (32*256+7)/8 = 1024` 字节，故可用栈缓冲一次 `put_bytes`：
```cpp
void bitpack(const uint32_t* v, size_t n, uint8_t w, ByteSink* out) {
    if (w == 0) return;
    const size_t packed = (static_cast<size_t>(w) * n + 7) / 8;
    out->reserve(packed);                  // 新增 ByteSink::reserve(additional)
    // 仍逐字节填，但已无 realloc；保留原 acc/filled 位拼装逻辑，字节完全一致
    ...
}
```
为兼容未来可能的大 n 直接调用，采用 `reserve(packed)`（通用、简单、必然位级一致）作为主方案；栈缓冲 + 单次 `put_bytes` 作为可选优化（需 `packed <= 栈上限` 的运行时回退）。本计划落地 `reserve` 方案。`ByteSink::reserve`：
```cpp
void reserve(size_t additional) { buf_.reserve(buf_.size() + additional); }
```

### FORMAT-COMPATIBILITY 结论
**reader/writer-only，零在盘变更**。三处 framing、emit_region raw、bitpack 均产出与改前**逐字节相同**的 on-disk 字节与 CRC（§3 各点已逐一论证 crc 覆盖范围/长度不变）。

### CONCURRENCY 结论
**N/A，无共享可变状态**。全部改动位于单线程 build 路径；`ByteSink` 是函数局部对象，`emit_region`/`bitpack` 仅操作入参 `Slice` 与调用方私有 sink。不触及共享 `LogicalIndexReader`、不在任何锁临界区内、不涉及 IO/解压（NO-IO-UNDER-LOCK 红线无关）。

## 4. 依赖

- **depends_on**：无硬依赖。（与 T19 `resize_uninitialized` 正交：本任务处理 append/编码缓冲，非 resize-then-overwrite 解码缓冲，不引入该原语。）
- **本任务提供的共享基础设施**：`ByteSink::reserve(size_t additional)`，供 bitpack 及后续写路径预留容量复用。

## 5. TDD 步骤（RED → GREEN → REFACTOR）

> 新增测试文件 `be/test/storage/index/snii_prx_pod_test.cpp`（`storage/*.cpp` GLOB_RECURSE 自动纳入 `doris_be_test`，无需改 CMake，见 `test/CMakeLists.txt:39`）。套件名沿用 `SniiPrxPodTest`。frq 相关用例放同文件或 `snii_frq_pod_test.cpp`（套件 `SniiFrqPodTest`）。

1. **RED — 黄金字节钉桩（golden）**：在重构**之前**，先写 `BuildPrxWindowFlatProducesByteIdenticalOutput`：对固定输入（`positions={...}, freqs={...}`）调用 `build_prx_window_flat(..., -1, &sink)`，把当前实现产出的 `sink.buffer()` 复制成 `expected_bytes`（首跑用 `EXPECT_EQ` 对照自身即通过；其作用是把当前字节序列固化为黄金）。同样为 `build_dd_region`/`build_freq_region`（raw 分支）、`SectionFramer::write` 各钉一份黄金。
   - 为构造真正的 RED：先把黄金常量写成**独立参考实现算得的期望值**（或先跑一次打印 hex 落为常量），故意在测试里断言「重构后字节 == 黄金常量」。在尚未重构时此测试 GREEN（证明黄金正确）；这是重构基线。
2. **RED — 往返等价**：`PrxWindowRoundTripAfterSingleCopy`：build 后用 `read_prx_window_csr` / `read_prx_window` 读回，`EXPECT_EQ` 解出的 doc/positions == 输入；`DdFreqRegionRoundTrip`：`build_dd_region`→`decode_dd_region`、`build_freq_region`→`decode_freq_region` 全量比对。重构前 GREEN，作为安全网。
3. **GREEN — 改 §3.1 framing**：改写 `write_pfor`/`write_raw`/`write_zstd_compressed`/`SectionFramer::write` 为单次拷贝。跑步骤 1/2 测试，黄金 + 往返必须保持 GREEN（位级不变）。
4. **GREEN — 改 §3.2 emit_region**：raw 分支去 `disk`。跑 dd/freq 黄金 + 往返，保持 GREEN。
5. **GREEN — 改 §3.3 bitpack + 新增 ByteSink::reserve**：加 `reserve`，bitpack 预留。跑全部 prx/frq 黄金（PFOR 路径字节不变），保持 GREEN。
6. **REFACTOR**：抽公共 framing 小工具（可选，如 `frame_into(sink, [&]{ ...header+payload... })` 内联模板）以消除四处重复；再次全跑黄金 + 往返 + 既有 `snii_query_test`（其 `:694/744/776/803` 已通过 `build_prx_window_flat` 实际行使本路径）。运行 `be-code-style`（clang-format）。
7. **回归**：`./run-be-ut.sh --run --filter='SniiPrxPodTest.*:SniiFrqPodTest.*:SniiPhraseQueryTest.*'` 全绿。

## 6. 功能验证

测试 target：`doris_be_test`（文件 `be/test/storage/index/snii_prx_pod_test.cpp` / `snii_frq_pod_test.cpp`，GLOB 自动纳入）。

| 用例ID | 前置/数据 | 输入 | 操作 | 期望断言 | 覆盖点 |
|---|---|---|---|---|---|
| PRX-BYTE-PFOR | 多 doc、freq 混合（含 freq=1 多数）positions_flat+freqs | auto(-1) | `build_prx_window_flat` 后取 `sink.buffer()` | `EXPECT_EQ(bytes, golden_pfor)` 逐字节 | PFOR framing 位级等价（新路==旧路） |
| PRX-BYTE-RAW | 小 payload（< kAutoZstdMinBytes，强 raw） | level=0 | `build_prx_window_flat` | `EXPECT_EQ(bytes, golden_raw)` | write_raw 位级等价 |
| PRX-BYTE-ZSTD | 大 payload（>=512，命中 zstd 分支） | level=3 | `build_prx_window`/`write_zstd` | `EXPECT_EQ(bytes, golden_zstd)` | write_zstd_compressed 位级等价 |
| PRX-RT-CSR | 同 PRX-BYTE-PFOR | — | build→`read_prx_window_csr` | `EXPECT_EQ(pos_flat,pos_off, 期望)` | 往返正确、CRC 校验通过 |
| PRX-EMPTY | 单 doc 空 positions / freqs={0} | auto | build→read | 解出空列表，`src.eof()` 真 | 退化（空/单元素）边界 |
| PRX-CRC-CORRUPT | 取 PRX-BYTE-RAW 输出，翻转 payload 中一字节 | — | `read_prx_window` | 返回 `Status::Corruption("prx: window crc mismatch")` | crc 覆盖范围正确、错误路径 |
| FRQ-BYTE-DD | 升序 docids + win_base | level=0(raw) | `build_dd_region` 取 `out.buffer()` 与 `meta` | `EXPECT_EQ(bytes, golden_dd)` 且 `meta.disk_len==plain.size()`、`meta.crc==golden_crc`、`meta.zstd==false` | emit_region raw 位级 + meta 不变量 |
| FRQ-BYTE-FREQ | freqs 数组 | level=0 | `build_freq_region` | `EXPECT_EQ(bytes, golden_freq)` + meta | freq region raw 等价 |
| FRQ-RT | 同上 | — | build→`decode_dd_region`/`decode_freq_region` | `EXPECT_EQ` 全量比对 docids/freqs | 往返 + open_region 校验 |
| FRQ-ZSTD-PATH | 强制 level=3 的 region（白盒测试间接构造或经 build_*_region level>0） | level=3 | build→open/decode | meta.zstd==true、disk_len==压缩长度、往返正确 | zstd 分支保留正确 |
| FRQ-DOCCOUNT0 | doc_count==0 freq region | — | `decode_freq_region(...,0,...)` | `meta.uncomp_len==0` 校验、freqs 空 | 退化边界 |
| SF-BYTE | 任意 type+payload | — | `SectionFramer::write` | `EXPECT_EQ(bytes, golden_section)` | section framing 位级等价 |
| SF-RT | 同上 | — | write→`SectionFramer::read` | `EXPECT_EQ(out.type/payload, 输入)` | 往返 + crc |
| BITPACK-RT | 各 w（0..32）含异常值的 uint32 run（n=1/255/256） | — | `pfor_encode`→`pfor_decode` | `EXPECT_EQ(decoded, input)` | bitpack reserve 后位级一致、w 边界 |

correctness-equivalence（新路==旧路）由所有 `*-BYTE-*` 黄金用例承担。corrupt-input 由 PRX-CRC-CORRUPT 承担。boundary/degenerate 由 PRX-EMPTY/FRQ-DOCCOUNT0/BITPACK-RT(n 边界) 承担。

## 7. 性能验证（单体）— 确定性优先

| 指标 | 隔离手法 | 基线 | 断言/阈值 | 是否确定性 | 测试文件/target |
|---|---|---|---|---|---|
| on-disk 字节逐字节一致（framing×3 + emit_region + bitpack/PFOR） | 固定输入，比对 `sink.buffer()`/`out.buffer()` 对黄金常量 | 重构前实现产出的黄金字节 | `EXPECT_EQ(bytes, golden)` 全等（证明重构未改字节，可安全上线 alloc/copy 削减） | 是（位级黄金，可进 CI 门禁） | `snii_prx_pod_test.cpp` / `snii_frq_pod_test.cpp` @ doris_be_test |
| meta 不变量（disk_len/crc/zstd） | `build_dd_region`/`build_freq_region` 取 `FrqRegionMeta` | 改前 meta 值 | `EXPECT_EQ(meta.disk_len, plain_size)`、`meta.crc==golden` | 是 | `snii_frq_pod_test.cpp` |
| 往返等价（解码不回归） | build→read 全链 | 输入数据 | `EXPECT_EQ(decoded, input)` | 是 | 同上 |
| 每窗口临时 alloc 次数 / payload 拷贝次数下降 | Google Benchmark 微基准（`benchmark_snii_framing.hpp` + `#include` 进 `benchmark_main.cpp`），可在该 TU 内挂一个本地 operator new 计数器统计 alloc 次数 | 改前每窗口 +1 临时缓冲 alloc + 1 拷贝 | report-only：alloc/拷贝计数与 build 吞吐改善（不设阈值门禁） | 否（report-only，非 CI 门禁；理由见 gaps：匿名内部临时无法在单测无侵入计数，且禁止全局 new override） | `be/benchmark`（-DBUILD_BENCHMARK=ON, RELEASE） |
| bitpack realloc 次数 | 微基准内观测 out 缓冲 reallocation | 改前每 run 多次增长 | report-only | 否（report-only） | 同上 |

**结论**：本节由确定性「位级黄金 + meta 不变量 + 往返等价」断言主导（纯重构任务的合规确定性门禁，规范 §4「位级黄金输出」）；wall-clock / alloc-count 仅 report-only。

## 8. 验收标准

- `[功能]` PRX-BYTE-PFOR/RAW/ZSTD、FRQ-BYTE-DD/FREQ、SF-BYTE 全部 `EXPECT_EQ(bytes, golden)` 通过（验证手段：黄金常量 + `sink.buffer()`）。
- `[功能]` 往返用例（PRX-RT-CSR、FRQ-RT、SF-RT、BITPACK-RT）`EXPECT_EQ` 解码==输入；PRX-CRC-CORRUPT 返回 `Status::Corruption`（验证手段：read_* 返回码）。
- `[功能]` FRQ-BYTE-DD 断言 `meta.disk_len == plain.size() && meta.zstd == false && meta.crc == 改前值`（验证手段：FrqRegionMeta 字段比对）。
- `[性能-确定性]` framing×3 + emit_region(raw) + bitpack/PFOR 的 on-disk 字节改前后**逐字节相等**（验证手段：`*-BYTE-*` 黄金 `EXPECT_EQ`，CI 门禁）。
- `[性能-report-only]` 微基准显示每窗口少 1 次临时 alloc + 1 次 payload 拷贝、bitpack 无 realloc 抖动（验证手段：`benchmark_snii_framing`，非门禁）。
- `[格式]` 无在盘字节变更；reader（`read_framed`/`open_region`/`SectionFramer::read`/`pfor_decode`）零改动且全部往返通过。
- `[并发]` N/A（无共享可变状态）；既有 `SniiPhraseQueryTest.*` 全绿（验证手段：`./run-be-ut.sh --run --filter='Snii*'`）。
- 全量：`./run-be-ut.sh --run --filter='SniiPrxPodTest.*:SniiFrqPodTest.*:SniiPhraseQueryTest.*'` 绿；`be-code-style` 通过。

## 9. 风险与回滚

- **CRC 覆盖范围/取 view 时机风险**（最高优先，验证器重点）：crc 必须在写完 payload 之后、写 fixed32 之前对 `view().subslice(start, framed_len)` 计算；若误在追加 crc 之后取 view 会把 crc 字节也纳入、或在中途 realloc 后持有失效指针。**缓解**：取 view 紧接 payload 写入；所有 `*-BYTE-*` 黄金 + `*-CRC-CORRUPT` 用例直接捕捉此类错误（字节或 CRC 不符即 RED）。
- **emit_region raw 不变量风险**：必须保持 `meta.disk_len == plain.size()`，否则 reader `open_region` 的 `uncomp_len==disk_len` 校验失败。**缓解**：FRQ-BYTE-DD 显式断言；zstd 分支保留独立 `disk` 缓冲不动。
- **ByteSink::reserve 语义风险**：reserve 取绝对容量 `size()+additional`（验证器纠正）；若误写成 `reserve(packed)`（被 std::vector 当绝对容量）在缓冲已增长后会 no-op，仅丧失优化、不致错。位级一致由 BITPACK-RT/PRX-BYTE-PFOR 保证。
- **线程安全/格式风险**：无——单线程 build、零在盘变更、reader 不动。
- **回滚**：四处函数 + emit_region + bitpack 改动彼此独立，可单独 `git revert` 任一处；`ByteSink::reserve` 为纯增量新增，回滚 bitpack 时保留无害。黄金测试在回滚后仍应 GREEN（字节不变），可作为回滚正确性校验。
