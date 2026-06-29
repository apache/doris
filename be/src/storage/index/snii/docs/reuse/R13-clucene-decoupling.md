# R13-clucene-decoupling — CLucene 解耦核查（集成层）

## R13 CLucene 解耦核查（集成层）— 决策文档

### 结论与依据（verdict: reuse-doris）
SNII **核心层已完全 CLucene-free**：`grep -rniE "clucene|lucene::|CL_NS|<CLucene" be/src/snii` 返回空（0 命中）。格式/存储/查询字节路径无任何 CLucene 依赖，符合架构原则 1。

集成层（`be/src/storage/index/snii`）共 **8 处** CLucene 接触点，全部位于 writer/reader，`snii_doris_adapter.{h,cpp}` 为 0：
- `snii_index_writer.h:33-35` 前向声明 `namespace lucene::analysis { class Analyzer; }`；`:72` 成员 `std::shared_ptr<lucene::analysis::Analyzer> _analyzer`
- `snii_index_writer.cpp:20` `#include <CLucene.h>`；`:69`、`:92` `catch (const CLuceneError& e)`
- `snii_index_reader.cpp:20` `#include <CLucene.h>`；`:265`、`:289` `catch (const CLuceneError& e)`；`:426` `read_null_bitmap(..., lucene::store::Directory* /*dir*/)`
- `snii_index_reader.h:52` override 声明同形参

这 8 处归为两类，**均符合"解耦 CLucene、复用 Doris"原则，无需迁移**：

**类 A — 复用 Doris 分析设施（期望耦合）**：writer 经 `InvertedIndexAnalyzer::create_reader/create_analyzer`（writer.cpp:64-67）与 `get_analyse_result`（writer.cpp:90），reader 经 `get_analyse_result`（reader.cpp:263、283、286）调用 Doris 分词。Doris 的 `AnalyzerPtr` 定义为 `std::shared_ptr<lucene::analysis::Analyzer>`（`analyzer.h:40`），CLucene 类型是 **Doris 自身的实现选择**；SNII 通过 Doris 封装层使用，属"耦合到 Doris"而非"耦合到 CLucene 格式"。对应的 `CLuceneError` catch（writer.cpp:69/92、reader.cpp:265/289）是 Doris analyzer 抛出异常的兜底，与该复用绑定，应保留。

**类 B — 基类签名一致性（vestigial）**：`read_null_bitmap` 的 `lucene::store::Directory* dir` 形参仅为匹配虚基类 `InvertedIndexReader::read_null_bitmap`（`inverted_index_reader.h:233-235`）。SNII 实现完全忽略该参数（reader.cpp:426 标注 `/*dir*/`），实际从自有 `section_refs().null_bitmap` + `snii::format::NullBitmapReader` 读取（reader.cpp:443-453）；调用方以 `nullptr` 传入（`inverted_index_iterator.cpp:127`）。删除它需改 Doris 全局基类签名 → 越界，保留。

### Doris 等价物
- 分词：`be/src/storage/index/inverted/analyzer/analyzer.h` 的 `InvertedIndexAnalyzer`（`create_reader` :44 / `create_analyzer` :51 / `get_analyse_result` :53-56）。
- 基类签名：`be/src/storage/index/inverted/inverted_index_reader.h:233-235`。

### 是否最优
最优。分词是 Doris 共享基础设施，保证 SNII 写读分词一致并复用建表 6 项 analyzer 属性；自研分词会重复造轮子且语义脱节。`Directory*` 为基类约定，非 SNII 可单独决定。

### 字节兼容性
n/a — 本组件不触碰磁盘字节（分词产出 term 字符串后由 SNII 自有编码落盘；null_bitmap 由 SNII NullBitmapReader 解析）。

### 迁移设计（仅可选 cosmetic 清理，非必需）
1. `snii_index_writer.h`：删除 `namespace lucene::analysis { class Analyzer; }` 前向声明，成员改为 Doris 别名 `inverted_index::AnalyzerPtr _analyzer;`（含 `#include "storage/index/inverted/analyzer/analyzer.h"`）。把 CLucene 类型隐藏到 Doris 别名后，进一步降低头文件对 CLucene 的可见耦合。调用点：仅 writer.cpp:67 赋值、:91 取 `.get()`，签名不变，零行为变化。回滚：还原前向声明与类型一行。
2. 不改 `read_null_bitmap` 形参与 `CLuceneError` catch（属基类约定与 Doris analyzer 异常契约）。
3. 建议增设 CI 防回归 guard（见 TDD），把"核心层 0 CLucene"固化为长期约束。

### 若 KEEP 的理由
集成层 CLucene 不可也不应清零：类 A 是架构原则 2 明确要求的"优先复用 Doris"，类 B 是 Doris 基类 ABI。强行移除即放弃 Doris 分词复用或分叉基类，违背原则。故 verdict 为 reuse-doris（维持复用 + 可选 cosmetic 清理）。

---

## TDD

## TDD 测试计划（gtest target: `doris_be_test`，目录 `be/test`）

本组件为审计 + 可选 cosmetic 清理，无磁盘字节与查询语义变更，故**无需字节黄金测试/ cross-decode**。重点是"解耦约束防回归 + 复用路径功能不回归"。

### RED
1. `SniiCluceneDecouplingGuardTest.CoreHasNoCluceneRef`（新增，be/test/storage/index/snii_clucene_guard_test.cpp）：以编译期/运行期断言形式执行等价于 `grep -rniE "clucene|lucene::|CL_NS|<CLucene" be/src/snii` 的检查（可读取源码树或维护白名单），断言命中数 == 0。在核心层故意引入一处 `lucene::` 引用时该测试应 FAIL（RED）。
2. 可选清理项的编译验证：将 `_analyzer` 改为 `inverted_index::AnalyzerPtr` 前先确认现有测试编译通过基线。

### GREEN
3. 复用功能验证（沿用 `be/test/storage/index/snii_query_test.cpp`）：
   - 写入侧 `_analyze` 在 `_should_analyzer=true/false` 两路径产出 term 与 Doris `get_analyse_result` 一致（term/position 逐项断言，确定性）。
   - 查询侧 `_parse_query_terms` 对 EQUAL / MATCH_ANY / MATCH_ALL / MATCH_PHRASE / PHRASE_PREFIX / REGEXP / WILDCARD 各类型，term_infos 与 Doris analyzer 结果一致。
   - `CLuceneError` 兜底：构造非法 analyzer 配置，断言返回 `INVERTED_INDEX_ANALYZER_ERROR` 而非崩溃。
4. `read_null_bitmap` 行为：以 `dir=nullptr` 调用，断言结果 bitmap 与直接从 `section_refs().null_bitmap` 解析一致；再以任意非空 `dir` 传入，断言结果**不变**（证明形参 vestigial、被忽略）。
5. 等价性（清理前后一致）：对同一组输入，cosmetic 清理（前向声明→AnalyzerPtr 别名）前后，分词与查询结果集逐位一致（roaring bitmap `==`）。

### REFACTOR
6. 应用可选清理后重跑 3-5，断言全绿；将 guard 测试（步骤 1）纳入 `doris_be_test` 常驻 CI，固化"核心层 0 CLucene"约束。