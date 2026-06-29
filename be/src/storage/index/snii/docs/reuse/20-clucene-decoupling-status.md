# CLucene 解耦状态（R13）

## 核心层：已完全解耦（CLEAN）

SNII core（`be/src/snii`）对 CLucene 的引用为 **0**（grep 为空）。索引格式、存储、查询路径中无任何 CLucene 类型/符号。核心层全部位于 `snii` namespace，符合根本原则之「格式 / 存储 / 查询路径零 CLucene」。

## 集成层：8 处接触点，均合规（符合「期望耦合到 Doris」）

CLucene 仅残留在 Doris 集成层，且每一处都属于「复用 Doris 共享设施」或「与 Doris 基类签名一致性」，**不**属于 SNII 耦合到 CLucene 的字节/格式/查询路径：

1. **分词设施（tokenization）** — 写入侧 `snii_index_writer.cpp:64-67`（`create_reader`/`create_analyzer`）、`:90`（`get_analyse_result`），查询侧 `snii_index_reader.cpp:263/280-287`（`get_analyse_result`）。经 `be/src/storage/index/inverted/analyzer/analyzer.h` 的 `InvertedIndexAnalyzer` 解析 `analyzer_name`/`parser_type`/`parser_mode`/`char_filter`/`lower_case`/`stop_words` 六项建表属性，保证写读分词一致。返回类型 `AnalyzerPtr = std::shared_ptr<lucene::analysis::Analyzer>`（analyzer.h:40）本身是 CLucene 类型——这是 **Doris 的依赖选择**，属「期望耦合到 Doris」，不是 SNII 自行触碰 CLucene 字节路径。若 SNII 自带分词器将重复造轮且与 Doris 建表语义脱节，属明显劣化。**结论：维持复用。**

2. **`read_null_bitmap` 的 `lucene::store::Directory*` 形参** — `snii_index_reader.h:52`、`.cpp:426`（标注 `/*dir*/` 未使用），纯属与虚基类 `InvertedIndexReader::read_null_bitmap`（`inverted_index_reader.h:233-235`）签名对齐；iterator 以 `nullptr` 调用（`inverted_index_iterator.cpp:127`）。SNII 实际从自有 `section_refs().null_bitmap` + `NullBitmapReader` 读取（`snii_index_reader.cpp:443-453`），**不触碰任何 CLucene Directory**。

3. **`CLuceneError` 捕获** — 集成层异常边界，与 Doris 倒排栈一致。

## 残留清理动作（Residual Cleanup，均可选、低/零风险）

- **[可选 cosmetic] Directory 形参** — 去掉 `read_null_bitmap` 的 `lucene::store::Directory*` 需修改 Doris 全局基类签名，牵动所有倒排 reader，属**越界改动**，建议**保留现状**。
- **[可选 cosmetic] AnalyzerPtr 别名** — 用 `inverted_index::AnalyzerPtr` 别名替代头文件中前向声明的 `lucene::analysis::Analyzer`，仅为头文件可读性，零运行期风险，回滚为一行。

## 风险

低。本项不触碰磁盘字节、不改查询语义。已知的分词分支差异（docs/perf/T25：phrase 分支 properties→analyzer_ctx 切换）与本解耦审计无关，由 C1/C2 用例覆盖。
