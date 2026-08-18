# ANTLR4 SQL Parser 优化 TODO

## 目标与边界

- [ ] 优化 `fe/fe-sql-parser` 的词法分析和语法分析耗时、吞吐与分配量，同时保持 SQL 语法语义不变。
- [ ] 独立 parser 返回的 CST 不是公共 API；允许修改 rule、alternative label、Context 类型和 CST 树形，不为这些结构保留兼容层。
- [ ] 保留独立 facade 的入口能力与配置语义：statement/statements/expression 三种入口、`noBackslashEscapes`、`ansiSqlSyntax` 和 EOF 要求不能随 Context 重构改变；若要变更必须拆成独立任务。
- [ ] `DorisParser.g4` 同时被 FE 的 Nereids parser 使用；即使独立 parser 的 CST 不兼容，也必须同步修改 FE visitor/listener，并保证最终 Statement/Expression 语义不变。
- [ ] 不把错误恢复策略变化伪装成纯性能优化。SLL/LL fallback 修复单独实施、单独验证。
- [ ] 每项优化独立测量；功能测试未通过时，不采信性能结果。

当前基线：`5af094f0b08a05f715f1391bb7286616a410a885`，ANTLR 4.13.1，JDK 17。正式测量时仍需记录实际环境，不能只依赖本文中的版本号。

## 2026-08-18 `STRING_LITERAL` 打样状态

本节只记录优化项 A 的框架打样，不表示整个 TODO 的 G0–G7 已完成。后续优化仍必须按下文依赖关系补齐通用 corpus、FE AST、错误路径、fuzz 和全量集成门禁。

- [x] 已新增独立 `fe-sql-parser-benchmark` 模块；只有显式启用 `benchmark` profile 才进入 reactor，默认 parser jar 不包含 JMH 类或 JMH runtime 依赖。
- [x] 已新增 lexer-only、独立 parser end-to-end 和无字符串控制 SQL 三类 JMH case，覆盖两种 `noBackslashEscapes`、两种引号、四种 payload pattern 和 16 B–64 KiB 长度。
- [x] 已新增字符串专项 UT：合法/非法边界、token 位置、两种 SQL mode、Unicode、长输入、谓词调用复杂度和 8 线程并发一致性。
- [x] 基线与候选对 46,908 条穷举/边界输入生成完整 token/error snapshot；两份文件 SHA-256 均为 `d9c0ca0018ff9cf3bac3c8320086d5e6a7c56db9383b877d86380428685908fc`，逐字节差异为 0。
- [x] `STRING_LITERAL` 在读到开引号后按 SQL mode 选择一次无谓词循环；实测 semantic predicate 调用由约 `3 × payload chars` 降为每个字符串固定 2 次。
- [x] 专项测试通过：`DorisLexerStringLiteralTest` 及 `DorisSqlParserTest` 共 12 个测试，0 failure/error/skip；其中并发用例使用 8 threads，各重复代表性 corpus 100 次。
- [x] G3 构建与隔离检查通过：`DISABLE_BUILD_UI=ON ./build.sh --fe` 成功；默认 parser jar 中 JMH/benchmark class 为 0。默认带 UI 构建被本机 Homebrew Node 缺失 `libsimdjson.30.dylib` 阻断，与 parser 改动无关。
- [x] 目标 case 的收益远高于测量噪声：plain/single-quote 的 4 KiB lexer 从约 1.40–1.74 ms/op 降到约 0.029–0.059 ms/op；64 KiB lexer 从约 21.6–24.4 ms/op 降到约 0.55–0.70 ms/op。64 KiB lexer 分配量从约 71–75 MiB/op 降到约 194 KiB/op。
- [ ] **G2 暂不放行精细耗时结论**：当前机器上同参数控制 SQL 的 baseline A/A 最大偏差约 15.9%，候选重复最大偏差约 6.2%，超过 3% 门槛。不得宣称无字符串路径的 1%–3% 变化；需在低负载固定频率机器上按 B-A-A-B 重跑。
- [ ] **G5 暂不最终放行**：目标热点的数量级收益已经成立，但必须先让 G2 通过，并补跑所有 pattern/quote 的正式两轮结果，确认 tiny/invalid/control 分组无超过 3% 的可复现回退。
- [ ] **G6/G7 未执行**：当前已完成标准专项 FE UT 和无 UI 的正式 FE build；仍需全量 FE UT、约定 regression、最终 corpus/fuzz、结果归档和人工验收。

打样结论：实现方向和“先语义差分、再复杂度守卫、后 JMH、控制组阻断”的整体框架有效；它既识别出明确的字符串热点收益，也成功阻止在不稳定机器上对微小控制组差异作过度结论。

## 审查结论

- [x] 目标明确：这是纯 parser 性能优化，语法接受范围和 FE 最终语义必须保持不变；独立 parser 的 CST 结构兼容不在目标内。
- [x] 需要同时覆盖两条运行路径：独立 `DorisSqlParser` 和 FE `NereidsParser`。只验证独立 jar 不足以放行。
- [x] 主要风险是语法歧义、错误恢复、token 重写和下游 visitor 适配；不涉及事务、持久化、FE/BE 协议、配置项或数据写入。
- [x] 存在并发使用场景：facade 本身无状态，但 ANTLR 生成类共享静态 DFA。需要并发正确性测试和多线程 benchmark，不需要引入业务锁。
- [x] 不需要新增线上日志或 metrics；JMH、ATN profiling 和可保存的结果文件足以提供开发期可观测性。
- [x] 原 TODO 的测试面基本完整，但缺少统一、可阻断的 Gate、硬依赖关系、A/A benchmark 校验和 AI 交付节奏。下面补齐这些约束。

## 门禁总览

任一 Gate 未通过，后续阶段只能继续补测试或诊断，不能开始下一项 grammar 优化。所有“差异为零”均指“未解释差异为零”；若确认基线本身是 bug，必须拆成独立正确性变更，不能混入性能 patch。

| Gate | 放行条件 | 阻断的后续工作 |
|---|---|---|
| G0 基线可复现 | 工作树、版本、生成物和环境已冻结；标准构建与现有测试通过 | 所有测试、benchmark 和优化 |
| G1 语义保护网 | 目标 rule 覆盖矩阵 100%；token/AST/error golden 经人工审阅；固定 seed fuzz 通过 | M0 baseline benchmark 及 A–G |
| G2 Benchmark 可信 | JMH harness 校验通过；A/A 两轮稳定；baseline 原始数据完整 | 所有性能结论及 A–G |
| G3 单项实现完整 | 单项 diff 聚焦；生成代码非手改；独立/FE 消费方均能编译 | 该项功能与性能验收 |
| G4 语义等价 | token、合法 SQL AST、非法 SQL 结果均为零未解释差异；专项/并发/压力测试通过 | 该项性能验收和后续优化 |
| G5 性能有效 | 收益超过预设门槛且主要场景无显著回退；两轮可复现 | 将该项纳入累计 candidate |
| G6 集成回归 | 全量 FE UT、选定 regression suites、最终 corpus/fuzz 全通过 | 最终交付 |
| G7 交付证据完整 | 测试、benchmark、环境、差异和已知限制均归档 | 宣布完成 |

### 门禁的量化口径

- [ ] G1：为 A–G 涉及的每个 grammar alternative 建立可追踪矩阵，每格至少有 1 个合法样本和 1 个相邻非法/边界样本；不能用单纯 SQL 数量代替 alternative 覆盖。
- [ ] G1：真实/回归 SQL corpus 至少包含 2,000 条去重合法 SQL；固定 seed mutation/fuzz 至少执行 50,000 条。若无法达到，必须记录数据来源和缺口并由人工决定是否放行。
- [ ] G1：两种 `noBackslashEscapes` 与两种 `ansiSqlSyntax` 的适用用例都必须覆盖；不适用的组合在 manifest 中显式标记，不得默认为未测。
- [ ] G2：对同一 baseline artifact 做 A/A 交错测量；主要 case 两轮差异绝对值不超过 3%，单轮 fork 间 CV 不超过 5%。超过时增加测量时间或排除环境噪声，不能直接提高阈值。
- [ ] G2：每份结果记录 artifact SHA-256、corpus SHA-256、git commit 和 JVM 参数，避免比较错二进制或错数据。
- [ ] G3：每项修改均通过 `./build.sh --fe` 的生成、编译和 Checkstyle；ANTLR 生成文件只由固定版本插件生成，发布 jar 不包含 JMH 类或新增 JMH runtime 依赖。
- [ ] G4：lexer token tuple 差异为 0；合法 SQL 接受/拒绝和 FE AST 语义快照差异为 0；非法 SQL 的接受/拒绝、首错位置、约定诊断字段差异为 0。
- [ ] G4：并发正确性测试至少使用 8 threads，各重复完整代表性 corpus 100 次；输出必须与单线程 golden 一致，且无异常、死锁或数据污染。
- [ ] G5：性能门槛在写实现前冻结。默认要求主要 end-to-end/FE end-to-end case 不回退超过 3%，bytes/op 不回退超过 3%。
- [ ] G5：纯性能改动还必须满足以下之一：真实 workload 加权指标提升至少 3%，或预先声明且有生产意义的目标热点提升至少 10%；统一换算为“正数代表改善”的相对变化后，95% 置信区间不能跨过 0 收益线。
- [ ] G5：generated class/jar 大小、冷启动时间或深度输入栈安全若回退超过 10%，必须单独评审，不能由吞吐提升自动抵消。

## 0. 开工前冻结基线

- [ ] 记录基线 commit、分支、是否有本地修改；不得把 `gen/` 等未跟踪生成物混入测试或 benchmark classpath。
- [ ] 如果 baseline/candidate 使用 Git worktree，先检查 `.worktree_initialized`；缺失时按根目录 `AGENTS.md` 执行 `hooks/setup_worktree.sh`，并验证依赖与 submodule。每个 worktree 的构建、测试、产物和端口都必须留在自身目录。
- [ ] 记录 JDK、Maven、ANTLR runtime/plugin、OS、CPU 型号、核心数、内存和 JVM 参数。
- [ ] 固定 benchmark 机器、电源模式和后台负载；笔记本接电，关闭会造成明显抖动的任务。
- [ ] 使用同一 JDK、同一 Maven 参数和同一份 SQL corpus 构建 baseline/candidate。
- [ ] 按仓库标准脚本执行干净基线构建和定向 UT；不要用裸 Maven 命令替代正式门禁：

  ```bash
  ./build.sh --fe
  ./run-fe-ut.sh --run org.apache.doris.sqlparser.DorisSqlParserTest
  ```

- [ ] 性能测试明确使用 RELEASE 构建；除性能测量外保持仓库默认构建类型。环境调整写入本机 `custom_env.sh`，不得纳入提交。

- [ ] 保存基线生成物诊断数据：生成后的 lexer/parser Java 文件大小、class/jar 大小、ATN decision 数、`adaptivePredict` 调用点数量。它们只用于解释结果，不作为性能结论。
- [ ] 建立结果目录，保存原始 JMH JSON/CSV、GC profiler 输出、命令行、环境信息和汇总表；禁止只保留人工摘录的数据。

## 1. 改动前必须补齐的测试

### 1.1 建立语义差分测试框架

- [ ] 新增共享 SQL corpus；单测和 benchmark 从同一份 corpus 读取，避免两套样本逐渐漂移。
- [ ] corpus 中每条用例至少记录：稳定 ID、SQL、入口（statement/statements/expression）、`noBackslashEscapes`、`ansiSqlSyntax`、预期成功或失败、所属语法类别。
- [ ] corpus manifest 额外记录所覆盖的 lexer branch、parser rule/alternative 和优化项 A–G；自动生成覆盖矩阵并在存在空格子时失败。
- [ ] 对基线结果进行人工审阅后再生成 golden，不能把当前 parser 的未知错误直接当成正确语义固化。
- [ ] 因为允许 CST 变化，baseline/candidate **不比较** Context 类名、alternative label、child 层级或 `toStringTree()`。
- [ ] 对 lexer 输出比较完整 token tuple：`type`、`channel`、原始 `text`、`startIndex`、`stopIndex`、`line`、`charPositionInLine`，直至 EOF。
- [ ] 对合法 SQL 比较：是否成功、是否消费到 EOF、statement 数量，以及 FE visitor 构造出的 Statement/Expression 的规范化语义快照。
- [ ] 规范化语义快照应排除对象 ID、origin 等非语义且不稳定的字段；至少保留节点类型、操作符、标识符、字面量、子节点顺序和关键选项。
- [ ] 如果暂时无法稳定序列化 FE AST，先使用现有 FE parser 的 visitor 结果做字段级断言，并将“补齐通用 AST snapshot”作为合入语法重构前的阻塞项。
- [ ] 对非法 SQL 比较：必须拒绝、异常类型、首个错误 token/offset/line/column。错误文案只有在被确认属于用户契约时才做全文 golden，其他情况比较结构化位置与关键片段。
- [ ] 保证测试能分别运行 baseline jar 和 candidate jar。推荐两个独立 JVM 输出 JSON 再做 diff，避免同名 ANTLR 生成类在同一 classloader 中互相污染。
- [ ] 加入 corpus 完整性检查：ID 唯一、每条用例显式声明入口和模式、所有用例确实执行、golden 不存在孤儿项。

### 1.2 Lexer：字符串与模式谓词

这是优化 `DorisLexer.g4` 中 `STRING_LITERAL` 循环内 semantic predicate 的前置保护。

- [ ] 单引号和双引号分别覆盖：空串、普通 ASCII、中文、补充平面 Unicode 字符、换行附近的定位信息。
- [ ] 覆盖连续引号转义：`''`、`""`、多组连续转义、转义出现在开头/中间/结尾。
- [ ] `noBackslashEscapes=false` 覆盖：`\\'`、`\\"`、`\\\\`、反斜杠加普通字符、奇偶数量连续反斜杠、反斜杠接换行。
- [ ] `noBackslashEscapes=true` 对同一组输入做镜像测试，确认 token 边界、后续 token 和报错位置与基线一致。
- [ ] 覆盖未闭合单/双引号、末尾孤立反斜杠、引号后紧跟标识符/数字/注释。
- [ ] 参数化长字符串长度：0、1、16、256、4 KiB、64 KiB；内容同时覆盖普通字符、密集反斜杠、密集 doubled quote。
- [ ] 增加 lexer predicate 调用计数测试或独立诊断程序；优化后确认调用次数不再随字符串字符数线性乘上模式判断次数。该项是实现检查，不代替耗时 benchmark。

### 1.3 PostProcessor：标识符语义

这是把全局 parse listener 改为局部 rule action 或等价处理的前置保护。

- [ ] 覆盖普通 `IDENTIFIER`、全部 `nonReserved` token 作为标识符、反引号标识符和包含 ```` 的反引号标识符。
- [ ] 验证处理后的标识符文本、token type、channel、source interval 与当前语义一致；不依赖旧 Context 层级。
- [ ] 在表名、库名、列名、别名、函数名、属性 key、multipart identifier 等位置复用上述用例。
- [ ] 覆盖大小写、Unicode、关键字与非保留字的边界组合。
- [ ] 覆盖非法未加引号连字符标识符：单个/多个 `-`、开头/结尾 `-`、表达式 `a-b` 与名称 `test-table` 的区分。
- [ ] 断言非法标识符仍抛出预期异常，并保留准确的 origin/line/column 和标识符文本。
- [ ] 加入 listener 移除后的守卫测试：解析期间不再注册遍历所有 rule 的 `PostProcessor`，局部处理仍只执行一次。

### 1.4 `primaryExpression` 左因子提取

- [ ] 为 `primaryExpression` 的每个现有 alternative 至少准备一个合法样本，而不是只覆盖常见函数和列引用。
- [ ] CASE：searched/simple、多个 WHEN、ELSE 有无、嵌套 CASE、CASE 中包含子查询或复杂表达式。
- [ ] CONVERT：`USING charset` 与 `, dataType` 两种形式，以及能逼迫 parser 看到分歧点之后才能决定的复杂参数。
- [ ] 括号前缀：parenthesized expression、scalar subquery、嵌套括号、WITH/query 开头的子查询。
- [ ] 标识符前缀：列引用、qualified star、函数调用、多层 dereference、数组下标、array slice、collate 及其组合链。
- [ ] 覆盖 CAST/TRY_CAST、CHAR、GROUP_CONCAT、TRIM、SUBSTR/SUBSTRING/MID、POSITION、ISNULL、变量、KEY、EXTRACT、interval、constant、`* EXCEPT/REPLACE`。
- [ ] 覆盖算术/比较/布尔优先级与结合性；使用 FE AST 快照断言树的语义结合方向，而不是 ANTLR CST 形状。
- [ ] 每类合法样本都补相邻非法样本：缺右括号、缺 END、缺 FROM/USING、缺下标、非法后缀、尾随垃圾。

### 1.5 `statementBase` 首 token 分派

- [ ] `statementBase` 每个顶层分支至少覆盖一个样本：query、DML、CREATE、ALTER、MV、JOB、CONSTRAINT、CLEAN、DESCRIBE、DROP、SET/UNSET、REFRESH、SHOW、LOAD、CANCEL、RECOVER、ADMIN、USE、OTHER、KILL、STATS、TRANSACTION、GRANT/REVOKE。
- [ ] 对共享前缀建立成组测试：所有 `CREATE ...`、`ALTER ...`、`SHOW ...`、`DROP ...`、`ADMIN ...` 变体。
- [ ] query/DML 覆盖 `EXPLAIN`、`WITH`、括号、hint 以及可选 outfile 等会延后分派决定的位置。
- [ ] 多语句覆盖空白、注释、分号、尾分号和中间非法 statement，验证 statement 数及首错位置。
- [ ] 为每个共享前缀加入截断输入，例如只有 `CREATE`、`ALTER TABLE`、`SHOW`，防止重构改变错误接受范围。

### 1.6 可空 helper rule 重构

- [ ] `queryOrganization` 覆盖：均无、仅 ORDER BY、仅 LIMIT、ORDER BY + LIMIT，以及三种 LIMIT/OFFSET 形式。
- [ ] `tableAlias` 覆盖：无 alias、裸 alias、`AS alias`、带 column alias list；在 table、subquery、TVF、join、lateral view 周围验证 token 归属不变。
- [ ] `errorCapturingIdentifierExtra` 覆盖空分支与错误分支，并确认 `a-b` 在表达式位置仍是减法。
- [ ] 将 helper 改为非空并在调用点加 `?` 后，使用 FE AST 快照确认“缺失”与“存在但为空”不会改变下游逻辑。
- [ ] 搜索并更新所有直接访问这些 Context 的 visitor/listener/test；不为旧 Context API 增加兼容代码。

### 1.7 SLL → LL fallback 与错误恢复

- [ ] 先构造至少一个能让 SLL 首次尝试失败、LL 成功的合法 SQL；测试必须证明两阶段路径真的被执行，不能只断言最终成功。
- [ ] 首次 SLL 使用 `BailErrorStrategy` 时，断言失败后 token stream 回到 0、parser reset、LL 使用正常错误策略且 listener 状态干净。
- [ ] 覆盖 SLL 和 LL 都失败的输入，确保只暴露一次最终错误，不出现重复 listener 回调或第一次尝试的残留错误。
- [ ] 覆盖开头错误、深层嵌套错误、末尾错误、超长错误输入和 expression 尾随垃圾。
- [ ] 独立 parser 与 FE `NereidsParser` 两条并行入口都执行同一组 fallback 测试。
- [ ] 将 fallback 修复与 grammar 性能重构分开测量；其目标首先是正确性，不能用平均性能提升决定是否保留。

### 1.8 注释 lexer（若实施）

- [ ] 覆盖普通块注释、嵌套块注释、相邻注释、comment-like 字符串、hint `/*+ ... */` 与普通注释的区分。
- [ ] 覆盖 1/10/100/1000 层嵌套，以及 16 B/4 KiB/64 KiB/1 MiB 注释。
- [ ] 覆盖未闭合外层/内层注释和注释后的 token 定位。
- [ ] 比较 token type、channel、text 和 source interval；mode/depth-counter 实现不得改变 hint 或 hidden channel 行为。

### 1.9 回归与质量门槛

- [ ] `fe-sql-parser` 新测试全部通过，现有 7 个测试保留或被更强的参数化测试取代。
- [ ] 使用 `./run-fe-ut.sh --run org.apache.doris.sqlparser.DorisSqlParserTest` 执行独立 parser 定向 UT。
- [ ] 使用 `./run-fe-ut.sh --run <comma-separated-parser-tests>` 执行受影响的 Nereids parser/visitor UT；至少包含 `NereidsParserTest`、`ExpressionParserTest`、`LimitClauseTest` 及本次修改直接影响的测试类。通过编译错误定位所有需要同步修改的 Context 消费者。
- [ ] 使用 `./run-fe-ut.sh` 执行全量 FE UT，作为 G6 的强制门禁。
- [ ] 使用 `./run-regression-test.sh -d nereids_syntax_p0 -s <suite-name>` 先跑定向 suite，再运行完整 `nereids_syntax_p0` 及受影响的 DDL/DML/query suites；实际 suite 清单在 G1 阶段冻结。
- [ ] regression 预期结果只能通过测试脚本生成，不得手写 `.out`；错误用例遵循 `test { sql ...; exception ... }`。
- [ ] 对大 corpus 做 baseline/candidate 差分，合法与非法结果均为零未解释差异。
- [ ] 对 fuzz/generated SQL 做固定 seed 测试并保存失败样本；至少包含 token mutation、截断、括号/引号/注释不平衡和深层嵌套。
- [ ] 添加深度/长度压力测试，确认不会引入新的 StackOverflowError、超线性耗时或异常内存增长。压力测试与普通 UT 分组，避免日常 UT 不稳定。
- [ ] 添加 8-thread 并发正确性测试，覆盖独立 parser 和 FE parser；使用 barrier/latch 同步启动，不使用 sleep 猜测时序。

## 2. Benchmark 基础设施

### 2.1 使用 JMH，而不是手写 `nanoTime`

- [ ] 在独立 Maven profile 或 benchmark module 中引入 JMH，避免 JMH 依赖进入发布 jar。
- [ ] benchmark 源码放在 `src/jmh/java` 或独立 benchmark module；默认 `mvn test/package` 不执行耗时 benchmark。
- [ ] 固定并记录：warmup iterations、measurement iterations、fork 数、每轮时间、threads、JVM 参数和 profiler。
- [ ] 建议初始配置：`forks=5`、warmup `8 x 1s`、measurement `10 x 1s`、单线程；根据误差再增加轮数，不能为追求好看结果减少 fork。
- [ ] 同时输出 Throughput 和 SampleTime；使用 `-prof gc` 记录 `gc.alloc.rate.norm`、GC 次数和 GC 时间。
- [ ] 每个 fork 使用全新 JVM，以包含 ANTLR 静态 DFA 从冷到稳态的真实行为；另设 steady-state benchmark，不混合解释两种结果。
- [ ] 不在 timed method 中读取文件、生成随机 SQL、打印日志或构建 corpus。
- [ ] 使用 JMH `Blackhole` 或返回解析结果，防止死代码消除。
- [ ] 对长输入使用参数化预生成字符串；baseline/candidate 必须读取字节完全相同的 corpus。

### 2.2 分层 benchmark

- [ ] **Lexer-only**：每次 invocation 新建 lexer 并消费到 EOF；覆盖两种 `noBackslashEscapes` 模式。
- [ ] **Parser-only**：输入使用预先生成的 token 列表，每次 invocation 新建 `ListTokenSource`/`CommonTokenStream`/parser，隔离 lexer 成本。
- [ ] **End-to-end**：调用公开 facade，从原始 SQL 创建 lexer、token stream、parser 并返回结果；这是主要用户指标。
- [ ] **FE end-to-end**：通过 `NereidsParser` 解析并完成 visitor/AST 构造，防止 grammar 局部加速却让下游变慢。
- [ ] **错误路径**：分别测 early error、late error、SLL fail + LL success、SLL + LL 都失败；错误 benchmark 不能与合法 SQL 聚合。
- [ ] **多语句**：1、10、100 条 statement，测吞吐和每 statement 平均分配。
- [ ] **并发扩展**：1/2/4/8 threads 单独运行，检查共享 DFA 下的吞吐扩展；不要把多线程结果与单线程结果混为一个分数。

### 2.3 Benchmark corpus 矩阵

- [ ] Tiny：`SELECT 1`、简单 expression，用于观察固定成本和 listener 开销。
- [ ] Typical：投影、过滤、聚合、排序、limit、常见 DDL/DML。
- [ ] Complex expression：嵌套 CASE、CONVERT、函数链、dereference、array subscript/slice、复杂布尔和算术表达式。
- [ ] Complex query：CTE、子查询、多 join、window、set operation、长 select list。
- [ ] Shared-prefix statements：CREATE/ALTER/SHOW/DROP/ADMIN 的所有主要变体。
- [ ] Identifier-heavy：宽 schema、长 multipart identifier、nonReserved、quoted identifier、alias list。
- [ ] String-heavy：两种 SQL mode 下，不同长度和不同转义密度的字符串。
- [ ] Comment-heavy：大量短注释、超长注释、深层嵌套注释和 hint。
- [ ] Large：4 KiB、64 KiB、1 MiB SQL；单独报告，避免大输入掩盖典型请求。
- [ ] Invalid：开头/中间/末尾错误、未闭合字符串/注释/括号、极深错误输入。
- [ ] Real-world：从脱敏 query log 或 regression suites 抽样；去重后按长度和 statement 类型分层，记录抽样方法与 seed。

## 3. 改动前后的运行方式

### 3.1 构建可比的 baseline 与 candidate

- [ ] baseline 使用固定 commit 的独立 worktree 或独立产物目录；candidate 使用当前工作树。不要在同一 `target/` 上交替覆盖后直接比较。
- [ ] 两侧都执行 clean generate/compile，确认生成代码来自各自 grammar，而非残留文件。
- [ ] 用相同命令分别运行，并将 JMH 结果输出到不同 JSON：

  ```bash
  java -jar <baseline-jmh.jar> '.*DorisParser.*' \
    -f 5 -wi 8 -i 10 -w 1s -r 1s -prof gc \
    -rf json -rff <result-dir>/baseline.json

  java -jar <candidate-jmh.jar> '.*DorisParser.*' \
    -f 5 -wi 8 -i 10 -w 1s -r 1s -prof gc \
    -rf json -rff <result-dir>/candidate.json
  ```

- [ ] 先运行一次不计入结果的 smoke benchmark，确认参数和 corpus 正确。
- [ ] 正式运行时 baseline/candidate 交错执行（例如 B-A-A-B 或随机顺序），降低温度、后台任务和系统漂移造成的偏差。
- [ ] 至少重复一整轮正式测量；若两轮结论方向不一致，先排查噪声，不下性能结论。

### 3.2 统计与判定

- [ ] 对每个 benchmark case 报告 score、单位、误差/置信区间、相对变化和 `gc.alloc.rate.norm`，不只报告总平均值。
- [ ] 主要指标：end-to-end typical/real-world 的吞吐、p50/p95/p99（SampleTime）和 bytes/op。
- [ ] 次要指标：lexer-only、parser-only、FE end-to-end、错误路径、冷启动和多线程扩展。
- [ ] 使用 G5 中已在实现前冻结的门槛判定；禁止测量后重新选择指标、workload 权重或阈值。
- [ ] 不以“总平均提升”抵消关键场景明显回退；tiny、typical、large、invalid 分组分别判定。
- [ ] 对异常结果用 JFR/async-profiler/perfasm 做归因，确认收益来自 predicate、`adaptivePredict`、listener dispatch 或 allocation 的下降，而非 corpus/错误路径变化。
- [ ] 用 `ProfilingATNSimulator` 复查热点 decision：invocations、SLL lookahead、LL fallback、timeInPrediction；profiling 模式的绝对耗时不与普通 JMH 数字混用。

### 3.3 每个优化项的 A/B 顺序

- [ ] A：将 `STRING_LITERAL` 的 SQL mode predicate 提到 rule-level alternative；跑字符串 lexer 测试、token diff、lexer-only 和 end-to-end benchmark。
- [ ] B：将 `PostProcessor` 三个局部行为移入对应 grammar rule/action 或等价局部处理并移除全局 listener；跑标识符测试、非法 identifier 测试、tiny/identifier-heavy/FE end-to-end benchmark。
- [ ] C：对 `primaryExpression` 做左因子提取；允许 Context 变化，同步 FE visitor；跑全 alternative/优先级测试、AST diff、complex-expression/parser-only/end-to-end benchmark。
- [ ] D：按首 token 重构 `statementBase` 分派；跑所有 statement family 和共享前缀测试、AST diff、shared-prefix benchmark。
- [ ] E：消除 `queryOrganization`、`tableAlias`、`errorCapturingIdentifierExtra` 等空 Context；同步所有调用点；跑 helper 专项测试、AST diff、allocation benchmark。
- [ ] F：使用正确的 SLL bail + LL retry 流程；独立 parser 与 FE parser 同步；跑 fallback/错误诊断测试和错误路径 benchmark。
- [ ] G（可选）：将递归块注释改为 lexer mode + depth counter；跑注释 token diff、深度压力测试和 comment-heavy benchmark。
- [ ] 每完成一项只与它的直接前一版本及原始 baseline 各比较一次，避免收益/回退在后续改动中被掩盖。
- [ ] `CaseInsensitiveStream` ASCII fast path、重写当前低 lookahead 的表达式左递归、仅重排 alternatives、仅拆分 grammar 文件，不进入首轮实施；除非 profiler 给出新的证据。

## 4. 前后依赖关系

### 4.1 工作项依赖表

“硬依赖”未完成时不得开始该项；“排期依赖”用于保证 benchmark 可归因，即使代码层面可以并行，也按顺序合入累计 candidate。

| ID | 工作项 | 硬依赖 | 排期依赖 | 产出/放行 Gate |
|---|---|---|---|---|
| F0 | 冻结工具链、工作树和构建基线 | 无 | 无 | 环境清单、基线测试；G0 |
| T0 | 定义 corpus manifest、snapshot 格式和差分协议 | F0 | 无 | schema、runner、人工审阅流程 |
| T1 | Lexer/string/comment/identifier token 测试 | T0 | 无 | Lexer alternative 覆盖矩阵 |
| T2 | Parser/FE AST/statement/expression/helper 测试 | T0 | 无 | Parser alternative 覆盖矩阵、AST golden |
| T3 | 非法 SQL、SLL/LL、压力和并发测试 | T0 | 无 | Error golden、fallback 证据、fuzz 报告；G1 |
| J0 | JMH harness 和结果归档工具 | T0 | 可与 T1–T3 并行开发 | 可运行 benchmark jar、JSON/GC 输出 |
| M0 | baseline A/A、正式 baseline 和 ATN profile | T1、T2、T3、J0 | G1 后执行 | 稳定性报告、baseline 数据；G2 |
| A | STRING_LITERAL predicate 提升 | T1、M0 | 首个优化 | G3 → G4 → G5 |
| B | 移除全局 PostProcessor listener | T1、T2、M0 | A 被接受或撤销后 | G3 → G4 → G5 |
| C | `primaryExpression` 左因子提取 | T2、M0 | B 被接受或撤销后 | G3 → G4 → G5 |
| D | `statementBase` 首 token 分派 | T2、M0 | C 被接受或撤销后 | G3 → G4 → G5 |
| E | 消除可空 helper Context | T2、M0 | D 被接受或撤销后 | G3 → G4 → G5 |
| F | 修正 SLL bail + LL retry | T3、M0 | C/D/E 结束后，避免 grammar 变化反复改 fallback 样本 | G3 → G4；性能只作观察 |
| G | 块注释 mode + depth counter（可选） | T1、M0 | A 结束后；与 A 同属 lexer，不并行修改 | G3 → G4 → G5 |
| I0 | 累计 candidate 集成验证 | 所有被接受项 | F 完成，G 已执行或明确跳过 | G6 |
| R0 | 结果归档和最终 review | I0 | 无 | G7 |

### 4.2 关键路径

```text
F0
 └─ T0
     ├─ T1 ─┐
     ├─ T2 ─┼─ M0 ─ A ─ B ─ C ─ D ─ E ─ F ─ I0 ─ R0
     ├─ T3 ─┤                 └───────────────┘
     └─ J0 ─┘
               A ─ G（可选）───────────────┘
```

- [ ] T1、T2、T3、J0 可以并行准备，但必须先冻结 T0 的 corpus/snapshot 接口，避免四套不兼容工具。
- [ ] A–E 必须串行进入累计 candidate。原因不是代码一定冲突，而是每项都需要和“直接前一版本”做 A/B，才能判断收益来源。
- [ ] C、D、E 都会改变 parser Context 和 FE visitor，禁止多个 AI agent 同时编辑 grammar/visitor；可以并行做只读分析或补互不重叠的 corpus。
- [ ] F 的正确性不依赖 A–E，但 fallback 样本和 prediction 行为会被 grammar 重构影响，所以排在 grammar 形状稳定之后。
- [ ] G 与 parser 重构相对独立，但和 A 共用 lexer 文件；只有 comment-heavy profile 证明值得做时才排期。
- [ ] 任一优化未通过 G4，立即撤销该项 candidate，不进入后续累计版本；通过 G4 但未通过 G5，默认也不保留纯性能复杂度，除非人工批准。

## 5. 后续使用 AI 开发的排期

### 5.1 推荐排期

以下按“AI 有完整代码上下文、测试资源可用”的工作日估算；集群排队、长 benchmark 和人工审阅等待时间不计入。建议以 Gate 为进度单位，不以日期强行放行。

| 周期 | AI 批次 | 预计投入 | 主要工作 | 结束条件/人工确认点 |
|---|---|---:|---|---|
| 第 1 周前半 | AI-0 / F0+T0 | 0.5–1 天 | 冻结基线，定义 manifest、snapshot、差分输出和目录 | G0；人工确认语义比较口径 |
| 第 1 周 | AI-1 / T1–T3 | 2–3 天 | 补 lexer/parser/error/并发/压力测试，抽取并审阅 corpus | G1；**人工批准 golden，AI 不得自动接受差异** |
| 第 1 周末 | AI-2 / J0+M0 | 1.5–2 天 | 建 JMH、做 A/A、跑正式 baseline 和 ATN profile | G2；人工冻结性能门槛与目标 case |
| 第 2 周前半 | AI-3 / A | 0.5–1 天 | string predicate 优化、专项差分、两轮 benchmark | G3–G5；决定保留或撤销 A |
| 第 2 周后半 | AI-4 / B | 1–1.5 天 | listener 局部化、同步独立/FE 路径、benchmark | G3–G5；决定保留或撤销 B |
| 第 3 周前半 | AI-5 / C | 1.5–2.5 天 | `primaryExpression` 左因子化、visitor 适配、语义与性能验证 | G3–G5；高风险人工 review |
| 第 3 周后半 | AI-6 / D | 1–2 天 | `statementBase` 分派重构和共享前缀验证 | G3–G5；高风险人工 review |
| 第 4 周前半 | AI-7 / E | 1–1.5 天 | 可空 helper 重构、allocation 验证 | G3–G5 |
| 第 4 周中 | AI-8 / F | 1–1.5 天 | SLL/LL fallback 正确性修复和错误路径验证 | G3–G4；人工确认错误行为 |
| 第 4 周后半 | AI-9 / G | 0 或 1–1.5 天 | 仅在 profile 支持时优化嵌套注释 | G3–G5，或记录跳过理由 |
| 第 5 周 | AI-10 / I0+R0 | 1–2 天 | 全量 FE UT、regression、最终 fuzz/benchmark、归档和 review | G6–G7；最终人工验收 |

预计关键路径为 11–18 个 AI 工作日，通常跨 4–5 周完成；如果 C、D 或 E 没有达到 G5 并被撤销，实际时间会缩短。长时间 benchmark 应安排在夜间，但必须在下一批 grammar 修改开始前完成判定。

### 5.2 AI 每批次固定工作协议

- [ ] 开始时重新读取根目录及相关模块 `AGENTS.md`、适用 skill 和上一批 handoff，确认当前 baseline/candidate SHA。
- [ ] 一次只领取一个表中 ID；先列受影响 grammar rules、Java 消费方、测试和 benchmark case，再修改代码。
- [ ] 修改前运行该项定向测试并保存 baseline；修改后先跑最小专项测试，再跑差分，最后才跑 benchmark。
- [ ] 禁止 AI 自动更新 golden 来消除失败。任何 golden 变化必须输出结构化 diff、说明是否为语义变化，并停在人工确认点。
- [ ] 禁止通过放宽错误匹配、删除慢 case、缩短 warmup/fork、改变 corpus 权重或提高回退阈值让 Gate 变绿。
- [ ] 每批输出 handoff：改动文件、目标假设、测试命令与结果、语义 diff、JMH 原始文件、性能汇总、未解决问题、保留/撤销建议。
- [ ] 若使用多个 AI agent，只允许并行处理互不重叠的测试数据、只读 profiling 或结果分析；grammar、生成器调用和 `LogicalPlanBuilder` 修改由单一 agent 串行完成。
- [ ] 每项被接受后先固定其 candidate artifact，再启动下一项；不得在 benchmark 未完成时继续叠加优化。
- [ ] 实现阶段按用户授权和仓库提交规范操作；本文档 review 阶段不创建 commit。

### 5.3 人工必须介入的节点

- [ ] H1（G1）：批准 corpus 来源、AST snapshot 规范和初始 golden，确认没有固化已知 parser bug。
- [ ] H2（G2）：批准目标 workload 权重和 G5 阈值，避免看到结果后修改判定标准。
- [ ] H3（每个 G4/G5）：审阅任何语义差异、性能回退和复杂度增加，明确保留或撤销。
- [ ] H4（F）：确认错误类型、位置和消息是否属于兼容契约。
- [ ] H5（G6/G7）：审阅最终累计收益、未实施项及回归证据，再决定交付。

## 6. 每项优化的完成定义

- [ ] grammar 与 Java 消费方修改聚焦于单一优化目标，没有顺手改变语法接受范围。
- [ ] 新增专项正向、负向和边界测试，且先能在 baseline 上建立预期。
- [ ] lexer token diff 为零；若优化本身不涉及 lexer，任何 token 差异都视为阻塞。
- [ ] 合法 SQL 的 FE 语义快照 diff 为零。
- [ ] 非法 SQL 的接受/拒绝、首错位置和约定的诊断字段无未解释差异。
- [ ] 全量 corpus 与固定 seed fuzz 无未解释差异。
- [ ] UT、受影响 FE tests 和选定 regression suites 全部通过。
- [ ] `./build.sh --fe` 通过，包含生成代码一致性、编译和 Checkstyle；release jar 中没有 JMH 类或新增 benchmark runtime 依赖。
- [ ] JMH 原始结果、环境、命令和汇总已保存；收益可在至少两轮正式测量中复现。
- [ ] 没有主要场景超过约定的回退门槛；所有例外有明确原因和评审结论。
- [ ] 生成代码未被手工编辑；未来实现提交只包含 grammar、运行时代码、测试、benchmark 基础设施及必要文档，不包含环境文件或 benchmark 原始临时产物。

## 7. Benchmark 结果记录模板

| 项目 | Baseline | Candidate | 变化 | 误差/置信区间 | bytes/op 变化 | 结论 |
|---|---:|---:|---:|---:|---:|---|
| Lexer/string/default mode |  |  |  |  |  |  |
| Lexer/string/no-backslash mode |  |  |  |  |  |  |
| Parser/complex expression |  |  |  |  |  |  |
| Parser/shared-prefix statement |  |  |  |  |  |  |
| End-to-end/tiny |  |  |  |  |  |  |
| End-to-end/typical |  |  |  |  |  |  |
| End-to-end/real-world |  |  |  |  |  |  |
| FE end-to-end/typical |  |  |  |  |  |  |
| Invalid/early error |  |  |  |  |  |  |
| Invalid/late error |  |  |  |  |  |  |
| SLL fail + LL success |  |  |  |  |  |  |

最终结论必须同时附上：测试结果、语义差分摘要、benchmark 环境、原始结果文件位置，以及任何未解决的回退或差异。
