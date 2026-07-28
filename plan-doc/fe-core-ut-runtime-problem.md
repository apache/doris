# 问题记录 — fe-core 全量单测跑一次要 3.5 小时，其中一半以上不在跑测试

> **性质**：问题记录 + 初步结论，**不是**执行方案。
> **处置**：用户 2026-07-28 明确 —— **在另外的分支单独处理**，不并入 catalog-spi 主线，
> 也不属于 [`fecore-property-cleanup`](./fecore-property-cleanup/) 任务空间。
> **数据来源**：`fecore-property-cleanup` 的 FPC-04 验证跑（改 `ExternalCatalog` 基类 ⇒ 按纪律必须跑全量）。
> **⚠️ 数据是运行中快照**（跑到 3:20:08 / 1111 个测试类时采集），非最终值；量级结论不受影响。

---

## 1. 触发场景

改动落在 `ExternalCatalog`（**每个 catalog 都继承的基类**），窄 `-Dtest` 列表覆盖不到间接依赖基类
行为的测试，所以按纪律必须跑全量：

```bash
mvn -f <repo>/fe/pom.xml -pl fe-core -am test \
    -Dcheckstyle.skip=true -DfailIfNoTests=false \
    -Dmaven.build.cache.enabled=false --fail-at-end
```

结果：**跑了 3 小时 20 分还没结束**，而真正相关的改动只有 135 行纯删除。

---

## 2. 实测数据

### 2.1 总账（快照 @ 3:20:08）

| | 秒 | 占比 |
|---|---|---|
| 主 maven 已运行 | 11841 | 100% |
| ├─ 测试真正执行（各类 `Time elapsed` 之和） | 5550 | **47%** |
| └─ 差额：编译 + JVM fork + reactor 开销 | 6291 | **53%** |

**一半以上的时间不在跑测试。**

### 2.2 按包分（测试执行时间，非总耗时）

| 包 | 类数 | 测试执行 | fork 开销估算¹ | 合计估算 |
|---|---|---|---|---|
| `nereids` | 545 | 4023s | ~2998s | ~7021s |
| `datasource` | 114 | 304s | ~627s | ~931s |
| `common` | 73 | 236s | ~402s | ~637s |
| `qe` | 43 | 247s | ~236s | ~483s |
| `statistics` | 24 | 167s | ~132s | ~299s |
| `mtmv` | 18 | 149s | ~99s | ~248s |
| `persist` | 37 | 45s | ~204s | ~248s |
| `alter` | 13 | 111s | ~72s | ~182s |

¹ 按下文估算的 **~5.5s/类** 乘类数，是估算不是实测，**别当精确值引用**。

**`nereids` 一个包占了测试执行时间的 72%**，而它与 catalog/storage 改动毫无关系。
**`persist` 是 fork 开销最刺眼的例子**：45s 的测试背了约 204s 的 JVM 启动，**开销是测试本身的 4.5 倍**。

### 2.3 最慢的 12 个测试类

```
 237.8s  nereids.jobs.joinorder.joinhint.DistributeHintTest        ← 一个类 4 分钟
  64.8s  nereids.jobs.joinorder.hypergraphv2.OtherJoinTest
  54.3s  nereids.jobs.joinorder.hypergraphv2.GraphSimplifierConsistencyTest
  50.4s  cluster.DecommissionBackendTest
  45.0s  statistics.CacheTest
  38.6s  journal.bdbje.BDBEnvironmentTest
  33.7s  nereids.mv.PreMaterializedViewRewriterTest
  31.4s  common.profile.ProfileManagerTest
  28.0s  nereids.trees.plans.commands.CreateResourceCommandTest
  27.3s  alter.SchemaChangeHandlerTest
  25.8s  nereids.memo.StructInfoMapTest
  24.9s  mtmv.MTMVPlanUtilTest
```

### 2.4 测试类的粒度分布

```
类数=1110  用例数=8153  平均每类 7.3 个用例
其中「只有 1-2 个用例」的类 = 368 个（33%）
```

⇒ **三分之一的测试类，为了 1-2 个用例付一次完整 JVM 启动。**

---

## 3. 根因（**有证据，不是推测**）

### 3.1 哪些测试 fork JVM？—— **全部，无一例外**

`fe/fe-core/pom.xml:837-849`：

```xml
<artifactId>maven-surefire-plugin</artifactId>
<configuration>
    <!-->set larger, eg, 3, to reduce the time or running FE unit tests<-->
    <forkCount>${fe_ut_parallel}</forkCount>
    <!-->not reuse forked jvm, so that each unit test will run in separate jvm. to avoid singleton conflict<-->
    <reuseForks>false</reuseForks>
    ...
    <argLine>-Xmx1024m ...</argLine>
</configuration>
```

`reuseForks=false` ⇒ **每一个测试类都起一个全新 JVM**。这不是某个子集的问题，是全局配置。
注释写明了动机：**避免单例冲突**（Doris FE 大量 `Env` / `Catalog` 之类的进程级单例）。
⇒ **这个设定有正当理由，不能简单地改成 `true`。**

### 3.2 并发度是 1，而机器有 16 核

- `fe/fe-core/pom.xml:36` → `<fe_ut_parallel>1</fe_ut_parallel>`
- 唯一的覆盖入口是 profile `ut_parallel`（`fe-core/pom.xml:50-61`），靠**环境变量** `FE_UT_PARALLEL` 激活
- 本次运行 **`FE_UT_PARALLEL` 未设置** ⇒ `forkCount=1`
- 机器：**16 核 / 123G 内存**，每个 fork `-Xmx1024m`

⇒ **15 个核在空转。** 而且 pom 里那句注释 `set larger, eg, 3, to reduce the time` 说明
**这个逃生舱是已知的，只是没人用**。

### 3.3 单类 JVM 启动开销估算

```
(11841 总 − ~200 编译² − 5550 测试执行) / 1111 类 ≈ 5.5 s/类
```

² 编译时间用先前独立跑的全反应堆 `test-compile`（136s）做同量级估计，**未实测本次 `-am` 的编译耗时**。

按 1225 个测试类算，**纯 JVM 启停约 112 分钟**。

---

## 4. 初步结论（**未验证，供另开分支时参考**）

三个瓶颈性质完全不同，**不要混为一谈**：

| # | 瓶颈 | 性质 | 初步方向 |
|---|---|---|---|
| A | `forkCount=1`，15 核空转 | **配置问题，最易改** | 设 `FE_UT_PARALLEL`（pom 注释建议 3）。**须先验证并发下的稳定性**：`reuseForks=false` 保证了类间隔离，但并发 fork 仍可能撞共享外部资源（端口、BDB 目录、临时文件、`Env` 落盘路径）。**这是本项最大风险，不是改个数字就完事。** |
| B | 每类 5.5s JVM 启动 × 1225 类 | **结构性，动机正当** | 不建议动 `reuseForks`。可考虑的是**减少类数**：33% 的类只有 1-2 个用例，合并同源小类能直接砍掉对应的 fork 次数。工作量大、收益线性。 |
| C | `nereids` 占测试时间 72% | **与 catalog 无关的重型套件** | 与本条线无关，另议。 |

### 一条独立的流程建议（**需要用户拍板，我没有擅自改纪律**）

给**删除型/基类改动**定义一个「相关片区快速通道」：

```
datasource + connector + filesystem + persist  ≈ 260 个类
```

实测这几片的测试执行时间合计**只有约 6 分钟**（fork 开销另计）。
日常迭代跑它，**全量只在合入前跑一次**。

⚠️ **这条建议本身有风险**，正是 FPC-04 这类改动暴露的：改基类时，「相关片区」的边界很难先验判定
—— `ExternalCatalog` 的方法可能被任何包间接依赖。所以快速通道**只能用于日常迭代反馈，
不能替代合入前的全量**。

---

## 5. 未验证 / 明确不知道

- **编译耗时未实测**（§3.3 的 ~200s 是同量级估计），所以 5.5s/类 是估算。
  真要动手，第一步应该是**实测拆分**（`-o` 离线跑、分别计时 compile 与 test 阶段）。
- **`FE_UT_PARALLEL>1` 下的稳定性完全没验过。** 并发 fork 是否撞端口/BDB 目录/临时路径未知。
- 本次数据是**运行中快照**（3:20:08 / 1111 类），maven 自报的 `Total time` 与最终
  `Tests run / Failures / Errors / Skipped` 汇总行**尚未产出**。
- **只测了这一台机器**（16 核 / 123G）。CI 机器的核数与该配置的交互未知。
- 本次运行中另有其他 worktree 的 maven 在同机跑，**可能有资源竞争**，未量化其影响。

---

## 6. 附：复现命令

```bash
S=<log>
# 总账
grep -oE 'Time elapsed: [0-9.]+ s' $S | grep -oE '[0-9.]+' | awk '{t+=$1} END {print int(t)" s"}'
# 按包
grep 'Tests run:.*in org.apache.doris' $S \
 | sed -E 's/.*Time elapsed: ([0-9.]+) s - in org\.apache\.doris\.([a-z0-9]+).*/\1 \2/' \
 | awk '{t[$2]+=$1; c[$2]++} END {for (p in t) printf "%8.0f s %4d 类 %s\n", t[p], c[p], p}' | sort -rn
# 最慢类
grep 'Tests run:.*in org.apache.doris' $S \
 | sed -E 's/.*Time elapsed: ([0-9.]+) s - in (.*)/\1 \2/' | sort -rn | head -20
# 小类占比
grep 'Tests run:.*in org.apache.doris' $S | grep -oE 'Tests run: [0-9]+' | grep -oE '[0-9]+' \
 | awk '{n++; s+=$1; if($1<=2) k++} END {printf "类=%d 用例=%d 仅1-2用例的类=%d (%.0f%%)\n", n, s, k, k*100/n}'
```
