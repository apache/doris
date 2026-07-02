# CDC Stream Include Delete Sign Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 为 `cdc_stream` 增加默认关闭的 `include_delete_sign` 参数，开启后在 TVF 输出 schema 末尾暴露 `__DORIS_DELETE_SIGN__`，并验证 MySQL CDC DELETE 能写入 Unique Key MOW 表。

**Architecture:** 参数解析和 schema 扩展全部封装在 `CdcStreamTableValuedFunction` 中，不修改 CDC client、sink 绑定或 job plan rewrite。单元测试通过 mock JDBC schema 验证参数和输出列；回归测试使用显式目标列写入 delete sign。

**Tech Stack:** Java 17、JUnit、Mockito、Doris Groovy regression framework、MySQL CDC。

## Global Constraints

- `include_delete_sign` 默认值必须为 `false`。
- 参数只接受大小写不敏感的 `true` 或 `false`，非法值在分析阶段失败。
- 不新增持久化字段，不修改 FE image、edit log 或 FE-BE 协议。
- 代码注释使用英文。
- 回归测试使用 Awaitility 轮询，不使用固定时长 sleep。

---

### Task 1: 参数校验与 TVF 输出 schema

**Files:**
- Create: `fe/fe-core/src/test/java/org/apache/doris/tablefunction/CdcStreamTableValuedFunctionTest.java`
- Modify: `fe/fe-core/src/main/java/org/apache/doris/tablefunction/CdcStreamTableValuedFunction.java:45-221`

**Interfaces:**
- Consumes: TVF properties 中的字符串参数 `include_delete_sign`。
- Produces: `public static final String INCLUDE_DELETE_SIGN = "include_delete_sign"`；开启时 `getTableColumns()` 末尾返回非空 `TINYINT` 类型的 `Column.DELETE_SIGN`。

- [ ] **Step 1: 编写失败的 FE 单元测试**

新增测试，mock `StreamingJobUtils.getJdbcClient()` 和 `getRemoteDbName()`，分别断言：默认 schema 只有业务列；参数为 `true` 时末尾追加 `Column.DELETE_SIGN`；非法参数 `invalid` 抛出包含参数名的 `AnalysisException`。

- [ ] **Step 2: 运行测试并确认 RED**

Run: `source ~/.zprofile17 && ./run-fe-ut.sh --run org.apache.doris.tablefunction.CdcStreamTableValuedFunctionTest`

Expected: FAIL，因为 `include_delete_sign` 尚未校验且 schema 未追加 delete sign。

- [ ] **Step 3: 实现最小生产代码**

在 `validate()` 中复用 `validateBooleanIfPresent()` 校验参数。在 `getTableColumns()` 中复制 JDBC 返回列，仅当 `Boolean.parseBoolean(processedParams.getOrDefault(INCLUDE_DELETE_SIGN, "false"))` 为真时追加：

```java
new Column(Column.DELETE_SIGN, PrimitiveType.TINYINT, false)
```

- [ ] **Step 4: 运行单元测试并确认 GREEN**

Run: `source ~/.zprofile17 && ./run-fe-ut.sh --run org.apache.doris.tablefunction.CdcStreamTableValuedFunctionTest`

Expected: PASS，全部新增测试通过。

### Task 2: MySQL 和 PostgreSQL streaming job DML 回归

**Files:**
- Modify: `regression-test/suites/job_p0/streaming_job/cdc/tvf/test_streaming_job_cdc_stream_mysql.groovy`
- Modify: `regression-test/suites/job_p0/streaming_job/cdc/tvf/test_streaming_job_cdc_stream_postgres.groovy`

**Interfaces:**
- Consumes: MySQL 和 PostgreSQL `cdc_stream(..., "include_delete_sign" = "true")` 输出的 delete sign。
- Produces: 显式写入 `(name, age, __DORIS_DELETE_SIGN__)` 的 Unique Key MOW streaming job。

- [ ] **Step 1: 将现有 MySQL 和 PostgreSQL 用例目标表改为 Unique Key MOW，并显式绑定 delete sign**

保留 snapshot 和 incremental INSERT 覆盖；在 TVF 参数中开启 `include_delete_sign`，SELECT 和目标列都包含 delete sign。

- [ ] **Step 2: 增加上游 UPDATE、DELETE 及结果轮询**

分别更新 MySQL 和 PostgreSQL 的 `C1`，再删除 `D1`；通过 Awaitility 轮询 Doris，直到更新值可见且删除行不可见。

- [ ] **Step 3: 检查回归用例结构**

Run: `git diff --check -- regression-test/suites/job_p0/streaming_job/cdc/tvf/`

Expected: 无 whitespace 错误。根据用户要求，本次不运行回归测试。

### Task 3: 风格、自审与提交

**Files:**
- Verify all modified Java/Groovy/design/plan files.

**Interfaces:**
- Consumes: Task 1 和 Task 2 的实现。
- Produces: 可审查的一行 commit。

- [ ] **Step 1: 运行 FE Checkstyle/构建验证**

Run: `source ~/.zprofile17 && ./build.sh --fe`

Expected: PASS，无 Checkstyle 错误。

- [ ] **Step 2: 检查 diff 和测试规范**

Run: `git diff --check && git status --short && git diff --stat master...HEAD`

Expected: 无 whitespace 错误，只包含当前功能相关文件。

- [ ] **Step 3: 提交实现**

```bash
git add fe/fe-core/src/main/java/org/apache/doris/tablefunction/CdcStreamTableValuedFunction.java \
  fe/fe-core/src/test/java/org/apache/doris/tablefunction/CdcStreamTableValuedFunctionTest.java \
  regression-test/suites/job_p0/streaming_job/cdc/tvf/test_streaming_job_cdc_stream_mysql.groovy \
  regression-test/suites/job_p0/streaming_job/cdc/tvf/test_streaming_job_cdc_stream_postgres.groovy \
  docs/superpowers/plans/2026-07-02-cdc-stream-include-delete-sign.md
git commit -m "[fix](streaming-job) Support delete sign in CDC stream TVF"
```
