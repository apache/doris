# AGENTS.md — Kinesis Routine Load（PR #61325 测试阶段）

本指南用于 PR `#61325`（Kinesis Routine Load）的测试阶段。
当前实现基本完成，后续工作重点应放在可重复验证和失败排查上。

## 范围

- 主要测试目录：`regression-test/suites/load_p0/kinesis_routine_load/`
- 相关运行时/排查代码：
  - FE：`fe/fe-core/src/main/java/org/apache/doris/load/routineload/`
  - FE 测试：`fe/fe-core/src/test/java/org/apache/doris/load/routineload/KinesisRoutineLoadJobTest.java`
  - BE：`be/src/load/routine_load/`、`be/src/io/`
- 不要先全仓搜索，从上述路径开始定位。

## 当前目标

checklist被所有case覆盖:

1. 基础功能
   - 创建routineload : test_kinesis_routine_load_property
   - 启动/暂停/恢复/停止 : test_kinesis_routine_load_pause_resume
   - 正常消费单分区/分片 : test_kinesis_routine_load
   - 正常消费多分区/分片 : test_kinesis_routine_load
   - 断点续跑 : test_kinesis_routine_load_restart_be, test_kinesis_routine_load_restart_fe
2. 进度追踪
   - 进度更新 : test_kinesis_show_routine_load
   - FE/BE重启后的进度恢复 : test_kinesis_routine_load_restart_be, test_kinesis_routine_load_restart_fe
3. 调度逻辑
   - 有未消费数据时立即调度 : test_kinesis_routine_load
   - 已消费完但有新数据到来 : test_kinesis_routine_load
   - 无数据时空转/轮询行为 : test_kinesis_routine_load
   - 多 task 并发调度 : test_kinesis_routine_load
   - 调度去重/防抖/重复消费风险 : test_kinesis_routine_load
4. 分片/分区变更
   - shard split : test_kinesis_routine_load_shard_change
   - shard merge : test_kinesis_routine_load_shard_change
   - parent/child shard 关系处理 : test_kinesis_routine_load_shard_change
   - 新旧 shard 进度继承 : test_kinesis_routine_load_shard_change
   - 变更期间数据不丢不重 : test_kinesis_routine_load_shard_change
5. 可观测性
   - UI/SHOW PROC/SHOW ROUTINE LOAD 展示 : test_kinesis_show_routine_load

## Case 编写范式

在单个 case 文件中需要覆盖多个测试点时，使用如下分段结构：

```groovy
// test1 : 描述测试内容
try {

} finally {

}

// test2 : 描述测试内容
try {

} finally {

}

...
```

约定：
- 每个测试点都必须在对应 `finally` 中处理本测试点相关清理，保证失败时也能回收资源。
- 对于跨测试点复用资源，失败路径要有兜底清理，避免泄漏到后续用例。

## 排查清单

用例失败时按以下顺序检查：

1. 回归框架输出：
   - `output/regression-test/log/`
2. FE 日志：
   - `/mnt/disk2/huangruixin/apache/doris/output/fe/log/fe.log`
   - 如需历史日志，查看同目录下滚动文件（如 `fe.log.*`）
3. BE 日志：
   - `/mnt/disk2/huangruixin/apache/doris/output/be/log/be.INFO`
   - `/mnt/disk2/huangruixin/apache/doris/output/be/log/be.INFO.log.xxx`（滚动日志）
   - 如需补充可查看同目录下 `be.WARNING`、`be.out`
4. SQL 状态：
   - `SHOW ROUTINE LOAD;`
   - `SHOW ROUTINE LOAD TASK WHERE JobName="<job_name>";`

## 约束

- 改动必须聚焦在 Kinesis routine load 范围内。
- 新加case需要参考test_kinesis_routine_load
- 测试收敛阶段不要修改无关 suite/模块。
- 不要提交本地环境改动（`conf/`、本地凭据、临时调试配置）。
- 保持测试清理逻辑完整（`finally` 中应停止 job、删除 stream、删除表）。
- 添加UT需要向我确认并且描述UT的测试点
- 不要跑任何测试
