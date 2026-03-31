# Doris Paimon Insert 功能说明（support_paimon_insert）

## 1. 功能概述

本次在 `support_paimon_insert` 分支合入了来自场内 `97e70a800ed20363ef2e8c73956f040f4d30507a` 的核心能力：
支持通过 Doris `INSERT INTO` 向 Paimon 外表写入数据，覆盖两条写入实现路径：

- JNI Writer 路径（Java 扩展写入）
- paimon-cpp Native Writer 路径（C++ 本地写入）

同时补齐了 FE/BE 从规划、下发、执行到提交的端到端链路。

## 2. 端到端链路

### 2.1 FE 侧

- Nereids 识别 `PaimonExternalCatalog` 目标表，绑定为 `UnboundPaimonTableSink` / `PhysicalPaimonTableSink`
- `InsertIntoTableCommand` 选择 `PaimonInsertExecutor`
- `PaimonTableSink` 将表位置、序列化表对象、列名、写入参数等下发到 BE
- 事务类型扩展为 `PAIMON`，由 `PaimonTransaction` 统一收集并提交 commit messages

### 2.2 BE 侧

- `vpaimon_table_sink` 根据参数选择 JNI Writer 或 Native Writer
- Writer 生成 commit payload 回传到 `RuntimeState`
- Fragment 上报时将 `paimon_commit_messages` 带回 FE

### 2.3 提交阶段

- FE Coordinator 收集 BE 回传的 `paimon_commit_messages`
- `PaimonTransaction` 反序列化并执行 Paimon commit

## 3. 可用 Session 参数

以下参数已在会话变量中接入（按功能分组）：

### 3.1 写入策略

- `enable_paimon_jni_writer`
- `enable_paimon_distributed_bucket_shuffle`
- `enable_paimon_adaptive_buffer_size`
- `paimon_writer_queue_size`

### 3.2 文件与缓冲

- `paimon_target_file_size`
- `paimon_write_buffer_size`

### 3.3 Spill 相关（JNI Writer）

- `enable_paimon_jni_spill`
- `paimon_spill_max_disk_size`
- `paimon_spill_sort_buffer_size`
- `paimon_spill_sort_threshold`
- `paimon_spill_compression`
- `paimon_global_memory_pool_size`

## 4. 优化点说明

### 4.1 自适应 Buffer

按 bucket 数动态调整写缓冲上限，避免 bucket 数较大时单桶缓冲过大导致内存放大。

### 4.2 Bucket Shuffle

通过 `DistributionSpecPaimonBucketShuffle` 和 `paimon_bucket_id` 函数，增强数据按 bucket 分布的稳定性，降低写入侧重分桶成本。

### 4.3 JNI 写入批处理

JNI Writer 采用 Arrow 批量传输并在 close 阶段统一 `prepareCommit`，减少跨语言调用开销。

### 4.4 Commit Message 聚合上报

BE 侧按 fragment 聚合 payload，FE 侧统一事务提交，保证多实例写入的一致提交。

## 5. 编译说明（按你提供的环境）

进入容器后在代码目录执行：

```bash
sudo docker exec -it lll_tob_sr_3320 /bin/bash
cd /root/osdoris/doris
bash build.sh
```

按模块编译可使用：

```bash
bash build.sh --fe
bash build.sh --be
```

## 6. 本次合入边界

本次仅保留 Paimon Insert 主功能及必要依赖；以下场内改造未纳入：

- unique mtmv 相关
- 场内鉴权相关
- 场内 build.sh 定制改造
- 其他与 Paimon Insert 主链路无关的第三方改造
