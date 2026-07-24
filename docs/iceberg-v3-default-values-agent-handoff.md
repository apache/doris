# Iceberg V3 Default Values 全权处理交接

> 状态日期：2026-07-24
>
> 当前 PR：[apache/doris#65851](https://github.com/apache/doris/pull/65851)
>
> 唯一工作目录：`/mnt/disk1/changyuwei/doris-worktrees/iceberg-v3-defaults`

## 1. 最终任务和不可变边界

接手 agent 对 Iceberg V3 default-value 相关工作全权负责。不要再把读取
`initial-default` 和写入时消费 `write-default` 拆成两个 PR：当前 PR #65851 必须同时完成
这两个执行阶段。

本文仍使用“读取阶段”和“写入阶段”描述实现顺序，但它们是同一个 PR 内的两个阶段，不是
两个独立 PR。

开始任何新的实现或测试修复前有两个强制前置条件：

1. 保护当前任务 WIP 和本地环境 overlay，然后执行一次基于**最新**
   `apache/master` 的 rebase；不能继续在当前旧 merge base 上开发。
2. rebase 完成后，从指定的 Doris Third Party automation release 下载 Linux x86_64
   预编译三方库，并用它替换当前 worktree 的 `thirdparty/installed`；不能从基础 checkout
   复制三方库，也不能直接沿用 rebase 前已有的安装目录。

除状态核对和保护现有改动外，rebase 是接手 agent 的第一个实际操作。具体命令见第 3.2
节；三方库命令见第 4.4 节。

本 PR 的范围如下：

1. 读取阶段：当旧数据文件没有某个字段 ID 时，File Scanner V1 和 V2 按 Iceberg
   `initial-default` 语义物化缺失字段。
2. 写入阶段：Doris 写 Iceberg 时，对省略列或显式 `DEFAULT` 使用当前目标 schema 中该
   字段的 `write-default`。
3. 支持 Doris 在本 PR 之前已经能够映射的全部 Iceberg 数据类型，包括这些类型组成的
   ARRAY、MAP 和 STRUCT；不要借本 PR 新增 Doris 尚不支持的 Iceberg 类型映射。
4. Parquet 和 ORC 都必须覆盖；File Scanner V1 与 V2 的读取实现和测试必须保持独立。
5. Spark-Iceberg Docker 负责生成真实的 Iceberg V3 元数据、旧 schema 数据文件以及读取
   校验数据，不允许手改 Iceberg metadata JSON。

以下内容仍不在本 PR：

- CREATE/ALTER 语法创建、修改或删除 Iceberg 默认值；
- 新的 Iceberg-to-Doris 类型映射；
- Iceberg 依赖升级；
- 与 default-value 无关的重构、CI 基础设施问题或回归用例改动；
- UPDATE 和 MERGE UPDATE 自动填充 `write-default`。

DDL 元数据创作是后续独立工作。对应的设计阶段仍记录在
[`docs/iceberg-v3-default-values-design.md`](iceberg-v3-default-values-design.md)。

## 2. 工作目录纪律

所有查看、编辑、编译、部署、测试、提交和推送操作都只能在下面的 worktree 中进行：

```text
/mnt/disk1/changyuwei/doris-worktrees/iceberg-v3-defaults
```

不要进入或借用基础 checkout `/mnt/disk1/changyuwei/doris` 中的源码、构建产物、依赖或
配置。进入 worktree 后先检查：

```bash
cd /mnt/disk1/changyuwei/doris-worktrees/iceberg-v3-defaults
test -f .worktree_initialized
test -d thirdparty/installed
git submodule status
```

截至本文状态日期：

- `.worktree_initialized` 已存在；
- `thirdparty/installed` 已存在，但 rebase 后仍必须按第 4.4 节从指定 URL 重新安装；
- 递归 submodule 已初始化；
- `be/build_ASAN` 和 `output/` 已存在；
- `/mnt/disk1` 约有 153 GiB 可用空间，使用率约 93%。运行全量构建前仍应重新检查空间。

如果未来重新创建 worktree 且 `.worktree_initialized` 不存在，必须遵循仓库
`AGENTS.md`：从新 worktree 内执行 `hooks/setup_worktree.sh`，设置
`ROOT_WORKSPACE_PATH`，随后重新验证上述三项。不要跳过初始化。

## 3. Git 和 PR 现状

### 3.1 当前基线

截至 2026-07-24 的已核对状态：

| 项目 | 值 |
| --- | --- |
| 本地分支 | `feature/iceberg-v3-defaults` |
| PR | `https://github.com/apache/doris/pull/65851` |
| PR 远端 head | `5dbdb411df39843eda96aec3463a5c0b7202bedb` |
| 当前代码基线 HEAD | `5dbdb411df39843eda96aec3463a5c0b7202bedb` |
| PR base OID | `9ec7f76d15efc2df544b8ba6c1afdee76fb9d8c4` |
| 已刷新 `apache/master` | `a765b36ebc2cb9ea882ae1bdcfb37fedb3a9599b` |
| merge base | `9ec7f76d15efc2df544b8ba6c1afdee76fb9d8c4` |
| 相对 `apache/master` | 本分支 1 个提交、master 侧 32 个提交 |
| GitHub merge 状态 | `CONFLICTING` |
| GitHub review 状态 | `CHANGES_REQUESTED` |

本文档本身可能位于后续的本地 documentation-only 提交中，因此接手后必须重新运行：

```bash
git rev-parse HEAD
git rev-parse apache/master
git merge-base HEAD apache/master
git rev-list --left-right --count HEAD...apache/master
gh pr view 65851 --repo apache/doris \
  --json headRefOid,baseRefOid,mergeable,reviewDecision,state,url
```

当前 SHA 只用于理解交接时的状态，不能作为 rebase 目标。必须重新 fetch 后以当时最新的
`apache/master` 为准。

### 3.2 强制前置：先 rebase 最新 `apache/master`

当前 worktree 同时有任务 WIP 和本地环境 overlay，不能直接 rebase。除了保护这些现有
改动，不允许先做新的功能实现或测试修复。建议使用两个按路径隔离的 stash，并立即记录
它们的不可变对象 SHA：

```bash
cd /mnt/disk1/changyuwei/doris-worktrees/iceberg-v3-defaults

git stash push -m "pr65851-task-wip-before-master-rebase" -- \
  be/src/format/table/iceberg_reader_mixin.h \
  be/src/format/table/iceberg_scan_semantics.h \
  be/src/format_v2/table/iceberg_reader.cpp \
  be/src/format_v2/table/iceberg_reader.h \
  be/test/format/table/iceberg/iceberg_reader_test.cpp \
  be/test/format_v2/table/iceberg_reader_test.cpp \
  fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergScanNode.java \
  fe/fe-core/src/test/java/org/apache/doris/datasource/iceberg/source/IcebergScanNodeTest.java
TASK_WIP_STASH=$(git rev-parse stash@{0})

git stash push -u -m "pr65851-local-env-before-master-rebase" -- \
  conf/be.conf \
  conf/fe.conf \
  docker/thirdparties/custom_settings.env \
  docker/thirdparties/docker-compose/iceberg/iceberg.env \
  docker/thirdparties/docker-compose/iceberg/iceberg.yaml.tpl \
  docker/thirdparties/run-thirdparties-docker.sh \
  regression-test/conf/regression-conf.groovy \
  tools/find_libjvm.sh \
  docker/thirdparties/docker-compose/iceberg/entrypoint.sh \
  docker/thirdparties/docker-compose/iceberg/scripts/entrypoint.sh
LOCAL_ENV_STASH=$(git rev-parse stash@{0})

printf 'TASK_WIP_STASH=%s\nLOCAL_ENV_STASH=%s\n' \
  "${TASK_WIP_STASH}" "${LOCAL_ENV_STASH}"
git status --short
```

`git status --short` 必须为空。把打印出的两个 SHA 记录到
`/mnt/disk1/changyuwei/tmp/pr65851-rebase-stashes.txt` 或 agent 的工作记录中；不要只记录
会变化的 `stash@{n}` 索引。

然后刷新并 rebase：

```bash
git fetch apache master
LATEST_APACHE_MASTER=$(git rev-parse apache/master)
printf 'LATEST_APACHE_MASTER=%s\n' "${LATEST_APACHE_MASTER}"

git rebase apache/master
git merge-base --is-ancestor "${LATEST_APACHE_MASTER}" HEAD
git rev-list --left-right --count HEAD...apache/master
```

rebase 冲突必须结合双方语义逐个解决，不能机械选择 ours/theirs。rebase 完成后，
`git merge-base --is-ancestor` 必须返回 0，`git rev-list` 的 master 侧计数必须为 0。

随后使用前面记录的对象 SHA 恢复 task WIP 和环境 overlay：

```bash
git stash apply "${TASK_WIP_STASH}"
git stash apply "${LOCAL_ENV_STASH}"
git status --short
```

如果切换了 shell，就把变量替换成已经记录的完整 stash 对象 SHA。恢复时发生的冲突也要
按最新 master 语义处理。确认 rebase、三方库、编译和测试都稳定前不要 drop 这两个 stash。

rebase 会重写本 PR 的提交 SHA。最终推送必须使用带**精确旧远端 head** 的
`--force-with-lease=<ref>:<expected-old-sha>`，禁止裸 `--force`；推送前重新查询 PR head，
确保没有覆盖其他人的新提交。

### 3.3 当前未提交的任务 WIP

以下 8 个文件属于本 PR 的 review 修复，必须保留：

```text
be/src/format/table/iceberg_reader_mixin.h
be/src/format/table/iceberg_scan_semantics.h
be/src/format_v2/table/iceberg_reader.cpp
be/src/format_v2/table/iceberg_reader.h
be/test/format/table/iceberg/iceberg_reader_test.cpp
be/test/format_v2/table/iceberg_reader_test.cpp
fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergScanNode.java
fe/fe-core/src/test/java/org/apache/doris/datasource/iceberg/source/IcebergScanNodeTest.java
```

这些 WIP 已经在本地修复最新一轮 review 的主要问题，详见第 7 节。接手 agent 不应恢复或
覆盖它们。

### 3.4 绝对不能提交的本地环境文件

下面的改动只用于这个 worktree 的编译、容器和端口隔离，不能 `git add`，不能出现在 PR：

```text
conf/be.conf
conf/fe.conf
docker/thirdparties/custom_settings.env
docker/thirdparties/docker-compose/iceberg/iceberg.env
docker/thirdparties/docker-compose/iceberg/iceberg.yaml.tpl
docker/thirdparties/run-thirdparties-docker.sh
regression-test/conf/regression-conf.groovy
tools/find_libjvm.sh
docker/thirdparties/docker-compose/iceberg/entrypoint.sh
docker/thirdparties/docker-compose/iceberg/scripts/entrypoint.sh
```

最后两个当前为 untracked 文件，同样是本地环境文件。每次提交前至少执行：

```bash
git status --short
git diff --cached --name-only
```

只能显式暂存任务文件，禁止使用 `git add -A` 或 `git add .`。

### 3.5 历史 stash

仓库中存在下面的历史 stash：

```text
bca52fa2459c2e7e15e26032e42b1d960d347db5
On support_iceberg_nested_sc_41: pre-iceberg-nested-master-pick dirty work
```

它包含比本 worktree 现有 overlay 更广的环境改动，还混有 demo、regression framework 等
无关文件。不要直接 `git stash apply stash@{0}`；stash 索引会变化，也不要依赖索引。
当前 worktree 已经有从中挑选出的所需环境配置。只有确认某个具体文件确实缺失时，才允许
通过上述不可变对象 SHA 查看并按路径提取。

## 4. 本地端口、Docker 和临时目录

### 4.1 FE/BE 端口

当前本地 overlay 使用以下不冲突端口：

| 服务 | 端口 |
| --- | ---: |
| FE HTTP | 55030 |
| FE RPC | 56020 |
| FE MySQL query | 56030 |
| FE edit log | 56010 |
| FE Arrow Flight SQL | 55070 |
| BE service | 56060 |
| BE webserver | 55040 |
| BE heartbeat | 56050 |
| BE brpc | 55060 |
| BE Arrow Flight SQL | 55050 |

FE 和 BE 的 `priority_networks` 当前都是 `172.20.32.136/32`。机器 IP 变化时应在本地
环境文件中调整，但仍不能提交。

`regression-test/conf/regression-conf.groovy` 当前对应：

```text
jdbc:mysql://127.0.0.1:56030/
FE thrift: 127.0.0.1:56020
FE HTTP:   127.0.0.1:55030
enableIcebergTest=true
```

### 4.2 Spark-Iceberg Docker

当前环境为：

| 项目 | 值 |
| --- | --- |
| Spark image | `apache/spark:4.0.0` |
| Iceberg Spark runtime | `iceberg-spark-runtime-4.0_2.13-1.10.1.jar` |
| Iceberg AWS bundle | `iceberg-aws-bundle-1.10.1.jar` |
| Iceberg REST fixture | `apache/iceberg-rest-fixture:1.10.0` |
| container prefix | `doris-defaults-` |
| Spark container | `doris-defaults-spark-iceberg` |
| Spark Thrift host port | 31000 |
| Iceberg REST host port | 38181 |
| MinIO API host port | 39001 |
| Docker subnet | `168.50.0.0/24` |

启动和停止命令从 worktree 执行：

```bash
docker/thirdparties/run-thirdparties-docker.sh -c iceberg
docker/thirdparties/run-thirdparties-docker.sh -c iceberg --stop
```

不要把生成后的 `iceberg.yaml`、entrypoint 脚本或本机端口改动提交。真实的 fixture
生成逻辑位于：

```text
docker/thirdparties/docker-compose/iceberg/scripts/java/
  CreateIcebergInitialDefaultFixtures.java
```

写入阶段可以重命名或扩展该 fixture，但必须继续通过官方 Iceberg Java API 创建 V3
metadata，不得手工改 metadata JSON。

### 4.3 临时目录硬约束

所有 TeamCity 下载、日志、artifact 和本地构建测试临时文件必须放在：

```text
/mnt/disk1/changyuwei/tmp
```

禁止下载到宿主机 `/tmp`。建议每个命令先设置：

```bash
mkdir -p /mnt/disk1/changyuwei/tmp/pr65851-local-tests
export TMPDIR=/mnt/disk1/changyuwei/tmp/pr65851-local-tests
export MAVEN_OPTS=-Xmx12g
```

TeamCity helper 必须这样调用：

```bash
TMPDIR=/mnt/disk1/changyuwei/tmp \
  bash /mnt/disk1/changyuwei/.claude/skills/teamcity/teamcity.sh <command>
```

不要使用 helper 的 `log-cat` 或 `diagnose-logs`，它们会默认使用宿主机 `/tmp`。如果需要
下载日志，显式给出 `/mnt/disk1/changyuwei/tmp/...` 目标路径。回归 fixture 容器内部的
`/tmp` 不属于这个限制。

现有日志包括：

```text
/mnt/disk1/changyuwei/tmp/pr65851-teamcity-1003306/build.log
/mnt/disk1/changyuwei/tmp/pr65851-fe-build-after-latest-master.log
/mnt/disk1/changyuwei/tmp/pr65851-be-four-suites-after-latest-master.log
/mnt/disk1/changyuwei/tmp/pr65851-iceberg-initial-defaults-regression-after-latest-master.log
/mnt/disk1/changyuwei/tmp/pr65851-check-format-after-latest-master.log
/mnt/disk1/changyuwei/tmp/pr65851-clang-tidy-after-latest-master.log
```

这些日志只代表各自记录时的代码状态，不是当前 WIP 的新鲜证明。

### 4.4 rebase 后必须重新安装预编译三方库

完成第 3.2 节的 rebase 后、第一次编译前，必须从下面这个指定 URL 下载三方库：

```text
https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-linux-x86_64.tar.xz
```

仓库脚本 `thirdparty/download-prebuild-thirdparty.sh master` 也会解析到这个 URL，但该脚本
只下载文件，不负责解压。为确保下载位置和安装过程明确，直接执行下面的完整流程：

```bash
cd /mnt/disk1/changyuwei/doris-worktrees/iceberg-v3-defaults

THIRDPARTY_URL='https://github.com/apache/doris-thirdparty/releases/download/automation/doris-thirdparty-prebuilt-linux-x86_64.tar.xz'
THIRDPARTY_ARCHIVE='/mnt/disk1/changyuwei/tmp/doris-thirdparty-prebuilt-linux-x86_64.tar.xz'
THIRDPARTY_PART="${THIRDPARTY_ARCHIVE}.part"
THIRDPARTY_BACKUP="/mnt/disk1/changyuwei/tmp/pr65851-thirdparty-installed-before-prebuilt-$(date +%Y%m%d-%H%M%S)"

curl -fL --retry 5 --retry-delay 3 \
  -o "${THIRDPARTY_PART}" "${THIRDPARTY_URL}"
mv "${THIRDPARTY_PART}" "${THIRDPARTY_ARCHIVE}"

tar -tJf "${THIRDPARTY_ARCHIVE}" | sed -n '1,20p'
test -d thirdparty/installed
mv thirdparty/installed "${THIRDPARTY_BACKUP}"
tar -xJf "${THIRDPARTY_ARCHIVE}" -C thirdparty

test -d thirdparty/installed/include
test -f thirdparty/installed/lib/hadoop_hdfs/native/libhdfs.a
```

automation asset 是可更新的 release 资产，所以每次基于最新 master rebase 后都要重新
下载，不能无条件复用旧缓存。压缩包顶层应为 `installed/`；`tar -tJf` 的检查结果不符合
这个结构时立即停止，不能把未知目录覆盖进 worktree。

原 `thirdparty/installed` 被移动到 `${THIRDPARTY_BACKUP}`，位于
`/mnt/disk1/changyuwei/tmp`，不是 Git 任务文件。新目录验证和完整编译通过前保留备份。
不要把 archive、backup 或 `thirdparty/installed` 加入提交。

## 5. 文档和源码入口

### 5.1 文档

| 文件 | 作用 |
| --- | --- |
| `docs/iceberg-v3-default-values-agent-handoff.md` | 本文，全权处理入口和实时交接 |
| `docs/iceberg-v3-default-values-design.md` | 语义、读取实现、写入范围和后续 DDL 设计 |
| `.github/workflows/code-review-runner.yml` | 最终完整 PR 自审方法 |
| `.github/PULL_REQUEST_TEMPLATE.md` | PR/commit 描述格式 |

修改实现边界后，应同步更新前两份文档，不能只更新 PR 描述。

### 5.2 读取阶段主要代码

```text
gensrc/thrift/ExternalTableSchema.thrift

fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergUtils.java
fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergScanNode.java
fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalUtil.java
fe/fe-core/src/main/java/org/apache/doris/datasource/FileScanNode.java

be/src/format/table/iceberg_default_value.h
be/src/format/table/iceberg_reader.cpp
be/src/format/table/iceberg_reader_mixin.h
be/src/format/table/table_schema_change_helper.*
be/src/format/parquet/vparquet_column_reader.*
be/src/format/orc/vorc_reader.*

be/src/format_v2/column_data.h
be/src/format_v2/column_mapper.*
be/src/format_v2/table/iceberg_reader.*
be/src/format_v2/table_reader.*
```

### 5.3 写入阶段需要重点检查的代码

```text
fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/insert/InsertUtils.java
fe/fe-core/src/main/java/org/apache/doris/nereids/rules/analysis/BindSink.java
fe/fe-core/src/main/java/org/apache/doris/nereids/rules/expression/rules/RewriteDefaultExpression.java
fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/IcebergMergeCommand.java
fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/insert/IcebergInsertCommandContext.java

fe/fe-core/src/main/java/org/apache/doris/nereids/analyzer/UnboundIcebergTableSink.java
fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/logical/LogicalIcebergTableSink.java
fe/fe-core/src/main/java/org/apache/doris/nereids/rules/implementation/
  LogicalIcebergTableSinkToPhysicalIcebergTableSink.java
fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/physical/PhysicalIcebergTableSink.java
fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java

fe/fe-core/src/main/java/org/apache/doris/planner/IcebergTableSink.java
fe/fe-core/src/main/java/org/apache/doris/planner/IcebergMergeSink.java
fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergTransaction.java

gensrc/thrift/DataSinks.thrift
be/src/exec/sink/writer/iceberg/viceberg_table_writer.*
be/src/exec/sink/viceberg_merge_sink.*
```

## 6. 必须保持的语义

### 6.1 Iceberg API 返回的是“类型 + 已类型化值”

FE 从 Iceberg API 获得的不是一段无类型 SQL 字符串：

- 字段类型来自 `Types.NestedField.type()`；
- 读取默认值来自 `Types.NestedField.initialDefault()`；
- 写入默认值来自 `Types.NestedField.writeDefault()`。

当前读取阶段的 `TField.initial_default_value` 使用 `string`，只是 FE 到 BE 的 Thrift
传输编码，不代表 Iceberg API 返回字符串。FE 必须先按 Iceberg 类型规范化，BE 再结合同一
递归 `TField` 类型元数据构造 Doris typed constant。

当前读取阶段确实有 additive Thrift 改动：

```text
TField.7 initial_default_value
TField.8 initial_default_value_is_base64
TField.9 name_mapping_is_authoritative
```

字段 7 不能改成某个单一具体 Thrift 类型，因为它需要承载 Doris 已支持的所有 Iceberg
primitive 和 complex 类型；类型本身已经由递归 `TField.type/nestedField` 描述。UUID、
FIXED、BINARY 使用 Base64，复杂值使用 Iceberg `SingleValueParser` 的 single-value
JSON。

LIST/MAP 的递归子 `TField` 只能描述 element/key/value 的类型和字段级元数据，不能表达
某个实际容器有多少元素、元素顺序或每个位置的值，所以实际容器值必须保留在父字段的
single-value JSON 中。`build_json_array_default` 和 `build_json_map_default` 上已经有
解释该边界的注释，不要删除。

### 6.2 读取 `initial-default`

必须保持：

1. 物理字段存在时，读取真实值；物理 NULL 仍是 NULL。
2. 物理字段缺失且有 `initial-default` 时，物化 typed constant。
3. 缺失的 optional 字段没有 default 时为 NULL。
4. 缺失的 required 字段没有 default 时必须报错。
5. 父 STRUCT、LIST element 或 MAP value 为 NULL 时，不得用子字段 default “复活”父值。
6. 子 default 只在父容器实际存在时递归应用。
7. 普通当前表读取使用当前 `table.schema()`；time travel/ref 使用查询绑定的历史 schema。
8. equality-delete key 必须按字段 ID 和必要的历史 schema 解析。
9. File Scanner V1/V2 独立实现；不能让一个 scanner 委托另一个 scanner 物化默认值。
10. 新语义必须使用 `iceberg_scan_semantics_version = 2`。master 在本 PR 之前已经占用
    version 1，不能改变旧 FE/BE 组合的 version 1 行为。

### 6.3 写入 `write-default`

必须保持：

1. 只有省略列或显式 `DEFAULT` 使用 `write-default`。
2. 显式 NULL 和显式值原样保留。
3. required 字段既未提供值又没有 `write-default` 时，在 FE 分析阶段报错。
4. optional 字段既未提供值又没有 default 时填 NULL。
5. UPDATE 和 MERGE UPDATE 不自动应用写默认值。
6. MERGE `NOT MATCHED INSERT` 与普通 INSERT 使用同一套 Iceberg 专用解析规则。
7. analyzer 使用的 schema、默认值来源、sink 的 `schema_json` 和事务开始时确认的 schema
   必须一致；schema skew 必须在数据下发前失败，不能混用 schema A 的默认值和 schema B
   的 writer schema。
8. branch 写入必须固定 branch 对应的 schema，不能无条件使用 main 的
   `table.schema()`。
9. rewrite/compaction 不能因为新增 write-default 而向已有行注入默认值。

## 7. 读取阶段现状

### 7.1 已提交到 PR head 的实现

远端 head `5dbdb411...` 已经完成：

- FE 读取 typed `NestedField.initialDefault()`；
- primitive 使用类型感知编码，complex 使用 `SingleValueParser` JSON；
- UUID/FIXED/BINARY 通过 Base64 无损传输；
- recursive `TField` 元数据，不使用通用 `Column.defaultValue`；
- V1 的 Parquet/ORC missing-column materialization；
- V2 的 `ColumnDefinition`、`TableColumnMapper`、`TableReader` materialization；
- required/optional、父 NULL、物理 NULL、嵌套 child default；
- equality-delete key 和必要历史 schema；
- 普通表、time travel/ref schema 选择；
- Spark-Iceberg Docker 生成 Parquet/ORC fixture；
- 四条强制路径的 regression test；
- Doris 已支持类型的 primitive 和 complex 覆盖。

`initial-default` 和 `write-default` fixture 值故意不同，用来证明读取旧文件时没有误用当前
`write-default`。

### 7.2 最新 review 的四个 P1

远端 committed head 有四个已确认问题：

1. `snapshot.deleteManifests(io)` preflight 在
   `preExecutionAuthenticator` 之外执行。
2. rolling-upgrade gate 没有覆盖“物理上可能缺失、required 且没有
   `initial-default`”的字段。
3. equality-delete schema carrier 无差别加入所有 dropped primitive，导致与查询无关的
   dropped `TIMESTAMP_NANO` 也可能让 `parseField()` 失败。
4. 只要有 delete manifest 就触发 equality gate，导致只有 position delete 的 scan 被误挡。

### 7.3 当前本地 WIP 已做的修复

当前未提交 FE WIP：

- semantics version 从 1 调整为 2；
- 在认证上下文内，通过 Doris manifest cache 只加载
  `FileContent.EQUALITY_DELETES`；
- 即使 equality-delete 文件 record count 为 0，也收集 equality field IDs；
- schema carrier 只加入 equality delete 实际引用的历史字段；
- required/missing rolling gate 根据 metadata schema history 和 pruned
  slot/access-path 判断；
- LIST/MAP wrapper field ID 不误判，实际嵌套 STRUCT child 仍参与 gate；
- position-delete-only scan 返回空 equality field ID 集；
- 增加相应 FE 单测。

当前未提交 BE WIP：

- `iceberg_scan_semantics.h` 新增 version 2；
- V1 保留 master 的 version 1 兼容行为，version 2 才开启本 PR 的递归/default-required
  语义；
- V2 的 missing-required 拒绝和严格 equality-history 校验只在 version 2 开启；
- 大部分本 PR 新增测试已切到 version 2；
- 增加一个 version 1 compatibility test。

### 7.4 接手后先补完的读取 WIP

至少检查
`be/test/format_v2/table/iceberg_reader_test.cpp` 中以下测试，在
`make_local_parquet_scan_params()` 后显式设置 version 2：

```text
IcebergEqualityDeleteUsesDroppedFieldHistoricalInitialDefault
IcebergEqualityDeleteRejectsDroppedFieldWithoutSchemaMetadata
```

同时检查本 PR 新增 helper
`expect_idless_equality_key_uses_delete_file_name` 是否也应显式设置 version 2。原则是：
所有证明本 PR 新语义的测试都明确使用 version 2；只保留专门的 rolling-upgrade
compatibility test 使用 version 1。

完成后先跑读取阶段的 fresh FE/BE 测试，再开始 write-default。不要把旧日志当作当前 WIP
通过。

### 7.5 当前验证证据

当前 WIP 上最新鲜的验证只有：

```bash
TMPDIR=/mnt/disk1/changyuwei/tmp/pr65851-local-tests \
MAVEN_OPTS=-Xmx12g \
./run-fe-ut.sh --run \
  org.apache.doris.datasource.iceberg.source.IcebergScanNodeTest
```

结果：41/41 tests passed，BUILD SUCCESS，checkstyle passed。

下面是较早 committed/older 状态的证据，修改 WIP 后必须重跑：

- BE 四个 suite 共 202 tests 通过：
  `IcebergReaderTest` 27、`MockTableSchemaChangeHelper` 25、
  `IcebergV2ReaderTest` 67、`TableReaderTest` 83；
- FE build 通过；
- `test_iceberg_initial_defaults` regression：1 suite，0 failed；
- format check 通过；
- clang-tidy 曾执行，但本机 LLVM resource 出现 `stddef.h not found`，并夹杂现有全局
  warning，不能宣称 clang-tidy 已通过。

## 8. 写入阶段现状和推荐实现

### 8.1 Doris 当前写路径

当前 Doris 的通用默认值路径依赖 `Column.getDefaultValue()` /
`getDefaultValueSql()`：

- `InsertUtils.generateDefaultExpression(Column)` 处理 VALUES；
- `BindSink.getColumnToOutput()` 处理省略列和显式 DEFAULT；
- `RewriteDefaultExpression` 处理 `DEFAULT(column)`；
- `IcebergMergeCommand.buildInsertProjection()` 处理 MERGE NOT MATCHED INSERT。

Iceberg schema 转换当前有意不设置通用 `Column.defaultValue`。不要为了省代码把
Iceberg `write-default` 填进通用 `Column.defaultValue`，否则很容易把 read default 和
write default 混淆，并污染非 Iceberg 路径。

`IcebergTableSink.bindDataSink()` 当前会再次调用
`targetTable.getIcebergTable()`，然后把当时的 `SchemaParser.toJson(schema)` 写入
`TIcebergTableSink.schema_json`。`IcebergMergeSink` 有同样的 schema JSON carrier。

BE 的 `VIcebergTableWriter` 已经解析 `schema_json`，并要求 output expression 数量与
schema columns 数量完全相等。因此正常方案是 FE 为所有目标列生成完整输出表达式，BE
不再补 default。

### 8.2 推荐的数据流

```text
认证上下文内取得目标/branch Iceberg Schema
                  |
                  v
创建 statement-scoped IcebergWriteSchemaContext
  - schema ID / format version / schema JSON
  - field ID -> typed write-default expression
                  |
                  v
BindSink / InsertUtils / MERGE INSERT 生成完整列投影
  - omitted / DEFAULT -> write-default
  - explicit NULL/value -> 原值
                  |
                  v
Logical sink -> Physical sink -> PhysicalPlanTranslator
                  |
                  v
IcebergTableSink / IcebergMergeSink 使用同一个 pinned schema JSON
                  |
                  v
IcebergTransaction.beginInsert/beginMerge 在下发数据前校验 schema ID
                  |
                  v
BE VIcebergTableWriter 接收完整表达式，不负责选择 default
```

推荐实现细节：

1. 在认证上下文中只获取一次目标 Iceberg schema。创建 immutable、
   statement-scoped、field-ID keyed 的 `IcebergWriteSchemaContext`，至少包含 schema ID、
   format version、schema JSON 和 typed `write-default` expressions。
2. branch 写入从指定 branch/ref 固定 schema；不要先按 main 解析 default、事务开始时再
   切 branch。
3. 新增 Iceberg 专用 default resolver，只在 Iceberg sink 路径使用。保持
   `IcebergUtils.parseField()` 生成的通用 `Column.defaultValue` 为空。
4. 从 Iceberg typed value 直接构造 Nereids literal tree，不要先拼 SQL 字符串再交给 SQL
   parser。可以复用读取阶段的类型规则，但必须用独立 API 读取 `writeDefault()`。
5. primitive、decimal、date、timestamp/timestamptz、UUID/FIXED/BINARY 必须无损；
   ARRAY/MAP/STRUCT 递归构造 Nereids 的 `ArrayLiteral`、`MapLiteral`、`StructLiteral`
   等已有 literal。
6. 让 context 穿过 `UnboundIcebergTableSink`、`LogicalIcebergTableSink`、
   implementation rule、`PhysicalIcebergTableSink` 和 `PhysicalPlanTranslator` 的所有
   constructor/copy/withChildren 路径。MERGE sink 做等价传递。
7. `IcebergTableSink` / `IcebergMergeSink` 必须发送 context 中的精确 pinned
   `schema_json`，不能临时再次取一个可能更新的 schema。
8. 把 pinned schema ID 传入 `IcebergInsertCommandContext` / merge transaction
   context，或增加等价的明确 carrier。在 `IcebergTransaction.beginInsert()` /
   `beginMerge()` 的认证上下文内、数据下发前比较当前目标 schema ID；不一致时失败并要求
   重新规划。
9. rewrite/compaction 沿用已有 projection，不调用 write-default resolver。
10. 新增代码遵循仓库“断言正确性”规则：已知前置条件用 `DORIS_CHECK`/Java
    precondition；只有存在明确、可复现的失败路径才使用容错 `if`。

### 8.3 是否需要新的 Thrift 改动

推荐方案不需要把 `write-default` 再通过 Thrift 发送给 BE，因为 FE 已经把它变成完整输出
表达式，而已有 `schema_json` 能让 BE 按固定 schema 写文件。

可能需要的唯一新 Thrift 信息是 pinned schema ID/version，用于跨 planner/sink/transaction
校验或可观测性。如果 Java 侧 context 可以在数据下发前完成强校验，则甚至不必新增
Thrift。

如果最终确实新增 Thrift 字段：

- 只能 additive `optional`；
- 说明旧 FE/新 BE、新 FE/旧 BE 的行为；
- 添加 rolling-upgrade 单测；
- 不能把新的 `write_default_value` 字符串 carrier 复制到 BE 后再做一次默认值解析。

先用测试证明 schema pinning 需要的 carrier 边界，再决定是否改 Thrift。

## 9. Spark-Iceberg 对比边界

fixture 使用 Iceberg Java API：

1. 添加字段时指定 initial default；
2. commit；
3. 调用 `updateColumnDefault` 只改变 write default；
4. 重新加载 schema，验证 initial 和 write default 不同。

这能可靠验证 Iceberg metadata 和 typed value 语义。Spark-Iceberg 1.10.1 的 Spark SQL
写入端却会拒绝 partial INSERT column list，而不是替省略列消费 write default；现有
regression fixture 已经明确记录该限制。

因此：

- Spark/Iceberg 用于 metadata authoring、typed conversion 对比、写后读取物理结果；
- Iceberg V3 specification 和 `NestedField.writeDefault()` 是写语义的事实来源；
- 不要声称 Spark SQL 4.0 + Iceberg 1.10.1 已实现省略列 write-default；
- Doris 写入测试必须由 Doris 执行省略列/DEFAULT，之后由 Doris 和 Spark 分别读取写出的
  行，确认物理文件中保存的是当前 `write-default`。

上游入口：

- [Iceberg default values specification](https://iceberg.apache.org/spec/#default-values)
- [Iceberg SingleValueParser](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SingleValueParser.java)
- [Iceberg SchemaUpdate default APIs](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/core/src/main/java/org/apache/iceberg/SchemaUpdate.java)
- [Spark Parquet missing-field handling](https://github.com/apache/iceberg/blob/apache-iceberg-1.10.1/spark/v4.0/spark/src/main/java/org/apache/iceberg/spark/data/SparkParquetReaders.java)
- [Upstream default-value engine support discussion](https://github.com/apache/iceberg/issues/10761)

## 10. 测试矩阵

### 10.1 读取阶段

必须保持并 fresh 验证：

- File Scanner V1 + Parquet；
- File Scanner V1 + ORC；
- File Scanner V2 + Parquet；
- File Scanner V2 + ORC；
- 所有现有 mapped primitive types；
- UUID/FIXED/BINARY byte-for-byte；
- ARRAY/MAP/STRUCT 和嵌套 child defaults；
- present parent、NULL parent、explicit physical child NULL；
- optional absent/no default；
- required absent/no default error；
- predicate；
- equality delete key、dropped equality field 的历史 schema；
- ordinary current schema、time travel/ref schema；
- version 1 rolling compatibility 与 version 2 新语义。

### 10.2 写入阶段 FE/planner

至少覆盖：

- INSERT VALUES 省略列；
- 显式 `DEFAULT`；
- reordered column list；
- multi-row VALUES；
- INSERT SELECT 省略列；
- 显式 NULL 不替换；
- 显式值不替换；
- `DEFAULT(column)`；
- optional no default -> NULL；
- required no write default -> analysis error；
- MERGE NOT MATCHED INSERT；
- MERGE UPDATE 不变；
- branch schema；
- analyzer/sink/transaction schema skew；
- rewrite/compaction 不注入 default；
- 所有已映射 primitive 和 complex 类型。

planner test 应直接断言：

- 完整 output projection 的字段顺序；
- literal 的 Doris 类型和值；
- field ID 对应关系；
- pinned schema ID；
- `schema_json` 与解析 default 的 schema 完全相同。

### 10.3 写后端到端回归

扩展 Spark-Iceberg Docker fixture：

1. 用旧 schema 写 Parquet 和 ORC 文件；
2. 使用 Iceberg API 添加 initial/write default；
3. 更新 write default，使其与 initial default 不同；
4. Doris 读取旧文件，得到 initial default；
5. Doris 对当前 schema 执行省略列和显式 DEFAULT 写入；
6. Doris 读取新行，得到当前 write default；
7. Spark 读取 Doris 新写的物理行，结果也必须是当前 write default；
8. 显式 NULL 和显式值保持不变。

覆盖 File Scanner V1/V2 读取结果以及 Parquet/ORC 写出格式。复杂 default 也必须进入 fixture
或 focused unit test；如果 Iceberg 1.10.1 public API 拒绝某个合法 spec-level complex
default，必须记录 API 限制，并用 focused typed-conversion test 补足，不能手改 metadata。

Regression 的 `.out` 只能由 runner 自动生成，禁止手写。特别是
`regression-test/data/export_p0/test_export_data_types.out` 与本功能无关，不应改动。

## 11. 编译、格式和测试命令

执行本节之前必须已经完成第 3.2 节的 latest-master rebase 和第 4.4 节的预编译三方库
安装。不能先在旧 base/旧三方库上构建，再把结果当作 rebase 后的验证证据。

下面所有命令都从唯一 worktree 执行：

```bash
cd /mnt/disk1/changyuwei/doris-worktrees/iceberg-v3-defaults
mkdir -p /mnt/disk1/changyuwei/tmp/pr65851-local-tests
export TMPDIR=/mnt/disk1/changyuwei/tmp/pr65851-local-tests
export MAVEN_OPTS=-Xmx12g
```

### 11.1 完整 FE + BE 构建

只使用仓库 `build.sh`：

```bash
./build.sh --fe --be
```

默认保持 ASAN。除非用户明确要求性能测试，不要把 `BUILD_TYPE` 改为 RELEASE。

### 11.2 FE 定向测试

读取阶段：

```bash
./run-fe-ut.sh --run \
  org.apache.doris.datasource.iceberg.source.IcebergScanNodeTest
```

写入阶段至少复用/扩展相关 Iceberg planner test：

```bash
./run-fe-ut.sh --run \
  org.apache.doris.datasource.iceberg.IcebergDDLAndDMLPlanTest
```

如果新增独立 resolver、BindSink 或 MERGE test class，使用其完整类名再各跑一次。最终还要
跑受影响 FE module 的完整测试面，不能只跑单个方法。

### 11.3 BE 定向测试

读取实现的已知核心 suite：

```bash
./run-be-ut.sh --run \
  --filter='IcebergReaderTest.*:MockTableSchemaChangeHelper.*:IcebergV2ReaderTest.*:TableReaderTest.*:ColumnMapperTest.*'
```

推荐方案下 write-default 由 FE 生成完整表达式，通常不需要 BE default parser；但如果修改
writer contract、schema carrier 或 merge writer，必须增加并运行对应 BE writer tests。

### 11.4 格式和静态检查

BE C++ 只能使用仓库提供的 clang-format 16 脚本：

```bash
build-support/clang-format.sh
build-support/check-format.sh
git diff --check
```

`clang-format.sh` 会处理仓库定义的 C++ 目录，运行后立即检查 diff，防止混入无关变化。

FE checkstyle 集成在 FE build/test 中。BE build 生成 `compile_commands.json` 后运行：

```bash
build-support/run-clang-tidy.sh \
  --base apache/master \
  --build-dir be/build_ASAN
```

如果再次遇到 `stddef.h not found`，保留完整日志，确认是本机 LLVM resource 问题后如实
报告；不能把“尝试运行”写成“clang-tidy passed”。

### 11.5 Regression

当前读取 suite：

```bash
./run-regression-test.sh --run \
  -d external_table_p0/iceberg \
  -s test_iceberg_initial_defaults
```

如果将 suite 重命名为同时覆盖 read/write default，命令中的 `-s` 必须改成 Groovy
`suite("...")` 的真实名称。用 runner 更新 `.out`，不要手写。

### 11.6 本地集群

构建产物和运行目录必须是本 worktree 的 `output/`：

```bash
./output/fe/bin/start_fe.sh --daemon
./output/be/bin/start_be.sh --daemon
```

启动后至少等待 30 秒；未就绪就继续等待并检查本 worktree 的 FE/BE log。首次启动可能需要：

```bash
mysql -h 127.0.0.1 -P 56030 -uroot \
  -e 'ALTER SYSTEM ADD BACKEND "127.0.0.1:56050";'
```

`SHOW FRONTENDS` 只能证明 FE 启动，不能证明 BE-backed query path 可用。

## 12. 冲突处理、review 和 CI

### 12.1 推荐执行顺序

1. 重新核对 `git status` 和 PR 远端 head。
2. 严格按第 3.2 节分别保护任务 WIP 和本地环境 overlay。
3. `git fetch apache master`，立即 rebase 当时最新的 `apache/master` 并解决冲突。
4. 按第 4.4 节从指定 URL 下载并重新安装预编译三方库。
5. 恢复两个 stash，解决 WIP 与最新 master 的冲突。
6. 补齐读取 review WIP，fresh 跑读取 FE/BE tests。
7. 在同一分支实现 write-default。
8. 扩展 Spark fixture 和 regression，完成全部测试矩阵。
9. 更新设计文档与 PR 描述，说明一个 PR 同时包含 read/write execution。
10. 完整 build、format、checkstyle、clang-tidy、unit、regression。
11. 按 `.github/workflows/code-review-runner.yml` 对完整 three-dot diff 自审。
12. 只提交任务文件，使用精确 `--force-with-lease` 推送当前 PR。
13. 在 PR 上重新请求 `/review` 和 `run buildall`，监控新 head 的 TeamCity。

与最新 master 冲突时重点检查 V2 reader 热路径文件，例如：

```text
be/src/format_v2/column_mapper.*
be/src/format_v2/table_reader.*
be/src/format_v2/table/iceberg_reader.*
be/test/format_v2/...
```

不能机械选择 ours/theirs；必须重新验证 master 新增的 Parquet V2 行为和本 PR 的
default/equality-delete 语义都保留。

### 12.2 完整 PR 自审标准

依据 `.github/workflows/code-review-runner.yml`：

1. 建立 changed-file/risk summary 和去重 issue ledger；
2. 独立执行 code-reviewer 与 architect 两轮检查；
3. 每条 finding 都对照真实 diff 和 fresh test 证据确认；
4. 修复确认的问题后重复检查；
5. 只有 code reviewer 为 `APPROVE`、architect 为 `CLEAR` 且没有 verified blocking
   finding，才算 review-clear。

额外核对：

- 类型正确性与 field-ID 语义；
- scanner V1/V2 边界；
- rolling upgrade；
- schema pinning 和 branch；
- authentication boundary；
- explicit NULL；
- complex literal ownership/lifetime；
- error path 是否明确失败而不是静默降级；
- 环境文件是否误暂存；
- 所有测试结果是否与当前 head 对齐。

### 12.3 TeamCity

旧 head `5dbdb411...` 曾有 compile/cloud/P0/cloud_p0/nonConcurrent/performance/vault
通过；GitHub rollup 曾显示 BE UT、FE UT、External Regression、FE coverage 失败。这些结果
在新 head 推送后全部过时。

CI 处理规则：

- 先确认 TeamCity build 对应 PR 当前精确 head；
- 比较同一 head 的 rerun，区分基础设施噪声和真实源码失败；
- 只修复与本 PR 可归因的问题；
- 不因无关 flaky/infra failure 扩大代码范围；
- 所有下载都落到 `/mnt/disk1/changyuwei/tmp`。

## 13. 提交要求

每个提交只包含任务相关文件。环境文件、`AGENTS.md`、hooks、端口和本机 JDK 配置绝不
提交。提交消息遵守仓库模板，例如：

```text
[feature](iceberg) Support Iceberg write defaults

### What problem does this PR solve?

Issue Number: None

Related PR: #65851

Problem Summary: <完整说明问题、根因、修改和前后行为>

### Release note

Support Iceberg V3 initial defaults when reading missing fields and write
defaults for omitted or DEFAULT values when writing Iceberg tables.

### Check List (For Author)

- Test: <只填写实际运行的测试>
- Behavior changed: Yes
- Does this need documentation: Yes
```

如果最终 squash 为一个功能提交，标题和 Problem Summary 必须同时覆盖 read/write；不要再
把 PR 描述成只支持 initial defaults。

## 14. Definition of Done

只有同时满足以下条件，本 PR 才算完成：

- 分支已经 rebase 到执行时最新的 `apache/master`，master 侧 ahead count 为 0；
- 三方库来自第 4.4 节指定的 automation URL，且不是基础 checkout 的复制品；
- 读取阶段四条 scanner/file-format 路径 fresh 通过；
- 最新四个 P1 review 问题有代码和测试闭环；
- 同一 PR 中完成 write-default 的 INSERT、DEFAULT 和 MERGE INSERT 路径；
- explicit NULL/value、UPDATE、MERGE UPDATE、rewrite 行为保持正确；
- schema/branch pinning 在数据下发前验证；
- 所有 Doris 既有 Iceberg mapped types 以及 complex defaults 有测试；
- Spark-Iceberg Docker 创建真实 V3 metadata，Doris 和 Spark 共同验证写后物理结果；
- 不新增不相关类型映射或 DDL；
- format、FE checkstyle、相关 FE/BE UT、regression fresh 通过；
- clang-tidy 成功，或对环境阻塞留有可复查的诚实证据；
- 完整 PR 自审达到 `APPROVE` + `CLEAR`；
- PR 与最新 master 无冲突；
- 新 PR head 的相关 TeamCity 通过，或剩余失败被证明与本 PR 无关；
- commit 中没有任何本地环境文件；
- PR 标题、描述和两份设计/交接文档都反映 read + write 已合并。

## 15. 接手后的第一轮动作

无需重新从零调研。按下面顺序开始：

```text
1. 阅读本文和 iceberg-v3-default-values-design.md。
2. 核对 HEAD、PR head、apache/master 和 git status。
3. 分别 stash 8 个任务 WIP 和本地环境 overlay，记录两个对象 SHA。
4. 立即 fetch 并 rebase 执行时最新的 apache/master；这是第一个实际代码操作。
5. 从指定 automation URL 下载并重新安装 thirdparty/installed。
6. 恢复两个 stash，解决与最新 master 的冲突。
7. 补齐 V2 semantics version 2 测试标注。
8. fresh 跑 IcebergScanNodeTest 和五个 BE 核心 suite。
9. 完成当前 review 修复并形成干净任务提交。
10. 实现 statement-scoped、field-ID keyed、schema-pinned write-default context。
11. 打通 INSERT / DEFAULT / MERGE NOT MATCHED INSERT。
12. 扩展 Spark fixture、unit tests 和 regression。
13. 完整构建、自审、推送和 TeamCity 闭环。
```

遇到实现分歧时，优先级为：Iceberg V3 spec 与 typed API 契约、当前查询/写入绑定的 schema
ID、字段 ID 语义、显式 NULL、rolling-upgrade 安全、Doris 既有类似代码模式。不能为了缩小
改动而退回到通用 `Column.defaultValue` 或让 BE 接收无类型 SQL 文本重新猜值。
