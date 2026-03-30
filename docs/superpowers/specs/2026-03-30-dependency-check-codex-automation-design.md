# Dependency-Check Codex 自动修复设计

## 目标

提供一套由 Codex 直接驱动的 `dependency-check` 治理流程，在不修改 `fe/pom.xml` 增加 plugin 配置的前提下，实现如下闭环：

- 对单个 Maven 扫描目录执行 `dependency-check`
- 识别漏洞对应的是直接依赖还是传递依赖
- 优先在父 `pom` 的版本属性或 `dependencyManagement` 中统一升级
- 对每轮升级执行一次 `mvn clean install -DskipTests` 验证
- 对短期无法兼容修复的问题，写入外置 `suppression.xml`
- 保留每次运行的计划、结果和报告，支持重复执行

## 范围

本设计覆盖：

- 单个扫描目录的 `dependency-check` 扫描与报告解析
- 基于 `mvn dependency:tree` 的直接依赖 / 间接依赖判定
- Codex 自动修改 `pom.xml`、版本属性、`dependencyManagement`、BOM 版本
- 构建验证、失败回退、白名单沉淀
- 外置工具目录、配置文件和运行产物布局

本设计不覆盖：

- 在 Doris 仓库 `pom.xml` 中新增 `dependency-check` plugin 声明
- 同时处理多个扫描目录
- 自动提交 Doris 代码改动之外的发布流程
- 对非 Maven 项目的漏洞治理

## 核心约束

### 1. 不侵入 Doris Maven 配置

`dependency-check` 通过命令行直接执行，不在 `fe/pom.xml` 或其它模块 `pom.xml` 中新增 plugin 配置。标准扫描命令如下：

```bash
mvn org.owasp:dependency-check-maven:12.2.0:check \
  -DfailBuildOnCVSS=7 \
  -DnvdApiServerId=nvd-api-key \
  -DskipTests
```

需要使用白名单时，附加：

```bash
-DsuppressionFiles=<absolute-path-to-suppressions.xml>
```

### 2. 只处理单个扫描目录

每次运行只接受一个扫描目录，例如 `fe/`。这降低了依赖树判定、构建验证和回滚的复杂度，也符合当前使用方式。

### 3. 版本升级优先集中到父级统一入口

当漏洞可以通过版本升级修复时，优先顺序固定为：

1. 父 `pom` 中的版本属性
2. 父 `dependencyManagement`
3. 父级导入 BOM 的版本
4. 子模块 `pom.xml`

除非依赖只在某个子模块局部定义，否则不允许把修复散落到多个子模块中。

## 外置工具目录

所有工具、配置、报告和白名单都放在仓库外的独立目录：

`/mnt/disk1/gq/idea/dependency-check-tools/`

建议布局如下：

- `bin/run.sh`
- `configs/incubator-doris-fe.yaml`
- `runs/incubator-doris/fe/<timestamp>/...`
- `suppressions.xml`

其中：

- `bin/run.sh` 是统一入口
- `configs/*.yaml` 保存仓库路径、扫描目录、构建命令、报告输出位置等配置
- `runs/...` 保存每次执行的扫描报告、中间计划和最终结果
- `suppressions.xml` 路径由用户显式指定，不强制固定在某个子目录

## 配置模型

建议使用单个 YAML 配置文件，例如：

```yaml
repo_root: /mnt/disk1/gq/idea/incubator-doris
scan_dir: fe
working_dir: /mnt/disk1/gq/idea/incubator-doris/fe
suppression_file: /mnt/disk1/gq/idea/dependency-check-tools/suppressions.xml
report_root: /mnt/disk1/gq/idea/dependency-check-tools/runs/incubator-doris/fe
fail_cvss: 7
nvd_api_server_id: nvd-api-key
check_cmd:
  - mvn
  - org.owasp:dependency-check-maven:12.2.0:check
  - -DfailBuildOnCVSS=7
  - -DnvdApiServerId=nvd-api-key
  - -DskipTests
build_cmd:
  - mvn
  - clean
  - install
  - -DskipTests
```

运行时允许通过 CLI 参数覆盖：

- `--config`
- `--mode`
- `--suppression-file`
- `--dry-run`

## 运行模式

入口脚本提供三个模式：

### 1. `analyze`

只执行扫描、报告解析和修复计划生成，不修改任何文件。

### 2. `fix`

执行扫描并自动修复可升级的问题。每一组改动后执行一次构建验证。验证失败则回退当前组改动并标记为待人工或待白名单问题。

### 3. `fix-and-suppress`

在 `fix` 的基础上，把确认短期无法兼容修复的问题写入 `suppression.xml`，然后重新执行扫描确认结果收敛。

## 主流程

完整流程如下：

1. 读取配置，进入 `working_dir`
2. 执行 `dependency-check` 扫描并生成 HTML / XML / JSON 报告
3. 解析报告，提取受影响坐标、CVE/GHSA、CVSS 和建议版本信息
4. 对每个受影响坐标执行 `mvn dependency:tree` 定位依赖路径
5. 将问题分为：
   - 可通过升级修复
   - 短期无法兼容修复，需白名单
6. 对可修复问题按统一版本入口进行分组修改
7. 每组修改后执行一次 `mvn clean install -DskipTests`
8. 构建成功则保留改动；构建失败则回退该组改动并转为白名单候选
9. 如运行模式允许白名单，则把候选项写入 `suppression.xml`
10. 输出计划和结果文件；如修改了依赖，保留对应工作区改动

## 直接依赖与间接依赖判定

对每个漏洞坐标，通过如下命令缩小依赖树：

```bash
mvn -pl <module> -am dependency:tree \
  -Dincludes=<groupId>:<artifactId> \
  -Dverbose \
  -DskipTests
```

判定规则如下：

- 如果漏洞坐标出现在项目一层依赖中，或该版本由当前模块直接声明，则判定为直接依赖
- 如果漏洞坐标由其它库带入，则判定为传递依赖

针对传递依赖，再做进一步决策：

- 若升级上游直接依赖即可解决，则优先升级上游
- 若升级上游不兼容，但可在父 `dependencyManagement` 中统一覆盖传递依赖版本，则允许覆盖
- 若两种方案都无法通过构建验证，则进入白名单候选

## 升级策略

升级动作必须按“同一控制点分组”执行，避免一次只改一个受控 artifact 导致验证成本过高。典型分组包括：

- 同一个版本属性，例如 `${jackson.version}`
- 同一个 BOM，例如 `netty-bom`
- 同一个父 `dependencyManagement` 条目

Codex 修改规则如下：

- 优先修改父 `pom` 的版本属性
- 没有属性时，优先修改父 `dependencyManagement`
- 若依赖由 BOM 控制，则优先升级 BOM
- 仅在父级无统一控制点时修改子模块 `pom.xml`

在 `fe/` 场景下，`fe/pom.xml` 已经集中声明大量版本属性和 `dependencyManagement` 条目，因此绝大多数修复都应收敛在那里。

## 构建验证与回退

每组升级动作完成后，立即执行一次：

```bash
mvn clean install -DskipTests
```

验证规则如下：

- 构建成功：保留该组改动，并记录为 `fixed`
- 构建失败：只回退当前组改动，不影响之前已通过验证的修改
- 连续失败或明确不兼容：记录为 `suppression-candidate`

回退不能依赖破坏性 git 命令，应通过保存变更前文件内容并恢复，或由 Codex 生成反向补丁仅撤销当前组改动。

## 白名单策略

白名单文件由用户指定绝对路径，例如：

`/mnt/disk1/gq/idea/dependency-check-tools/suppressions.xml`

写入规则如下：

- 只有在尝试升级并完成构建验证后仍无法修复时，才允许加入白名单
- 每条 suppression 必须带原因说明，至少记录：
  - 受影响依赖
  - 漏洞 ID
  - 不能升级的原因
  - 对应失败验证或兼容性说明
- 匹配范围尽量精确到 `GAV + CVE/GHSA`
- 已通过升级修复的问题应主动清理对应 suppression

白名单的目的是沉淀“已知短期风险”，而不是跳过正常可修复问题。

## 输出产物

每次运行在时间戳目录下产出：

- `dependency-check-report.html`
- `dependency-check-report.xml`
- `dependency-check-report.json`
- `plan.json`
- `result.json`
- `summary.md`

输出语义如下：

- `plan.json`：漏洞、依赖路径、直接/间接依赖判定、预期修复动作
- `result.json`：每个问题最终状态，例如 `fixed`、`suppressed`、`manual-review`
- `summary.md`：给人工快速阅读的摘要，包括升级了哪些版本、哪些问题进入白名单、哪些需要后续人工决策

## 示例命令

分析模式：

```bash
/mnt/disk1/gq/idea/dependency-check-tools/bin/run.sh \
  --config /mnt/disk1/gq/idea/dependency-check-tools/configs/incubator-doris-fe.yaml \
  --mode analyze
```

自动修复并白名单收敛：

```bash
/mnt/disk1/gq/idea/dependency-check-tools/bin/run.sh \
  --config /mnt/disk1/gq/idea/dependency-check-tools/configs/incubator-doris-fe.yaml \
  --mode fix-and-suppress \
  --suppression-file /mnt/disk1/gq/idea/dependency-check-tools/suppressions.xml
```

## 验证

本设计进入实现后，至少需要验证：

- `dependency-check` 报告可稳定生成并被正确解析
- `dependency:tree` 可正确定位漏洞坐标路径
- 直接依赖和传递依赖可以按预期分类
- 版本属性、`dependencyManagement`、BOM 升级可按优先级生效
- 构建失败时只回退当前组改动
- `suppression.xml` 可正确增删并被扫描命令引用
- 最终输出的 `plan.json`、`result.json`、`summary.md` 与工作区改动一致

## 风险与后续事项

- `dependency-check` 对推荐修复版本的提示不一定足够精确，Codex 仍需结合 Maven 解析结果决定实际升级点
- 某些传递依赖的安全版本可能与当前上游库不兼容，导致频繁进入白名单候选
- `mvn clean install -DskipTests` 只能验证编译和打包闭环，不能替代功能测试
- 若后续要扩展到多个扫描目录，应在当前单目录实现稳定后再引入批处理编排
