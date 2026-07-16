# 🤝 Session Handoff — `fe/fe-connector` 剥离 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文。
> 完成明细**不落这里**（在 `git log` + [`progress.md`](./progress.md)）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# ✅ Phase 1+2+3 完成（建精简 shade + 切 hms/iceberg + 静态&打包闸门全绿） → 🆕 下一个 session = **Phase 4：e2e（唯一真闸门）**

> 分支 `catalog-spi-hive-shade-12`。行号信 HEAD 不信文档。代码改动已 commit（见 git log 最新一条）。

## 现状（一句话）
`fe/fe-connector/` 已整体脱离 122MB 胖 `hive-catalog-shade`，改用自建 15MB 精简 shade 模块 `fe-connector-hms-hive-shade`（只装 Hive 元数据客户端闭包，重定位 thrift→`shade.doris.hive.org.apache.thrift`）。build+UT（197 测试类全绿）、静态闸门（19 模块 dependency:tree 全空）、打包闸门（三插件 zip 无胖 shade、精简 shade 各 1 份、关键类无重复）、多 agent 对抗 review（零 confirmed）**均已过**。**唯一没做的是 e2e。**

## 第一件事（按顺序）
1. **读** `progress.md` 末段（2026-07-16 Phase 1+2+3 结论，含闸门证据 + 两处现补的缺类）+ `design.md §4`（决策速查）。
2. **动码/跑测前探并发**（`pgrep -af maven|grep wt-catalog-spi` + 近 90s mtime）。
3. **重新构建部署产物**跑 e2e（精简 shade 需重打包插件重部署；旧部署目录可能还是胖 shade）。

## 🎯 Phase 4 要做的（`tasklist.md` FCL-40/41/42）
- **异构 HMS docker 套件**：① 普通 hive catalog 读+写；② **iceberg-on-HMS** INSERT/DELETE/MERGE/read，断言与独立 iceberg 目录同表同结果；③ hudi-on-HMS 读。
- **🔴 TCCL 不回归**（memory `catalog-spi-plugin-tccl-classloader-gotcha` / D5）：`test_string_dict_filter` 类用例 + MetaStoreFilterHook/URIResolverHook 按名反射路径 + kerberos HMS（若环境有）；FE 启动 + 缓存（MetaCache/StatisticsCache/FileSystemCache）冒烟。
- **专项**：storage-api **2.7.0** 的 write/ACID 路径（本轮从胖 shade 的 2.8.1 换回 3.1.3 原生 2.7.0，review 判无回退但要 e2e 兜底）。
- 结果（含 CI 编号）追加 `progress.md`。

## ⚠️ 白名单 shade 的运行时缺类（Phase 4 可能再遇，按此法补）
精简 shade 用 **白名单 `<includes>`**，只装列出的 hive 客户端闭包；运行时其余靠各插件自带 hadoop-common 闭包/宿主 parent-first 提供。**跑到才暴露的缺类**本轮已补两处（老 Jackson `ObjectMapper`、iceberg `bundled-guava`）；e2e 若再报 `NoClassDefFoundError`（尤其 kerberos/filter-hook 路径），**按同法补**：查是哪个 artifact 的类 → 加进 `fe-connector-hms-hive-shade/pom.xml` 的 `<dependency>`(必要时显式 `<scope>compile</scope>` 覆盖 fe/pom.xml 里的 test-scope 管理) + artifactSet `<include>`。**别退回黑名单 excludes**（那会把 122MB junk 又拉回来）。

## ⚙️ 构建/验证坑（本轮实证，直接复用）
- `-pl` 选精简 shade 模块用 **`:fe-connector-hms-hive-shade`**（冒号 artifactId 选择器），裸名 maven 报 "Could not find the selected project"。
- 后台跑 maven **别** `nohup ... &` 套在 `run_in_background` Bash 里 → 脱离 harness 跟踪、误报秒完成；用 `tail --pid=<mvnpid>` 阻塞等真 `BUILD SUCCESS/FAILURE`。
- 依赖 shade 的模块**跑到 `package`**（`test-compile` 假报缺类）；`-am` 必填；`-Dmaven.build.cache.enabled=false` 且数 `Running org.apache.doris` 行；`mvn|tail` 后 `$?` 是 tail 的（重定向到文件读 BUILD 行）。
- maven 用绝对 `-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml`。
- `git add` 用 path-whitelist，**严禁 `-A`**（工作树大量非本线程 scratch）。

## ✅ 每个 Phase 收尾
commit（英文）+ 覆盖本 HANDOFF + `progress.md` 追加一行 + 勾 `tasklist.md`。
