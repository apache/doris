# 🤝 Session Handoff — `fe/fe-connector` 剥离 `hive-catalog-shade`

> **滚动文档**：每次 session 结束**覆盖式更新**，只保留下一个 session 必须的上下文。
> 完成明细**不落这里**（在 `git log` + [`progress.md`](./progress.md)）。
> 空间索引 [`README.md`](./README.md) · 设计 [`design.md`](./design.md) · 清单 [`tasklist.md`](./tasklist.md)

---

# ✅ Phase 0 完成（设计定案 + 用户签字 + 红队 GO） → 🆕 下一个 session = **Phase 1：建承重墙 shade 模块**

> 基线分支 `catalog-spi-hive-shade-12`（== 旧 `catalog-spi-11-hive` 内容）。行号信 HEAD 不信文档。

## 第一件事（按顺序）

1. **读** `progress.md` 末段（Phase 0 结论）+ `design.md §3`（精确 bundle 清单）+ `§4`（决策速查）。
2. **动码前探并发**（`pgrep -af maven|grep wt-catalog-spi` + 近 90s mtime）；`git add` 白名单严禁 `-A`。
3. **建 `fe/fe-connector/fe-connector-hms-hive-shade/pom.xml`**（镜像 `fe-connector-paimon-hive-shade/pom.xml`，324 行）。

## 🔑 Phase 0 已定的四件事（照做，别再议）

- **D1=A 共享一个** `fe-connector-hms-hive-shade`：hms 依赖它（compile 传递给 hive/hudi/iceberg），iceberg 复用同一制品。
- **D2=3.1.3**；**D3=复用前缀 `shade.doris.hive.org.apache.thrift`**（= 补丁客户端现有 import，**零源码改动**，别起新前缀）。
- **D4=KEEP_IN_HMS**：补丁 `HiveMetaStoreClient.java` 留在 fe-connector-hms 原地，**不搬不改写**（字节码已全是重定位名）。

## 📦 精简 shade 精确 bundle 清单（Phase 0 核实，直接照抄进 pom）

**bundle 进 shade（`<optional>true>` + 重定位 thrift）**：
- `org.apache.hive:hive-standalone-metastore:3.1.3` ← **⚠️不是 `hive-metastore:3.1.3`（13 类空壳）**
- `org.apache.thrift:libthrift:0.9.3`（**内联钉**，非 managed 0.16.0）+ `org.apache.thrift:libfb303:0.9.3`
- `org.apache.hive:hive-common:3.1.3`（带 hive-shims）+ `org.apache.hive:hive-storage-api:2.7.0`
- `org.apache.hive:hive-serde:3.1.3`（iceberg 用）+ `org.apache.iceberg:iceberg-hive-metastore:1.10.1`（iceberg 用，排除 iceberg-core）

**relocation**：`org.apache.thrift → shade.doris.hive.org.apache.thrift`（**必须此前缀**）；`it.unimi.dsi.fastutil → shade.doris.hive.it.unimi.dsi.fastutil`（防御）。
**artifactSet exclude**：paimon、完整 hadoop、hive-exec、DLF/aliyun、derby/datanucleus/bonecp/HikariCP/orc、bouncycastle、jersey/jaxb、arrow/parquet/avro、guava/protobuf/jackson/slf4j/log4j/commons、iceberg-core/api/caffeine。
**META-INF**：过滤 `*.SF/*.DSA/*.RSA`、`META-INF/maven/**`、`META-INF/versions/**`（照 paimon）。

## ⚠️ 两条红队约束（Phase 1/2 必守）

1. **iceberg 摘全局 shade（`fe-connector-iceberg/pom.xml` 现第 ~148 行那条）与接精简 shade 放同一次提交（原子切换）**，别留过渡期两份 `HiveCatalog` 并存（实测 37821B vs 1.10.1 37853B，谁生效随 classpath 序）。顺带订正 iceberg pom:138-145 过期的 "支持 DLF" 注释。
2. **Phase 1/4 验证**：重部署后类加载冒烟——每插件 `org.apache.hadoop.hive.metastore.api.Table` 与 `shade.doris.hive.org.apache.thrift.TException` **各仅一份**；HMS-on-hive/hudi/iceberg e2e；Kerberos + `MetaStoreFilterHook`/`URIResolverHook` 按名反射路径（D5 TCCL pin 别回归：`ThriftHmsClient.doAs` 钉 plugin loader、`HmsConfHelper.createHiveConf` `setClassLoader`）。

## ⚙️ 构建坑（兄弟任务血泪，全文见 `tasklist.md §构建坑`）

- 测试 **`-Dmaven.build.cache.enabled=false`** + 数 `Running org.apache.doris` 行；**`-am` 必填**；依赖 shade 的模块**跑到 `package`**（`test-compile` 假报 `HiveConf does not exist`）。
- maven 用**绝对 `-f`** `/mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml`；`-pl fe-connector-XXX`（reactor 相对名）。
- `hive-serde` 闭包首次需联网（`servlet-api:2.4` 不在本地仓）→ 首次去 `-o`。
- shade 模块无 src/main → 照 paimon 钉 `maven-jar-plugin` default-jar 到 package + `forceCreation`（并行 reactor 下 shade 早于 jar 会炸）。

## ✅ 每个 Phase 收尾

commit（英文）+ 覆盖本 HANDOFF + `progress.md` 追加一行 + 勾 `tasklist.md`。
