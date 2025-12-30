# Doris 支持 Iceberg JDBC Catalog（实现指南）

## 目标
在 Doris 中新增对 Iceberg JDBC Catalog 的支持（Iceberg 元数据存储在 JDBC 数据库中）。
集成 Iceberg `org.apache.iceberg.jdbc.JdbcCatalog` 到现有 Iceberg 外部 Catalog 框架。

## 范围
- 新增 Iceberg catalog 类型：`iceberg.catalog.type = jdbc`。
- FE 使用 Iceberg `JdbcCatalog` 做元数据操作。
- Iceberg 扫描链路保持不变（`IcebergApiSource` + BE 文件扫描）。

## 非目标
- Doris JDBC 外表（已有）。
- BE 侧 Iceberg 扫描逻辑调整。
- JDBC 驱动包分发或部署流程变更。

## 总体流程
1. `CREATE CATALOG ... type=iceberg` 由 `IcebergExternalCatalogFactory` 创建。
2. `CatalogProperty` 根据 `IcebergPropertiesFactory` 构建 `MetastoreProperties`。
3. 新增 `IcebergJdbcMetaStoreProperties`，内部初始化 Iceberg `JdbcCatalog`。
4. `IcebergScanNode` 继续使用 `IcebergApiSource` 读取 Iceberg 数据文件。

## 参数总览

### 必需参数

| 参数名 | 曾用名/别名 | 说明 | 默认值 | 必填 |
|--------|-------------|------|--------|------|
| type | - | 固定为 `iceberg` | - | 是 |
| iceberg.catalog.type | - | 固定为 `jdbc` | - | 是 |
| uri | iceberg.jdbc.uri | JDBC 连接串，如 `jdbc:postgresql://host:5432/iceberg` | - | 是 |
| warehouse | - | Iceberg warehouse 路径 | - | 是 |
| driver_url | - | JDBC 驱动 JAR 路径，如 `postgresql-42.7.3.jar` | - | 是 |
| driver_class | - | JDBC 驱动类名，如 `org.postgresql.Driver` | - | 是 |

### JDBC 认证参数

| 参数名 | 说明 | 默认值 | 必填 |
|--------|------|--------|------|
| jdbc.user | JDBC 用户名 | - | 否 |
| jdbc.password | JDBC 密码（敏感参数，会被脱敏显示） | - | 否 |

### JDBC Catalog 可选参数

| 参数名 | 说明 | 默认值 | 必填 |
|--------|------|--------|------|
| jdbc.init-catalog-tables | 是否自动创建 Catalog 元数据表 | false | 否 |
| jdbc.schema-version | 元数据表 Schema 版本（V0/V1） | V1 | 否 |
| jdbc.strict-mode | 是否启用严格模式 | false | 否 |

### JDBC 连接参数
以下参数直接透传给 JDBC 驱动：

| 参数名 | 说明 | 默认值 |
|--------|------|--------|
| jdbc.useSSL | 是否使用 SSL 连接 | false |
| jdbc.verifyServerCertificate | 是否验证服务器证书 | false |
| jdbc.sslMode | SSL 模式（MySQL 8.x 推荐使用） | - |

### 存储属性（StorageProperties）

JDBC Catalog 同样需要配置存储属性以访问 Iceberg 数据文件。支持的存储系统包括：

- HDFS
- AWS S3
- 阿里云 OSS
- 腾讯云 COS
- 华为云 OBS
- MinIO

存储属性配置方式与其他 Iceberg Catalog 类型一致，请参阅 Doris 文档【支持的存储系统】部分。

示例（S3 存储）：
```sql
CREATE CATALOG iceberg_jdbc PROPERTIES (
  -- JDBC Catalog 参数
  "type" = "iceberg",
  "iceberg.catalog.type" = "jdbc",
  "uri" = "jdbc:postgresql://host:5432/iceberg",
  "warehouse" = "s3://bucket/warehouse",
  "driver_url" = "postgresql-42.7.3.jar",
  "driver_class" = "org.postgresql.Driver",
  "jdbc.user" = "iceberg",
  "jdbc.password" = "secret",
  -- 存储属性
  "s3.endpoint" = "http://minio:9000",
  "s3.access_key" = "minioadmin",
  "s3.secret_key" = "minioadmin",
  "s3.region" = "us-east-1"
);
```

### 说明
- 保留 `jdbc.password` 可复用现有敏感信息屏蔽逻辑。
- 如新增别名（例如 `iceberg.jdbc.password`），同步加入敏感 key 列表。
- `jdbc.*` 需原样透传给 Iceberg JDBC Catalog（由 JDBC 驱动解析），不要过滤未知 `jdbc.` 参数。
- 部分 JDBC 驱动已弃用 `verifyServerCertificate`（例如 MySQL 8.x 建议使用 `sslMode`），
  以驱动版本支持情况为准。

## 代码改动

### 1) 新增 catalog 类型常量
文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergExternalCatalog.java`
- 增加 `ICEBERG_JDBC = "jdbc"`。

### 2) 新增 ExternalCatalog 实现
文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergJdbcExternalCatalog.java`
- 参考 `IcebergRestExternalCatalog`。
- 构造函数中设置 `catalogProperty = new CatalogProperty(resource, props)`。

### 3) 新增 Metastore Properties
文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/IcebergJdbcMetaStoreProperties.java`

职责：
- 继承 `AbstractIcebergProperties`。
- 使用 `@ConnectorProperty` 定义字段：
  - `uri`
  - `warehouse`
  - `jdbc.user`
  - `jdbc.password`
  - `jdbc.init-catalog-tables`
  - `jdbc.schema-version`
  - `jdbc.strict-mode`
- 实现 `getIcebergCatalogType()` 返回 `ICEBERG_JDBC`。
- 实现 `initCatalog(...)`：
  - 通过 `CatalogUtil.buildIcebergCatalog` 创建实例。
  - 设置 `CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC`。
  - 透传 `warehouse`、`uri` 及所有 `jdbc.*`。
  - 如需 FileIO 参数，复用 `IcebergRestProperties.toFileIOProperties(...)`。

### 4) 注册到 Properties Factory
文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/IcebergPropertiesFactory.java`
- 增加 `register("jdbc", IcebergJdbcMetaStoreProperties::new)`。

### 5) 注册到 Catalog Factory
文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergExternalCatalogFactory.java`
- 增加 `ICEBERG_JDBC` 分支，创建 `IcebergJdbcExternalCatalog`。

### 6) 注册到 Gson
文件：`fe/fe-core/src/main/java/org/apache/doris/persist/gson/GsonUtils.java`
- 在 `dsTypeAdapterFactory` 中注册 `IcebergJdbcExternalCatalog`。

### 7) 扫描节点支持
文件：`fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/source/IcebergScanNode.java`
- switch 中新增 `ICEBERG_JDBC` 分支。
- 仍使用 `IcebergApiSource`。

### 8) 单测
新增：`fe/fe-core/src/test/java/org/apache/doris/datasource/property/metastore/IcebergJdbcMetaStorePropertiesTest.java`

建议断言：
- `CatalogUtil.ICEBERG_CATALOG_TYPE` 为 `jdbc`。
- `warehouse` 与 `uri` 透传正确。
- `jdbc.*` 参数在最终 options 中可见。

可选集成测试：
- `regression-test/suites/external_table_p0/iceberg` 新增 JDBC 元数据库测试。
- 使用真实 JDBC 数据库（如 PostgreSQL）。

## 示例 SQL

### 基础示例（PostgreSQL + HDFS）
```sql
CREATE CATALOG iceberg_jdbc PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "jdbc",
  "uri" = "jdbc:postgresql://host:5432/iceberg",
  "warehouse" = "hdfs://namenode:8020/warehouse",
  "driver_url" = "postgresql-42.7.3.jar",
  "driver_class" = "org.postgresql.Driver",
  "jdbc.user" = "iceberg",
  "jdbc.password" = "secret",
  "jdbc.init-catalog-tables" = "true"
);
```

### S3 存储示例（PostgreSQL + MinIO）
```sql
CREATE CATALOG iceberg_jdbc_s3 PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "jdbc",
  "uri" = "jdbc:postgresql://localhost:5432/iceberg_db",
  "warehouse" = "s3://iceberg-bucket/warehouse",
  "driver_url" = "postgresql-42.7.3.jar",
  "driver_class" = "org.postgresql.Driver",
  "jdbc.user" = "iceberg_user",
  "jdbc.password" = "Iceberg123",
  "jdbc.init-catalog-tables" = "true",
  -- S3/MinIO 存储配置
  "s3.endpoint" = "http://minio:9000",
  "s3.access_key" = "minioadmin",
  "s3.secret_key" = "minioadmin",
  "s3.region" = "us-east-1"
);
```

### MySQL 示例
```sql
CREATE CATALOG iceberg_jdbc_mysql PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "jdbc",
  "uri" = "jdbc:mysql://host:3306/iceberg?useSSL=false",
  "warehouse" = "s3://bucket/warehouse",
  "driver_url" = "mysql-connector-j-8.0.33.jar",
  "driver_class" = "com.mysql.cj.jdbc.Driver",
  "jdbc.user" = "iceberg",
  "jdbc.password" = "secret",
  "jdbc.init-catalog-tables" = "true",
  -- S3 存储配置
  "s3.endpoint" = "https://s3.amazonaws.com",
  "s3.access_key" = "<access_key>",
  "s3.secret_key" = "<secret_key>",
  "s3.region" = "us-east-1"
);
```

### SQLite 示例（本地测试）
```sql
CREATE CATALOG iceberg_jdbc_sqlite PROPERTIES (
  "type" = "iceberg",
  "iceberg.catalog.type" = "jdbc",
  "uri" = "jdbc:sqlite:/path/to/iceberg_catalog.db",
  "warehouse" = "file:///tmp/iceberg-warehouse",
  "driver_url" = "sqlite-jdbc-3.44.1.0.jar",
  "driver_class" = "org.sqlite.JDBC"
);
```

## 驱动要求
Iceberg `JdbcCatalog` 在 FE 侧通过 JDBC 访问元数据数据库。
驱动加载方式与 Doris JDBC Catalog 保持一致，避免将驱动放入 `fe/lib` 导致依赖冲突。

### 驱动存放位置
将 JDBC 驱动 JAR 包放入以下任一目录：
- `$DORIS_HOME/jdbc_drivers/`（旧版默认）
- `$DORIS_HOME/plugins/jdbc_drivers/`（新版默认）

### driver_url 支持的格式
- 相对路径：`postgresql-42.7.3.jar`（从 `jdbc_drivers` 目录加载）
- 绝对路径：`file:///path/to/postgresql-42.7.3.jar`
- 远程 URL：`http://host/postgresql-42.7.3.jar`

### 常用驱动类名
| 数据库 | driver_class |
|--------|-------------|
| PostgreSQL | `org.postgresql.Driver` |
| MySQL | `com.mysql.cj.jdbc.Driver` |
| SQLite | `org.sqlite.JDBC` |

## 验证清单
- `SHOW CATALOGS` 能看到新 catalog。
- `SHOW DATABASES FROM iceberg_jdbc`。
- `SHOW TABLES FROM iceberg_jdbc.<db>`。
- `SELECT * FROM iceberg_jdbc.<db>.<table> LIMIT 1`。
- `INSERT INTO iceberg_jdbc.<db>.<table> VALUES (...)` （写入测试）。

## 功能支持

### 查询操作
- 基础查询：`SELECT * FROM table`
- 时间旅行：`FOR TIME AS OF` / `FOR VERSION AS OF`
- Branch 和 Tag 查询
- 系统表查询：`$snapshots`、`$history`、`$files` 等

### 写入操作
- `INSERT INTO`
- `INSERT OVERWRITE`
- `CTAS`（Create Table As Select）

### 库表管理
- `CREATE DATABASE`
- `DROP DATABASE`
- `CREATE TABLE`
- `DROP TABLE`
- Schema 变更（`ALTER TABLE`）
- Partition Evolution

### 表操作
- `rewrite_data_files`（小文件合并）
- `rollback_to_snapshot`
- `rollback_to_timestamp`
- Branch & Tag 管理

## 与其他 Iceberg Catalog 类型的对比

| 功能 | HMS | REST | JDBC | Hadoop |
|-----|-----|------|------|--------|
| 元数据存储 | Hive Metastore | REST 服务 | JDBC 数据库 | 文件系统 |
| 并发控制 | HMS 锁机制 | 服务端控制 | 数据库事务 | 文件锁（受限） |
| 部署复杂度 | 需要 HMS 集群 | 需要 REST 服务 | 需要数据库 | 最简单 |
| 适用场景 | 已有 Hive 环境 | 云原生/多租户 | 独立部署 | 开发测试 |

## 兼容性说明
- Doris 当前 Iceberg 版本见 `fe/pom.xml`（1.9.1）。
- Iceberg 属性名如有变化，需要同步更新 `IcebergJdbcMetaStoreProperties`。
- JDBC Catalog 支持的数据库：PostgreSQL、MySQL、SQLite（测试用）。

## 注意事项

1. **驱动加载**：JDBC 驱动必须放在正确的目录，且 `driver_class` 必须与驱动版本匹配。
2. **数据库初始化**：首次使用时建议设置 `jdbc.init-catalog-tables=true`，Iceberg 会自动创建元数据表。
3. **敏感信息**：`jdbc.password` 会在 `SHOW CREATE CATALOG` 中脱敏显示。
4. **存储配置**：JDBC Catalog 只管理元数据，数据文件存储在 `warehouse` 指定的位置，需要配置相应的存储属性。
5. **并发控制**：JDBC Catalog 使用数据库事务进行并发控制，确保数据库支持事务（如 PostgreSQL、MySQL InnoDB）。
