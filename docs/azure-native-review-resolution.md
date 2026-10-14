# Azure native review 修复记录

范围：本次处理 B-01～B-11 中的代码兼容问题、直接单测和复现说明；不把本地单测通过当成真实云环境验收通过。

## 修复与边界

| 项目 | 本次处理 | 证据或限制 |
|---|---|---|
| B-01：SAS 过期时间 | FE/BE 都接受日期、省略秒的时间；不改签名 token，继续取较早的有效期。 | `AzureSasTokenTest`、`AzureAuthFactoryTest`。 |
| B-02：旧 OAuth2 配置 | 恢复限制检查的大小写语义，不让旧小写配置新增 REST 限制。 | `AzureCatalogPropertyPersistTest`。 |
| B-03：旧字段、账号和 endpoint | 保留已选 Azure 的自定义地址及主权云地址；认证类型不再阻断旧 SharedKey 字段配对或旧账号检查。 | `AzureFileSystemPropertiesTest`、`AzureVendedCredentialsTest`。 |
| B-04：OneLake 名称 | OneLake 工作区不套用 Blob 容器的小写规则，继续走 Hadoop。 | `AzureUriTest`、`AzureBackendViewTest`。 |
| B-05：流式导入进度 | 记录对象 key，重组文件列表时保留原 scheme/authority；前缀不一致时失败，不拼接错误路径。 | `S3SourceOffsetProviderTest`。 |
| B-06：REST FileIO | 选择凭证时保留来源；静态 Hadoop 配置不再误判为新下发凭证；保留主机端口，访问时检查账号完整性。 | `IcebergRestFileIOPropertiesTest` 覆盖响应改写、Hadoop 配置、凭证代际和非 Azure 响应；Azure FileIO 单测覆盖端口。 |
| B-07：扫描器加载 | 复用已有独立扫描器加载与本地 HTTP range 测试。 | `IcebergSerializedManifestTaskTest`；不替代线上打包部署验收。 |
| B-08：复现条件 | 补充外部 catalog/权限、开关、格式矩阵和运行命令。 | [验收说明](azure-native-iceberg-acceptance.md)；dry-run 不访问 Azure、不执行 SQL suite，完整 `.out` 基线仍待外部环境生成。 |
| B-09：脱敏 | 登记静态 ADLS key/token，按前缀隐藏任意主机的 SAS；endpoint 和过期时间仍可见。 | `DatasourcePrintableMapTest` 验证实际打印结果及大小写/端口。 |
| B-10：HTTPS 编码 glob | 复核并运行已有空格、字面百分号及 ABFS 字面路径测试。 | `AzureFileSystemTest`、`AzureUriTest`；未改既有解码规则。 |
| B-11：旧请求、WASB、token | 新 BE 接收附带 Hadoop 字段的旧 SharedKey 请求；配置容器时生成 WASB SAS key；`adls.token` 留给 Java FileIO。 | `S3ClientFactoryTest`、Azure 属性及 vended 单测。 |

额外处理：SharedKey 的新 FE 参数同时携带一致的旧 wire 字段，新 BE 拒绝冲突的双份字段；SAS 过期错误补充账号、endpoint 和过期时间，不输出 token。

明确限制：

- 只有 token 的中间配置可以保存，但在输出 BE/Hadoop/FileIO 参数前必须已经绑定账号或 endpoint；不会从任意自定义域名猜账号。
- 静态 SAS + WASB + HadoopFileIO 需要配置容器，才能生成 `fs.azure.sas.<container>.<host>`；默认 ADLSFileIO/native 数据读取不是这条 Hadoop 路径。
- `adls.token` 不转换成 BE SAS 或 SharedKey。本次保留静态 native 凭证的旧行为，不新增 native bearer-token 支持。
- SAS/OAuth2 native 请求要求先升级所有 BE；SharedKey 保留双向 wire 兼容。升级/回滚步骤见验收说明。

## 仍待完成的合入验证

本次最终 FE 定向运行共 **401** 个用例通过（355 Azure、7 REST FileIO、32 FE 调用方、7 Java scanner），无跳过；使用 `run-fe-ut.sh`，日志保存在本地 `output/azure-review-final-fe-ut.log`。FE Checkstyle 与 C++ 格式检查通过。

BE 使用 `run-be-ut.sh --run -j 16 --filter=...` 完成 ASAN 定向构建和 **19** 个用例（12 AzureAuthFactory、7 S3ClientFactory），全部通过；日志保存在本地 `output/azure-review-final-be-ut.log`。FE 重新生成头文件导致的旧 PCH 缓存错误已通过重建 PCH 后重跑解决。

1. **真实 Azure 回归**：本次只执行 fixture dry-run；SharedKey、SAS、OAuth2 的真实端到端 suite 需要外部 catalog 和权限。
2. **M-00 合并基线**：本地 `master` 与 `origin/master` 已分叉。对 `origin/master`（`0aea7140de6`）的只读合并检查仍报告 7 个冲突；本次没有擅自把另一个基线合入工作分支。
3. **clang-tidy**：已执行标准脚本；工具链报 `stddef.h` 缺失和已有 `NOLINTEND` 配对错误，未得到完整静态检查通过结论。正常 ASAN 单测构建通过，不能用它替代 clang-tidy。

## 自审检查点

| 检查点 | 结论 |
|---|---|
| 目标与证明 | 已有针对错误路径的 FE/BE 单测；外部验收和合并冲突未宣称完成。 |
| 改动范围 | 修改集中在凭证解析、映射、URI、打印及对应测试，没有扩展通用存储 SPI。 |
| 并发与锁 | 没有新增线程或锁；请求内选择结果不跨表缓存，现有客户端缓存锁序不变。 |
| 生命周期/静态初始化 | 未新增跨 TU 全局依赖或资源所有权；REST 的 ThreadLocal 仍在 finally 恢复/清除。 |
| 新配置 | 没有新增生产配置项；验收使用已有的 opt-in fixture 参数。 |
| 兼容性 | 保留旧字段与回放行为，SharedKey 双份 wire 必须一致；SAS/OAuth2 的升级限制已写明。 |
| 平行路径 | 同时检查 FE/BE SAS、native/Hadoop/FileIO、Blob/DFS、旧/新 SharedKey 路径。 |
| 特殊条件 | 条件用于确定的旧协议/认证来源/账号范围，冲突和缺失信息显式失败。 |
| 测试覆盖 | 包含正向、过期、混合凭证、别名、端口、脱敏和来源判断；云端 E2E 仍待执行。 |
| 测试结果文件 | 未手写或提交本地外部环境生成的 `.out`；实际结果由标准测试脚本产生。 |
| 可观测性 | 过期错误增加安全上下文；打印保留非秘密信息，未新增高频 INFO 或指标。 |
| 事务与持久化 | 无事务协议/EditLog 格式变更；回放测试约束历史属性重建。 |
| 数据修改/原子性 | 未改提交、可见版本或 delete bitmap；本次修的是访问参数和流式对象 key。 |
| FE→BE 传递 | 未新增 Thrift 字段；SharedKey 在已有属性 map 中携带匹配的兼容字段。 |
| 性能与内存 | 新逻辑只处理小型属性 map、token 和当前批次文件列表；无新大内存或循环内网络访问。 |
| 其他问题 | 上述三个未完成验证保留为待办；不据本次结果声称其他 review 优化项全部解决。 |
