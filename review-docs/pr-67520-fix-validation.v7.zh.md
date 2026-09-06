# PR #67520 修复与实际验证记录 v7

日期：2026-09-06。基于工作树 HEAD `12e48c403e0a86aa7be73f6e6211af879cbbee73`。
本文件补充 v6，不覆盖历史需求、决策或评审记录。仅本地提交，不 push。

## 修复结果

- 新 follower 对未确认新协议的旧 master 返回结果，在发给客户端前适配 metadata 和最终 EOF。保留 warning/status；不再用成功状态码保护判断漏掉 EOF/1105，也不把原本可用的查询改成升级错误。
- DML/DDL 保留原始 OK，包括 affected rows、warning、info、label、txnId，不重新构造有损 OK，不重试已执行 SQL。
- Arrow Flight SQL 转发不访问其不支持的 MySQL channel。
- 握手 serializer 使用双方 capability 交集；通过可选 Thrift 字段 1009 转发完整协商 flags，保留旧请求兼容行为。
- connectionAttributes=none 采用已接受的旧 Connector/J 兼容策略，移除拒绝匿名 cursor 的新错误。
- 独立 review 发现 capability 交集会误拒绝 FE 文件导入：两个 LOAD handler 均改成只对客户端 LOCAL 上传要求 LOCAL_FILES。去掉无调用的转发 getter；未引入新配置或通用框架。

## 实际构建和单测

`./build.sh --fe -j8` 成功，含 Checkstyle，记录 `build-cursor-final.log`。

通过 `run-fe-ut.sh` 执行 18 个相关测试类：协议/转发/cursor、握手和 capability、认证分流/default/LDAP/plugin/integration/credential，以及两个 LOAD handler。
完整集合 113 项中最初有 1 项因新测试缺少 InternalCatalog mock 初始化报错；修复测试 setup 后，`./run-fe-ut.sh --run MysqlLoadCommandTest` 的 2 项全部通过。其余 111 项在完整集合中通过，最终这 113 项均有成功执行记录，未宣称完整集合在修复后再次整体执行。
证据：`test-cursor-final.log`、`test-load-recheck.log`。包级测试含 24 种旧 master metadata/cursor/驱动版本/空非空组合、幂等转换、OK/ERR 原样保留、状态/warning、实际 Arrow context 和 capability 恢复。

## 真实客户端和集群验证

所有服务在本工作树 output 内运行，端口独立。基线 FE 为保留的原始二进制 `4da5164ebc3`；候选 FE 为 build.sh 构建结果。四个端点分别为候选直连、基线直连、新 follower→新 master、新 follower→旧 master；后两者强制转发。版本显示使用旧生成元数据，不能据显示字符串判断候选 jar 是否更新。
测试 BE 为已有镜像的 4.1.3-rc02，设置 be_exec_version=10 兼容执行，能力限制见下节。

| 实测内容 | 结果与证据（output/protocol-validation 下） |
|---|---|
| Connector/J 5.1.49、8.0.28、8.0.33、8.2.0、8.4.0、9.0.0、9.4.0、9.5.0、9.6.0，MariaDB 3.5.6；基线/候选；明文/要求 TLS/校验证书 | 60 组通过，driver-results.json |
| 上述驱动；同版本和混版本 follower；三种传输模式 | 60 组通过，follower-results.json |
| Connector/J 6.0.6 基线/候选、三种模式 | 6 组均握手失败，基线同样失败；不算通过，也不扩展历史支持 |
| 8.2/9.5，四端点，明确 TLS 1.2/1.3 | 16 组通过，tls-results.json |
| MySQL Shell 8.0.44/9.5，四端点，普通密码登录、明文/证书校验 | 16 组通过，shell-results.json；不是 OIDC 登录 |
| PyMySQL 1.1.1，实际未协商 DEPRECATE_EOF，四端点明文/TLS | 8 组通过，legacy-eof-results.json |
| 8.2/9.5，四端点，SQL 报错后同连接继续查询 | 8 组通过，error-reuse-results.json |
| 8.0.28/8.2/8.4/9.4，connectionAttributes=none，候选/两个 follower | 12 组通过，no-attributes-results.json |
| 基线/候选，LOCAL_FILES 关闭时 FE 文件 INFILE，以及开启时客户端 LOCAL INFILE | 4 次均导入 2 行、0 warning，load-*-server.log / load-*-client.log |
| 两个 follower 实际 INSERT 的 OK info | label/status/txnId 保留，follower-insert-info.log / mixed-insert-info.log |

JDBC 每组使用 cursor 开关 × server prepare 开关 × fetchSize 0/1/10000 共 12 个连接配置，反复执行 Statement/PreparedStatement 空→非空→空，另测命名用户错误密码及时返回；连接/读超时约束防止无限等待。错误后连接复用的补充矩阵在原矩阵之后执行。脚本、驱动和逐组日志保留于 output/protocol-validation，测试凭据仅用于临时测试集群。

## Regression 与未完成验证

- `./run-regression-test.sh --run -d prepared_stmt_p0 -s cursor_fetch_empty_result -forceGenOut` 生成期望输出，再正常比较执行通过。`.out` 末尾空行由框架生成，未手写修正；git diff --check 仅报告这处空行。
- 整个 prepared_stmt_p0：6 个套件，cursor_fetch_empty_result、prepared_show 通过，其他 4 个失败于旧 BE 的 `Unsupported exec type in pipeline: Invalid plan node type`。相同四个套件在基线 FE + 相同版本 BE 也失败，同样的错误。证据：`regression-prepared-all.log` / `regression-prepared-baseline.log`。这些套件不是通过项。
- Arrow JDBC 经真实 follower 执行 test_ddl 通过：`regression-arrow-follower.log`。
- test_authentication_integration_auth 在查询 information_schema.authentication_integrations 时遇到旧 BE `no match column for this column(NAME)`，未完成：`regression-auth-integration.log`。
- 尝试以 `BUILD_TYPE=ASAN ./build.sh --be -j8` 构建匹配当前源码的 BE。在工作树内补齐 OpenMP 后 CMake 成功，但 Ninja 缺 crc32c、libevent_openssl、ADBC、Arrow、AWS、Lance 等依赖，无法完成 BE 构建。`build-be-validation.log` 保留失败。未为绕过问题修改产品计划生成或改写测试期望。
- 当前 OSS checkout 不含产品 OIDC provider / 新 TLS extension 完整实现。因此完成的是公共认证单测和真实普通密码、MySQL Shell、现有 SSL/TLS 通路验证；实际 OIDC token 登录、外部 LDAP/custom 服务、新 TLS 扩展端到端尚未验证。不能以单测替代这些产品集成验证。

上述限制意味着本轮不能宣称“全量回归完备”或保证客户绝无回退。下一步需匹配当前源码的 BE 依赖环境，以及产品 OIDC/TLS 扩展与测试 IdP，补跑四个 prepared 套件、authentication integration metadata 和实际 OIDC 登录。

## 既定兼容边界

旧 follower 不携带 cursor intent，新 master 无法恢复丢失的信息；保留历史普通 prepared 行为，不新增拒绝。旧 follower 上的历史 cursor hang 未修复，需要升级 follower 才能得到本次修复。真实混部矩阵覆盖的是新 follower→旧 master；没有把相反方向称为已解决。
匿名现代 Connector/J 9.x 仍按此前决定不作为新增支持目标；匿名策略优先此前支持的旧驱动。不能将这项边界解释成所有匿名驱动均兼容。

## 独立深度 review

按要求只启动一个独立 reviewer，完成两轮，最终 PASS、0 未解决代码问题。第一轮 LOAD_FILES 回退已修复并经单测和真实 baseline/candidate 导入对照验证。review 涵盖 code-review skill 的各检查点；详细逐项结论保留在 `output/protocol-validation/deep-review.md`，明确区分源码 PASS 与尚未完成的产品集成测试。
