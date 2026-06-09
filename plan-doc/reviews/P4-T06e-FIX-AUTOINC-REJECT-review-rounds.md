# P4-T06e · FIX-AUTOINC-REJECT — review 轮次记录

> issue=`P2-8 FIX-AUTOINC-REJECT`（DG-5 / F24, minor, regression）
> design=`plan-doc/tasks/designs/P4-T06e-FIX-AUTOINC-REJECT-design.md`
> 用户定方向：**加 SPI 字段 `ConnectorColumn.isAutoInc`**（full parity），非 deviation。

## 设计对抗验证（design workflow `weepgfhwu`）

verdict = **approve-with-nits**（0 mustFix，parityCorrect=true，blastRadiusComplete=true，testRule9=true，openQuestions=[]）。

## 实现

**改 3 生产 + 3 测试文件**（additive，无 SPI 方法签名变更）：
1. SPI `ConnectorColumn.java`：加 `private final boolean isAutoInc`；新 7 参 ctor（唯一全赋值）；6 参 ctor 改委托 7 参 `isAutoInc=false`；5 参不变（→6→7）；getter `isAutoInc()`；equals/hashCode 纳入 isAutoInc。
2. fe-core `CreateTableInfoToConnectorRequestConverter.convertColumns`：传 `d.getAutoIncInitValue() != -1` 作第 7 参（auto-inc 判别同 `toSql:225`）。
3. 连接器 `MaxComputeConnectorMetadata.validateColumns`：循环首加 `if (col.isAutoInc()) throw DorisConnectorException("Auto-increment columns are not supported for MaxCompute tables: " + name)`（镜像 legacy `:422-425`）；方法 private→package-private（+test-only 注释，因 createTable 入口需 live ODPS handle，连接器测模块无 mockito/fe-core，按 `MaxComputeBuildTableDescriptorTest` 离线 idiom 直调）。聚合列半（legacy `:426-429`）out-of-scope（F31，非-OLAP key 路径已覆盖），不加。

**守门**：**全连接器 compile**（es/hive/hms/hudi/iceberg/jdbc/maxcompute/paimon/trino + fe-core）BUILD SUCCESS——12 个 `new ConnectorColumn(` call site 全编译（additive default false，唯 converter 置 true）；UT ConnectorColumnTest 2/2 + MaxComputeValidateColumnsTest 2/2 + ConverterTest 9/9（7+2）；checkstyle 0×3；import-gate 净；mutation 三向红：(A) 删连接器 auto-inc throw→`autoIncColumnIsRejected` 红；(B) converter 回退 6 参→`autoIncInitValueIsPropagated` 红；(C) equals 去 isAutoInc→`equalsAndHashCodeDistinguishAutoInc` 红。
（操作注：mutation 还原一度因 `cd .../fe` 持久 + 相对路径 cp 失败未还原 ConnectorColumn，绝对路径强制还原后 final green 复验 2/2+2/2+9/9——见 auto-memory `doris-build-verify-gotchas`。）

## Round 1（impl 对抗 review，workflow `wj0pwt0u7`，4 lens）

6 finding 全 **nit**（0 mustFix/0 shouldConsider）：
- nit：converter 测 mock 掉 ColumnDefinition（蓄意——auto-inc ctor 牵 ColumnNullableType；mutation B 证非真空）。
- nit：converter 测漏 `autoIncInitValue==0` 边界（`0 != -1` 平凡成立，marginal）。
- nit×2：hashCode 不等断言"stricter-than-contract"（对固定输入确定性——Objects.hash 含翻转布尔必不同；reviewer 注"works in practice"）。
- nit：无测钉 auto-inc 检查 vs 重名检查的顺序（皆抛，仅"既 auto-inc 又重名"edge 才有别）。
- nit：读路径 `ConnectorColumnConverter.toConnectorColumn` 不带 isAutoInc（**正确**——MC 读表本不可能 auto-inc，false 即对；"in-scope OK"非缺陷）。

**收敛**：0 mustFix；6 nit 皆接受（测试已由 3 mutation 钉 3 属性，非真空）。

## 累计结论

- **根因**（DG-5/F24）：legacy `validateColumns:422-425` 显式拒 auto-inc；翻闸后 `ConnectorColumn` 无 isAutoInc 载体 → flag 在到连接器前被丢 → `CREATE TABLE (id INT AUTO_INCREMENT)` 静默建普通列（数据模型回归）。enabling 条件：nereids `ColumnDefinition.validate(isOlap=false)` 不拒 bare auto-inc（仅 generated 列拒，`:666-667`），故 `P4-maxcompute-migration.md:117` 的"nereids 已拒"对 auto-inc 为假。
- **修**：additive `ConnectorColumn.isAutoInc`（7 参 ctor，默认 false→12 call site 零行为变更，唯 converter 置 true）+ converter 透传 `getAutoIncInitValue() != -1` + 连接器 validateColumns 拒（镜像 legacy 文案）。
- **真值闸**：UT 充分（纯 FE 校验，throw 在任何 ODPS RPC 前，无需 live ODPS）+ mutation 三向红 + 全连接器 compile。
- **doc-sync 随后续**：更正 `P4-maxcompute-migration.md:117` 假声明（nereids 未拒 auto-inc）、decisions-log 登记 ConnectorColumn.isAutoInc 字段、DG-5 状态。
