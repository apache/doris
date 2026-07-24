# 类别 B — Schema / 列身份粒度

## 范围说明与基线声明

本类别聚焦 catalog SPI 迁移中与 **schema 转换、列身份粒度、嵌套字段属性传播** 相关的发现,共 6 条:#2(getColumnHandles 无 snapshot 参数)、#10(iceberg 列 DEFAULT 未读 + initial/write 塌陷)、#11(paimon CREATE 丢 per-列类型参数)、#12(paimon 嵌套 null/注释写路径)、#19(嵌套 null/注释读路径)、#22(iceberg 谓词下推用 latest 非 pinned schema)。

**基线声明**:本核实基于当前工作树 branch `catalog-spi-2-lvl-cache`,逐条独立 Read/Grep 当前代码 + 对抗复核(investigate → verify 两阶段),非轻信原 survey。凡涉及 fe-core 已删除的 legacy 文件(如 `IcebergUtils.java`/`PaimonUtil.java`/`DorisToPaimonTypeVisitor.java`)的历史逐行比对,任务约定不 `git show` 旧分支,故此类 "与 legacy 逐行一致" 断言仅由连接器现存注释与单测间接佐证,已在对应条目标注 provenance。

## 总表

| # | 严重 | 原报告结论 | 核实结论 | 一句话 |
|---|------|-----------|---------|--------|
| 2 | 🔴 | getColumnHandles 无 snapshot 参→time-travel 跨 RENAME 丢列,paimon 崩/错、iceberg 幸免 | **CONFIRMED**(高) | 机制成立、iceberg 幸免/paimon 更脆成立;但 headline 单列示例被空集回退救活,真正 repro 是**混合投影**,主导后果是 **BE crash** 非静默错数据 |
| 10 | 🟠 | iceberg parseSchema 硬编码 null 从不读 initialDefault/writeDefault;initial vs write 两 default 塌成一个槽 | **PARTIAL**(高) | 机械事实(单槽/parseSchema 传 null/writeDefault 未接)成立,但标题夸大——initialDefault 经 #65502 独立 thrift 字段被读并下发 BE,无塌陷;真缺口仅 writeDefault 未接 + Column 元数据默认恒 null,低危 |
| 11 | 🟠 | paimon CREATE:CHAR/VARCHAR 长度塌成 MAX、DATETIME scale 塌成默认微秒、decimal 保留 | **PARTIAL**(高) | 三条行为事实全对,但定性为 SPI 错配被高估——是有注释+mutation 单测钉死的**刻意 legacy parity** |
| 12 | 🟠 | paimon 写路径 column 粒度,嵌套 struct 子字段 nullability+comment 丢失 | **PARTIAL**(高) | nullability **已修**(FIX-L13);comment 属实但为已签字 display-only 偏差 DV-035,低危 |
| 19 | 🟡 | 读路径 column 粒度,嵌套 struct 子字段 nullability+comment 丢失 | **PARTIAL**(高) | 代码事实属实但**功能影响为零**——legacy parity + 读路径一律构造 nullable;非迁移引入的回归 |
| 22 | 🟡 | iceberg 谓词转换用 latest 非 pinned(time-travel)schema→下推丢失(仅丢下推、不错结果) | **CONFIRMED**(高) | 事实断言全对且有注释锁死为 legacy parity;仅"改名+time-travel+谓词命中改名列"罕见交集触发,只丢文件级剪枝不丢正确性——作现象成立、作待修 bug 不成立(有意设计) |

---

## #2 getColumnHandles 无 snapshot 参数

**核实结论**:**CONFIRMED**,高置信。两阶段一致确认机制成立。verify 与 stage-1 在**具体 repro 示例与后果措辞**上分歧,**最终以 verify 为准**(verify 独立 trace 了空集回退路径,发现 stage-1 的招牌单列示例大概率不触发,须换成混合投影 repro;且主导后果应校准为 BE crash 而非 "crash 或静默错数据二选一")。核心 verdict、根因、iceberg 幸免/paimon 更脆的对比不受影响。

**原报告主张**:`getColumnHandles` 从 `table.schema()`(latest)建、按名字 key,`buildColumnHandles` 做 `get(slot 名)` miss 就静默丢列;time-travel 跨 RENAME 时 query slot 带 pin 的旧名、latest map 里没有 → 列被丢;iceberg 因 pin 下从完整 pinned schema 重建 field-id dict 才幸免(防 BE StructNode DCHECK crash),paimon native projection 按名消费无此 fallback 故更脆。

**核实过程**(两阶段独立读同一批 file:line,结论吻合):
- `PluginDrivenScanNode.java:1263-1269` —— `buildColumnHandles()`(1263)在 `pinMvccSnapshot()`(1269)**之前**跑,`currentHandle` 此刻未 pin。
- `PluginDrivenScanNode.java:1917-1934` —— `buildColumnHandles` 用 `metadata.getColumnHandles(session, currentHandle)` 得 allHandles,对每 slot 做 `allHandles.get(slot.getColumn().getName())`,`if(ch!=null) selected.add`,miss 静默丢,**无 fail-loud**。
- `ConnectorTableOps.java:132-135` —— `getColumnHandles(session, handle)` 仅二参,**无 snapshot 形参**,即根因:SPI 方法看不到 pin,只能拿 latest。
- `PaimonConnectorMetadata.java:955-967` —— `resolveTable(handle)` 取 latest rowType,按 name key,`PaimonColumnHandle(name, i)`,`i` 为序号非稳定 field id。
- `PaimonConnectorMetadata.java:284-307` —— 3-arg `getTableSchema(handle, snapshot)` 走 `schemaAt(schemaId)` 建 pinned(旧名)schema,证实 FE 侧 slot 绑旧名。
- `PaimonTableResolver.java:64-88` —— `resolve` 只取 transient/reload base|branch,**不 apply scanOptions**(pin 只在 scan 侧 `resolveScanTable` 的 `table.copy(scanOptions)`,`PaimonScanPlanProvider.java:329-340` 施加)。
- `PluginDrivenMvccExternalTable.java:398-407` —— `applySnapshot → getTableSchema(3-arg) → pinnedSchema` 绑定,query slot 携旧名。
- `PaimonScanPlanProvider.java:485-510` —— native 投影 `fieldNames=rowType`(旧名)、`columns.indexOf`、`filter(i>=0)`、`if(projected.length>0)` 才 `withProjection`;`1543-1635` dict 的 -1 条目经 `resolveCurrentSchemaFields → selectCurrentSchemaFields` 遍历 columnNames。
- `IcebergScanPlanProvider.java:1594-1618` —— 注释**明确描述同一 bug**:query slots carry PINNED names、generic node builds handles from LATEST、renamed column would be dropped → BE StructNode DCHECK crash,故 `if(iceHandle.hasSnapshotPin())` 分支用 `pinnedSchema(table)` 全量重建 dict;且注释说 iceberg 投影 BE-tuple-driven、columns 只喂 dict → 丢列无害。原报告 "~1268" 行号线索过时,现 ~1594,内容吻合。

**背景**:通用节点在 MVCC pin 之前就调 `buildColumnHandles`,用未 pin 的 handle 拿 `getColumnHandles` 的 latest-keyed map;而时间旅行查询的 slot 名来自 pinned schema。根因是 SPI `getColumnHandles(session, handle)` 无 snapshot 形参,看不到 pin,只能按 latest 建 handle。名字对不上即静默丢列。

**影响(具体示例)**:
表 t 有列 c1、c2。快照 S1 后执行 `ALTER TABLE t RENAME COLUMN c1 TO c1_new`(paimon 支持,产生新 schemaId,列名变但数据不变)。

- **verify 修正后的确定触发 repro —— 混合投影**:
  ```sql
  SELECT c1, c2 FROM paimon_cat.db.t FOR VERSION AS OF <S1>;
  ```
  pinned schema(S1)含旧名 c1、c2 → slot 绑 c1、c2。`getColumnHandles` 走 latest → map 只有 `c1_new`、`c2` → `allHandles.get("c1")` 返回 null → **c1 被丢**,`columns=[c2]` 非空。下游:投影只读 c2、dict 的 -1(target)条目 `columnNames=["c2"]` 只含 c2、c1 被排除(与 resolvedFields 是否 pinned **无关**,因为只遍历 columnNames)。BE 的 c1 scan slot 在 -1 条目查无 field-id → `StructNode children.at("c1")` `std::out_of_range` → **SIGABRT**。

- **stage-1 的招牌单列示例大概率不触发(verify 关键修正)**:
  ```sql
  SELECT c1 FROM paimon_cat.db.t FOR VERSION AS OF <S1>;  -- 只投影被改名那一列
  ```
  c1 被丢后 `columns` 变**空**,两个下游消费者都有空集回退:(a)投影 `PaimonScanPlanProvider.java:508` `if(projected.length>0)` 空则不 withProjection → 读全列(良性);(b)dict -1 条目 `selectCurrentSchemaFields`(1614-1616)`if(columnNames.isEmpty()) return resolvedFields` 回退全量。若该 resolved schema 是 pinned(旧名,连接器 javadoc 1578-1589 断言 "snapshot-pinned schema wins…renamed column resolves its pinned id"),则 -1 条目含 c1 → BE 找得到 → **不崩、返回正确数据**。故单列改名 time-travel 被空集回退救活。

- **后果措辞校准(verify)**:`SCHEMA_EVOLUTION_PROP` dict 已 emit(非 force_jni、非系统表时),BE 走 field-id 匹配、以 -1 条目为 target key;target 缺列是**硬 miss → 崩溃**,不是名字回退读 NULL。静默错数据那条只在 dict 整体缺失(force_jni_scanner / 非 FileStoreTable)时才走名字匹配,与本 pin 路径不重合。故本缺陷**主导后果是 BE crash**,"静默错数据" 应删/弱化。

- **对照 iceberg 同查询**:pin 下忽略 columns、用 `pinnedSchema(table)` 全量重建 dict(`IcebergScanPlanProvider.java:1615-1618`),投影 BE-tuple-driven,丢列无害 → iceberg 正常返回。

**严重度校准**:机制为真,命中即 BE crash,属 S。但触发面比 stage-1 估计的**再窄一档**:不仅需 time-travel 到 rename 前快照,还需该查询是**混合投影**(改名列与存活列同现)——纯单列改名投影不炸。当前无 paimon rename+time-travel 的回归/单测覆盖(regress 无命中;现有 `selectCurrentSchemaFields` 单测只在 columns 已正确前提下测 dict,测不到上游丢列),属潜伏未测缺陷。

**残留不确定性(verify 自陈)**:paimon time-travel 下 `table.copy(scanOptions).schema()` 究竟返回 pinned 还是 latest 未穿透 paimon SDK 源核实;单列被救活依赖 resolvedFields=pinned 的 javadoc 断言。若实际返回 latest,单列例子也会崩(bug 更严重),不改 CONFIRMED。**混合投影 repro 与 crash 结论对该不确定性完全 robust**(不依赖 resolvedFields 是 pinned 还是 latest)。

**修复方案**:
1. **根因修(推荐,框架级、覆盖全连接器)**:给 SPI `getColumnHandles` 增加 snapshot/pinned-handle 形参,或在 `PluginDrivenScanNode` 把 pin 提到 `buildColumnHandles` 之前,使 handle 按 pinned(旧名)schema 建 → slot 名与 map key 一致、不再丢列。会碰 SPI 签名(正是本发现标题所指),需评估与 fe-core-source-isolation 铁律的关系。
2. **局部修(镜像 iceberg 先例,paimon 侧)**:检测 `paimonHandle.getScanOptions()` 非空(pin)时,投影与 dict 的列集改从 pinned `table.rowType()` 全量重建、忽略 latest-keyed 的 lossy columns。注意 paimon 投影须产出与 BE tuple 对齐的真实序号,不能像 iceberg 那样只喂 dict,需同步修 `planScanInternal` 的 projected 计算。
3. **兜底**:让 `buildColumnHandles` 在 slot 名 miss 时 fail-loud(抛错而非静默丢),把静默错数据/隐蔽崩溃升级为可见失败。
4. 补 paimon **混合投影 + FOR VERSION AS OF** 的 e2e 回归(参照 hudi HD-C5b、iceberg T07 的 pin 测试),按 verify 修正采用混合投影场景。

---

## #10 iceberg 列 DEFAULT 未读 + initial/write 塌陷

**核实结论**:**PARTIAL**,高置信。两阶段一致:主张的机械事实成立,但标题把两件不同的事混为一谈导致严重夸大。verify 与 stage-1 完全一致(agrees=true,final_verdict=PARTIAL),无实质修正。

**原报告主张**:iceberg `parseSchema` 硬编码 null、从不读 `initialDefault()`/`writeDefault()`;且 `ConnectorColumn` 单 `defaultValue` 槽,连 iceberg v3 的 initial(填历史行)vs write(新写)两 default 都塌成一个(P0 转换器传 null 已在 tip 修)。

**核实过程**(两阶段独立读同一批 file:line,结论吻合):
- `IcebergConnectorMetadata.java:1890-1897` —— `parseSchema` 对每列构造 `ConnectorColumn`,第 5 个参数 `defaultValue` **硬编码 null**(`1895`);方法体内不读 `initialDefault`/`writeDefault`。故 Doris 侧 `Column.defaultValue` 对每个 iceberg 列恒 null。
- `ConnectorColumn.java:34`(字段)/`159`(getter)—— **确只有单个 `defaultValue` 槽**。
- `ConnectorColumnConverter.java:68` —— 现已透传 `cc.getDefaultValue()`(而非硬编码 null),即主张括号里 "P0 转换器传 null 已修" **属实**。
- `IcebergSchemaUtils.java:293-301`(#65502)—— **initialDefault 并非无人读**:`buildField` 对每个 field 读 `field.initialDefault()`,序列化进下发 BE 的 thrift schema 字典 `setInitialDefaultValue`(binary/UUID/FIXED 走 base64 无损载体)。BE 据此在 "列在旧数据文件中缺失" 时用 initialDefault 物化该列(尤其 equality-delete 的 key 列)而非回填 NULL。有专门单测 `IcebergSchemaUtilsTest.java:274` `initialDefaultsAreCarriedOntoDictFields` 守护。
- 全仓 grep `writeDefault` **零命中** —— `writeDefault()` 确无任何读取点。
- 老实现 `IcebergUtils` 已从 fe-core 删除,无法直接比对(约定不 git show 旧分支)。

**为何定性 PARTIAL 而非 CONFIRMED 的 S**:主张把两件不同的事混为一谈。

- **属实部分**:`parseSchema:1895` 硬编码 null → Doris `Column` 元数据层默认值对每个 iceberg 列恒 null(DESC TABLE 不显示默认值;INSERT 省列不套用 iceberg write default);`ConnectorColumn` 确单 `defaultValue` 槽;`writeDefault()` 全仓零读取点,确未接入。
- **不成立/夸大部分(降级关键)**:标题 "从不读 initialDefault()" **错**——真正 load-bearing 的读时默认填充语义走 `IcebergSchemaUtils.buildField`(#65502)独立读并下发 BE。且 "initial 与 write 两 default 塌成一个槽" 也不成立:`ConnectorColumn.defaultValue` 本就是 null,没有任何值被塌进去;initialDefault 经**独立 thrift 字段**(`TField.initialDefaultValue`)传递,与 `ConnectorColumn.defaultValue` 无交集,不存在混同。报告把一个 "读时默认填充已由 #65502 正确实现" 的现状,误述为 "完全未读 + 语义塌陷" 的现行缺陷,方向性失真。

**真实剩余缺口(严重度低,具体示例)**:
```sql
-- iceberg v3 表 t,列 c 声明了 write default(如 DEFAULT 42),向其 INSERT 省略 c:
INSERT INTO iceberg_cat.db.t (other_col) VALUES (...);  -- 省略 c
```
1. **writeDefault 未接**:上述 INSERT 不会套用 iceberg 的写默认值 42——若 c 是 NOT NULL 会报错,否则落 NULL(而非 42)。
2. **Column 元数据层默认值恒 null**:`DESC iceberg_cat.db.t` 不展示列 c 的默认值。

二者均为边缘写路径/展示层能力,且很可能与老实现平价(Doris 外部 iceberg 表历来不把 iceberg 列默认暴露到 `Column.defaultValue`);#65502 的 BE 填充反而是本分支的增强。读时默认填充(旧文件缺列)已正确工作,不受影响。

**修复方案(可选增强,非纠错)**:接 `writeDefault` —— 在 `parseSchema` 用 `field.writeDefault()` 填 `ConnectorColumn.defaultValue`(转 Doris 字符串形式),使 Column 元数据默认与 INSERT 省列语义生效;`initialDefault` 保持现走字典路径不动。属可选行为增强,不构成正确性缺陷修复。

---

## #11 paimon CREATE 丢 per-列类型参数

**核实结论**:**PARTIAL**,高置信。两阶段一致:三条行为事实全部成立且为现行代码,但把它定性为 "SPI 抽象错配/迁移引入的缺陷(🟠/F)" 被高估——这是有注释+mutation 单测钉死的**刻意 legacy parity**。verify 无实质修正,仅补两点非否决性内容。

**原报告主张**:paimon CREATE 时 CHAR/VARCHAR 长度塌成 `VarChar(MAX)`、DATETIME scale 塌成默认微秒;decimal 精度保留。

**核实过程**:
- 写向映射在 `PaimonTypeMapping.toPaimonType(ConnectorType)`(`fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTypeMapping.java:213-271`):
  - CHAR/VARCHAR/STRING(`227-232`)一律 `return new VarCharType(VarCharType.MAX_LENGTH)`,注释 "Legacy parity: all char-family types collapse to VarChar(MAX); declared length is intentionally dropped"。声明长度丢弃。
  - DATETIME/DATETIMEV2(`243-248`)`return new TimestampType()` 无参,Paimon 默认 precision=6(微秒),注释 "the datetime scale is intentionally dropped ... plain timestamp (NOT LocalZonedTimestampType)"。
  - DECIMAL*(`236-242`)`new DecimalType(type.getPrecision(), type.getScale())`,精度+scale 都保留。
- CREATE 路径 trace 通(verify):`PaimonConnectorMetadata.createTable(821-834) → PaimonSchemaBuilder.build(824) → PaimonSchemaBuilder.java:127` `PaimonTypeMapping.toPaimonType(col.getType()).copy(col.isNullable())`。确认 CREATE DDL 确经此写向映射。
- 单测钉死:`PaimonTypeMappingToPaimonTest.java` 具名方法 `charFamilyCollapsesToVarcharMaxDroppingLength` 断言 CHAR(10)/VARCHAR(20)/STRING → `VarCharType(MAX_LENGTH)`(mutation 注释 "honoring the declared length ... makes these red");`datetimeDropsScaleToNoArgTimestamp` 断言 `DATETIMEV2(3) → TimestampType()` 且 `getPrecision()==6`。stage-1 引的行号 77-79/89-93 是近似,实际为具名方法,非实质错误。
- **读向非对称(verify 补充)**:`fromPaimon` 的 `toVarcharType`(`113-122`)保留 len(len<=0 || len>65533 才 STRING,否则 VARCHAR(len))、`toCharType`(`125-131`)保留 len。证明这是**写向单向窄化**。

**背景**:该行为是旧 fe-core `DorisToPaimonTypeVisitor`(已随 commit 186f0255631 从 fe-core 删除)的反向复刻。连接器类注释(`194-204`)显式声明 "faithful reverse of the legacy DorisToPaimonTypeVisitor"。**provenance 备注(verify)**:"新连接器逐行忠实复刻旧文件第 94-101 行" 这一历史比对因旧文件已删、约定不 git show,未能独立核实;但当前代码三处注释明确写 intentional/Legacy parity + mutation 单测锁定,足以支撑 "刻意保留而非迁移引入的 bug" 的定性。

**影响(具体示例)**:在 paimon catalog 上执行
```sql
CREATE TABLE t (c1 VARCHAR(20), c2 CHAR(4), c3 DATETIME(3), c4 DECIMAL(10,2)) ...;
```
落到 Paimon 侧的 schema:c1/c2 → VARCHAR(2147483646)(MAX_LENGTH,长度约束丢失),c3 → TIMESTAMP(6)(用户要的 scale=3 变成 6),c4 → DECIMAL(10,2)(正确)。**round-trip 非对称退化(verify 补充)**:因写向持久化成 2147483646 > 65533,再读回该列走 `toVarcharType` 会得到 **STRING** 而非 VARCHAR(20)——即 `CREATE TABLE t(c1 VARCHAR(20))` 往返后 c1 在 Doris 侧显示为 STRING(既不是原声明 VARCHAR(20),也不是 VARCHAR(MAX))。该退化仅影响 DDL 声明的长度/scale 透传与元数据展示,**不造成数据损坏或查询报错**(读向另有映射)。

**为何定性 PARTIAL 而非 CONFIRMED 的 F**:
1. 非 SPI 迁移引入的回退:新旧行为一致,迁移前后不变。
2. 被**刻意**保留:代码三处注释 "intentionally dropped / Legacy parity",且有 mutation-style 单测防止有人 "修好" 而破坏平价。
3. 非 "SPI 抽象错配":`ConnectorType` 本身携带 length/scale(decimal 分支正常使用 `getPrecision/getScale`),是映射逻辑主动选择丢弃,与抽象层能力无关。

**修复方案(非本任务必需,需用户拍板)**:在 `227-232` 把 CHAR → `new CharType(len)`、VARCHAR → `new VarCharType(len)`,在 `243-248` 把 DATETIME → `new TimestampType(scale)`。但这会打破 legacy parity 并使现有单测变红,须同步改测试并确认是有意的行为升级(参照本项目 hudi/paimon "完整对齐 vs 有意提升" 的签字先例),不能作为 surgical bugfix 静默改。

---

## #12 paimon 嵌套 null/注释(写)

**核实结论**:**PARTIAL**,高置信。两阶段一致:主张的两个事实断言**一真一假**——嵌套 nullability 丢失**不成立(已由 FIX-L13 修复)**,嵌套 comment 丢弃**成立但为已签字接受的 display-only 偏差 DV-035 M10.1**,低危。verify 无实质修正,仅补路径全名。

**原报告主张**:paimon 写路径 column 粒度而非 per 嵌套 field,嵌套 struct 子字段的 nullability 与 comment 均丢失。

**核实过程**:
- `PaimonTypeMapping.toPaimonRowType`(`PaimonTypeMapping.java:273-288`)逐字段建 paimon DataField:`284-285` `new DataField(fieldId.incrementAndGet(), fieldName, toPaimonType(children.get(i)).copy(type.isChildNullable(i)))`。**嵌套 nullability 经 `.copy(type.isChildNullable(i))` 保留**(注释标 FIX-L13);三参构造未传 description → **comment 丢弃**,注释 `283` 明标 "field comment stays dropped (accepted display-only deviation DV-035 M10.1)"。
- ARRAY 元素(`257` `.copy(type.isChildNullable(0))`)、MAP value(`263` `.copy(type.isChildNullable(1))`)nullability 保留;MAP key 强制 `.copy(false)`(`262`,结构性 non-null,合规)。
- 数据源头 `ConnectorColumnConverter.java:189-195` STRUCT 分支:`nullables.add(f.getContainsNull())`(`192`)、`comments.add(f.getComment())`(`193`)、`ConnectorType.structOf(names, types, nullables, comments)`(`195`)——nullability+comment 均在 SPI 层可用。
- SPI accessor 齐备:`ConnectorType.java`(实际路径 `fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorType.java`)`isChildNullable(int)@239`、`getChildComment(int)@246` 均存在。stage-1 未给完整路径但行号精确。
- 顶层列保留 comment+nullability:`PaimonSchemaBuilder.java:127-128` `toPaimonType(col.getType()).copy(col.isNullable())` + 第三参 `col.getComment()`,证实 "并非整条写路径列级都丢"。
- 单测 `PaimonTypeMappingToPaimonTest.java` `nestedNullabilityPreservedForStructField`:断言 `STRUCT<x:INT NOT NULL, y:STRING>` 字段 0 non-null、字段 1 nullable;注释明确因 comment 有意丢弃(DV-035)故只按 `type().isNullable()` 断言而非 DataField 全等。
- verify 额外确认:`PaimonTypeMapping.java:284` 是全库唯一的 `new DataField(...)` 三参构造调用(grep 仅此一处)。

**背景**:paimon CREATE 类型转换路径为 `ConnectorColumnConverter.toConnectorType`(逐嵌套字段带 nullability+comment)→ `PaimonSchemaBuilder.build`(顶层列)→ `PaimonTypeMapping.toPaimonType/toPaimonRowType`(嵌套字段逐个建 DataField)。

**影响(comment 部分——属实但低危)**:执行
```sql
CREATE TABLE paimon_cat.db.t (
  s STRUCT<a:INT COMMENT 'field a note', b:STRING> COMMENT 'top note'
) ...;
```
- 顶层列 s 的 `COMMENT 'top note'` 保留(`PaimonSchemaBuilder.java:128`);
- 嵌套字段 a 的 `COMMENT 'field a note'` 被丢弃:该 comment 已到达 SPI(`ConnectorColumnConverter.java:193`,`ConnectorType.getChildComment` 可取),却在 `PaimonTypeMapping.java:284` 建 DataField 时未作为 description 传入。结果:回读该 paimon 表(SHOW CREATE TABLE / DESC)时嵌套字段 a 无注释。**仅影响元数据展示,不影响数据读写正确性、类型、nullability**。

nullability 断言不成立(已修):`PaimonTypeMapping.java:285` 逐字段 `.copy(type.isChildNullable(i))`,有单测护;主张 "column 粒度而非 per 嵌套 field" 对 nullability 已不成立。

**为何 comment 部分非疏漏 bug**:代码注释(`283`)与单测(`184-185`)均显式声明这是已签字接受的 display-only 偏差 DV-035 M10.1,故实际严重度低于报告标注的 🟠。

**修复方案(如要消除该偏差,属产品取舍)**:paimon DataField 支持 4 参构造(id, name, type, description),在 `284` 改为传 `type.getChildComment(i)`:
```java
fields.add(new DataField(fieldId.incrementAndGet(), fieldName,
    toPaimonType(children.get(i)).copy(type.isChildNullable(i)),
    type.getChildComment(i)));
```
并把单测中 "有意丢弃 comment" 的断言改为验证 description 保留。因 DV-035 已签字接受,不修不构成正确性缺陷。

---

## #19 嵌套 null/注释(读)

**核实结论**:**PARTIAL**,高置信。两阶段一致:代码事实描述准确(读路径确按 column 粒度、嵌套 nullability+comment 未传播进 Doris StructField),但 "SPI 抽象错配/现行缺陷" 的定性被驳回——legacy 逐字节等价、非迁移引入、功能影响为零。verify 同意 verdict,但对 stage-1 的 "无害性论证" 有一处**事实性收敛**(见下),并补一处更强定性。

**原报告主张**:读路径 column 粒度而非 per 嵌套 field,嵌套 struct 子字段的 nullability/comment 丢失。

**核实过程**:
- 连接器类型映射用不带 nullable/comment 的 struct 工厂:iceberg `IcebergTypeMapping.java:77-89` STRUCT 分支 `ConnectorType.structOf(names, types).withChildrenFieldIds(fieldIds)`(只带 name/type/field-id,未采 `NestedField.isOptional()`/`.doc()`);paimon `PaimonTypeMapping.java:183-192` `toStructType` 用 `structOf(names, types)`(未采 DataField `.description()`/`.type().isNullable()`)。
- fe-core 读侧转换器也不消费:`ConnectorColumnConverter.convertStructType:250-259` 用 2 参 `new StructField(fieldName, convertType(...))`;`convertArrayType:234-240` 硬编码 `ArrayType.create(..., true)`;`convertMapType:242-248` 用默认构造。
- 非 SPI 能力缺失:`ConnectorType.java:32-43` 注释、`169-178` structOf 双工厂(2 参 / 4 参)、`isChildNullable@239`/`getChildComment@246` 全在;写/DDL 侧确实消费(`toConnectorType:168-195` 读 `getContainsNull()`+`getComment()` 走 4 参 structOf,消费者=`IcebergSchemaBuilder.java:118/128/139-140` + `PaimonTypeMapping.java:257/263/285` + PaimonSchemaBuilder)。读显示路径是**有意不消费**。
- read 单测以此为设计基线:`IcebergTypeMappingReadTest.java:214` 注释 "Legacy builds StructField(name, mapped(type)) per field in order",只断言名字/顺序/field-id,不断言 nullable/comment。
- **provenance(verify)**:stage-1 引的 legacy parity(master `IcebergUtils.java:702-707` / `PaimonUtil.java:301-303`)因这两个 fe-core legacy 文件已在本分支删除(find 无命中)、约定不 git show,**无法从当前树复核**;parity 主张仅由 `IcebergTypeMappingReadTest.java:214` 注释间接佐证。结论可信,但严格说是对 master 的断言。

**verify 对 stage-1 的事实性修正(收敛,但不改 verdict)**:
stage-1 反复用 "Doris 全局把嵌套字段视为恒 nullable"(引 `StructField.java:41` 注释 "Now always true")作为无害性支柱——**此为过头**。`StructField`(fe-type)`40-58` 存在 **4 参 ctor(可传 containsNull=false)**+ 3 参带 comment;`toSql:88-103` 的 `line91` 渲染 " not null"、`line99-101` 渲染 comment;且写/DDL 路径 `toConnectorType:192-193` 实际读 `getContainsNull()/getComment()` 并逐字段承载。即 **Doris 完全能表示且确会渲染嵌套 NOT NULL/comment**,`line41` 那句注释相对写路径已陈旧。精确表述应为:是【读路径】把嵌套字段一律构造成 nullable/无 comment,而非 "Doris 无法表示"。

**verify 补一处更强定性(仍不改 verdict)**:由上可推出——同一张声明了嵌套 NOT NULL 的表,若经 Doris 原生 DDL 建则 DESCRIBE 显示 not null,若是 iceberg/paimon 外表读则不显示,这是 Doris 内部的展示**不一致**,比 stage-1 说的 "纯与 legacy 一致的 cosmetic 差异" 更实。但因 legacy 读路径有完全相同的缺口,故仍非本次迁移引入的回归、仍无查询正确性影响。

**为何功能影响为零**:
- BE 查询正确性:外表嵌套字段的 NOT NULL 从不在查询期强制;把源端声明 NOT NULL 的嵌套字段当作 nullable 读**永远安全**(不产生错误结果)。
- 唯一可观察差异是纯展示层。示例:iceberg 表 `t(s struct<a:int NOT NULL COMMENT 'k'>)`,`DESCRIBE` 在 legacy 与新 SPI 下都显示 `s STRUCT<a:INT>`(子字段既不显示 not null 也不显示 comment);顶层列的 comment 仍保留(经 `ConnectorColumn.getComment`,`ConnectorColumnConverter.convertColumn:67-69`)。

**结论**:代码观察属实但无害——属 legacy parity + 读路径一律构造 nullable 的缺口,非 SPI 抽象错配、非回归、无功能影响。SPI 反而**新增**了承载嵌套 nullable/comment 的能力(`ConnectorType.java:32-43`)并在写/DDL 侧启用,读显示侧刻意不消费以保 parity。

**修复方案(若未来要在 DESCRIBE 展示嵌套 NOT NULL/comment,属独立增强而非修 bug)**:同时改连接器读映射(改用 `structOf` 4 参并采集 iceberg `.isOptional()/.doc()`、paimon `.isNullable()/.description()`)与 `convertStructType:256`(改用 4 参 StructField 构造)。

---

## #22 iceberg 谓词下推用 latest 非 pinned schema

**核实结论**:**CONFIRMED**,高置信。两阶段一致:主张的每个事实断言均成立。verify agrees=true、final_verdict=CONFIRMED,仅补两点非否决性说明(见核实过程)。需澄清其性质:这是有意设计而非缺陷——作为 "现象描述" 完全成立,作为 "错配/待修 bug" 则不成立。

**原报告主张**:iceberg 谓词转换用 latest schema 而非 pinned(time-travel)schema,导致下推丢失(仅丢下推,不错结果)。

**核实过程**(两阶段独立读同一批 file:line,结论吻合):
- `IcebergScanPlanProvider.java:1099-1124` —— `buildScan`:即便已有 time-travel pin(`1104-1110` `hasSnapshotPin → useSnapshot/useRef`),谓词转换器 `IcebergPredicateConverter` 仍用 `table.schema()`(当前/latest schema,`1118`)构造。`1112-1116` 明确注释:谓词转换用 CURRENT schema,对齐 legacy `createTableScan:589` `convertToIcebergExpr(conjunct, icebergTable.schema())`,而非 pinned schema。
- `IcebergPredicateConverter.java:295-301` —— `getPushdownField → caseInsensitiveFindField → null-if-absent`:pinned 后被改名的列在当前 schema 里找不到 → 返回 null;`238-241` `buildComparison` 据此 drop 到 null;`276-288` `buildIn` 同理。`javadoc:64-69` 确认无法解析的列 → null → BE residual 安全过近似(不漏行/不多行,结果正确)。
- `IcebergScanPlanProvider.java:1141-1150` —— 独立 `pinnedSchema()` 仅用于 schema-evolution 字段 ID 字典(BE 槽位命名,`1617`,与 `1131-1139` 的 dict/slot 字节一致 INVARIANT 协同),**不参与谓词转换**。
- 其余下推点均用 `table.schema()`:EXPLAIN `1670`、COUNT summary `2155`、delete metrics `2001`。
- **verify 补充两点(非修正)**:(1) `912-913` 也 `new IcebergPredicateConverter`,但那是 position-deletes 元数据表扫描用 `metadataTable.schema()`,上下文不同、正确地在本发现范围外,不影响结论。(2) 本分支 fe-core 的 legacy `IcebergScanNode`/`convertToIcebergExpr` 已被迁移删除(grep 零命中),故 stage-1 引的 `createTableScan:589` 是代码注释里的历史来源引用而非现存文件;但 "当前用 current schema" 的行为由当前代码本身直接证实,不削弱发现。

**背景**:iceberg time-travel 读(`FOR VERSION`/`TIME AS OF`)会把 scan pin 到历史 snapshot(`buildScan:1104`)。谓词下推转换器 `IcebergPredicateConverter` 却用 `table.schema()`(当前最新 schema,`1118`),而非该 snapshot 对应的 pinned schema。`1112-1116` 的注释明确说明这是刻意对齐 legacy `IcebergScanNode.createTableScan:589`(`convertToIcebergExpr` 恒用 `icebergTable.schema()`)。

**影响(具体示例)**:表 t 有列 a,后执行 `ALTER TABLE t RENAME COLUMN a TO b`。对历史快照做 time-travel:
```sql
SELECT * FROM iceberg_cat.db.t FOR VERSION AS OF <旧快照,此时列名还是 a> WHERE a = 5;
```
转换器用当前 schema(只有 b)去 `caseInsensitiveFindField("a")` → 找不到 → 该谓词被丢弃(`IcebergPredicateConverter.java:295-301`, `buildComparison:238-241`),退化为 BE residual 过滤。后果只是少了 manifest/文件级剪枝(扫更多文件、慢),**结果集仍正确**——converter 的 drop-to-residual 是安全过近似(`javadoc:64-69`),绝不会漏行或多行。未改名的普通列(绝大多数场景)pinned 名 == 当前名,行为与理想一致,零影响。

**是否需修:不建议改**。理由:
1. 代码有明确注释锁定为 legacy parity,legacy 本身即如此;
2. 仅 "schema 演进期间发生列改名 + 恰好 time-travel 到旧快照 + 谓词恰好命中被改名列" 这一多重罕见交集才触发,且仅损失下推性能不损正确性;
3. 若真要修需引入按 pinned schema 转换,并与 `1131-1139` 的 INVARIANT(dict-schema 与 slot-schema 必须字节一致,否则 BE `children.at` SIGABRT)协同,收益(边缘剪枝)远小于风险。

报告将其定为最低档 F/🟡、状态 "现行(仅丢下推)" 恰当;作为现象记录成立,作为待修 bug 不成立。

---

## 本类别小结

**真问题**:
- **#2(🔴 CONFIRMED)** 是本类别唯一的真实正确性/稳定性缺陷:SPI `getColumnHandles` 无 snapshot 形参,导致 time-travel 跨 RENAME 的**混合投影**查询在 paimon 侧 BE crash。触发面窄(需 pin 到 rename 前 + 混合投影)、无回归覆盖,属潜伏未测缺陷。iceberg 已在连接器侧用 `hasSnapshotPin()` 分支自防,paimon 未做。

**伪/已修/夸大/有意设计**:
- **#10(🟠→PARTIAL)**:机械事实(parseSchema 传 null、`ConnectorColumn` 单槽、`writeDefault` 未接)成立,但标题 "从不读 initialDefault + initial/write 塌陷" 严重夸大——initialDefault 经 #65502 独立 thrift 字段被读并下发 BE(带单测),无塌陷;真实剩余缺口仅 writeDefault 未接 + Column 元数据默认恒 null,均边缘写路径/展示层,低危,很可能与老实现平价。
- **#11(🟠→PARTIAL)**:行为事实全对,但是刻意 legacy parity(注释+mutation 单测钉死),非迁移引入 bug,无数据正确性影响。
- **#12 nullability 部分**:已由 FIX-L13 修复 + 单测护,主张不成立;**comment 部分**:属实但为已签字 display-only 偏差 DV-035,低危。
- **#19(🟡→PARTIAL)**:代码事实属实但功能影响为零,legacy 逐字节等价,非回归。
- **#22(🟡→CONFIRMED)**:事实断言全对(谓词转换用 latest 非 pinned schema),但代码有明确注释锁死为**有意 legacy parity**,仅 "改名+time-travel+谓词命中改名列" 罕见交集触发,只丢文件级剪枝不丢正确性(converter drop-to-residual 安全过近似)——作现象成立、作待修 bug 不成立,不建议改。

**共性根因**:
1. **列身份的 "名字 vs field-id / snapshot" 粒度**(#2/#22):SPI/连接器在按名字消费 schema 时看不到 pin。#2 是 `getColumnHandles` 层丢列(唯一真危害根因,BE crash);#22 是谓词下推层用 latest 而非 pinned schema(仅丢改名列的文件级剪枝、不损正确性、有意 legacy parity)。iceberg 在 handle/dict 侧靠连接器内 pin-aware 重建规避 #2,却在谓词转换侧刻意保持 current-schema(#22);paimon 缺 #2 的自防层。
2. **嵌套字段/默认值属性传播的方向性、刻意窄化或未接线**(#10/#11/#12/#19):写向 CREATE 有意窄化(#11 类型参数、#12 comment)以保 legacy parity;读向一律构造 nullable/无 comment(#19);列默认值 write-default 未接线、Column 元数据默认恒 null(#10,但读时 initialDefault 已经 #65502 独立字段正确下发 BE)。SPI(`ConnectorType` 4 参 structOf + `isChildNullable/getChildComment`、`ConnectorColumn.defaultValue`)大多已具备承载能力,是消费侧刻意不用或边缘路径未接,均非抽象缺失、无正确性危害。

**与其他类别关联**:
- #2 与 P6-1(iceberg BE StructNode DCHECK crash)是 schema-grain 兄弟,同一 `table_schema_change_helper.h` target-key 缺列崩溃族;iceberg 的 `IcebergScanPlanProvider.java:1594-1618` pin 分支即该族的连接器侧防线,可作 paimon 修复的模板。
- #11/#12/#19 共享 `PaimonTypeMapping` / `ConnectorColumnConverter` / `ConnectorType` 这套 schema 转换基础设施,与类别中 DDL/类型映射相关发现应交叉参照(尤其 hudi/paimon "完整对齐 vs 有意提升" 的签字先例决定 #11/#12 是否升级为增强)。
