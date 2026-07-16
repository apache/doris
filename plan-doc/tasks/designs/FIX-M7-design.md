# FIX-M7 — iceberg REST vended-cred client.region 别名收窄 → 拓宽对齐旧版

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M7。
> recon+对抗红队：`wf_40498e52-19f`（verdict SOUND_WITH_CHANGES：数组正确，唯一 required change = 注释措辞）。

## Problem

REST + vended credentials 的 iceberg 目录不绑定任何 fe-filesystem S3 存储（无静态 AK/SK/role →
`S3FileSystemProvider.supports` 为 false → `chooseS3Compatible` 空）。此路 `IcebergCatalogFactory
.appendS3FileIO`（rest/jdbc/hadoop flavor）**唯一**的 region 来源是 `firstNonBlank(props,
S3_REGION_ALIASES)`。原数组只有 4 项 `{s3.region, aws.region, region, client.region}`。若 region 仅经
`AWS_REGION` / `iceberg.rest.signing-region` / `rest.signing-region` 提供 → 返回 null → `client.region`
不发 → iceberg S3FileIO 落 AWS SDK `DefaultAwsRegionProviderChain` → 写提交报 **"Unable to load region"**。

## Root Cause

`:78-83` 注释自称「mirror legacy getRegionFromProperties」但**不实**：旧版 `getRegionFromProperties` 扫所有
`@ConnectorProperty(isRegionField=true)` 别名；fe-core `S3Properties`（:87-88）单 region field 就声明 **10**
个别名 `{s3.region, AWS_REGION, region, REGION, aws.region, glue.region, aws.glue.region,
iceberg.rest.signing-region, rest.signing-region, client.region}`。连接器手抄丢了 6 个 → 静默收窄。

## Design

把连接器本地 `S3_REGION_ALIASES` 拓宽为 fe-core `S3Properties` `isRegionField` 别名集的**逐字节副本**
（10 个、同声明序，`s3.region` 仍首位胜出）。fe-connector 不 import fe-core（门禁），故本地复制（沿用
`GLUE_ENDPOINT_PATTERN` 等既有 duplication 惯例）。

**注释措辞（红队 required change）**：不再声称是「getRegionFromProperties 精确镜像」——它是 `S3Properties`
`isRegionField` 别名的连接器副本，即旧版 `getRegionFromProperties` 所扫集合的 **S3 子集**；OSS/COS/OBS/Minio
子类 region 别名**刻意排除**（对 AWS-S3-backed vended REST 目录无关）。历史引用 `AbstractIcebergProperties
.toFileIOProperties` 已不存在于 fe-core，注释不再指其为「当前」。

## Implementation

- `IcebergCatalogFactory.java` `:78-88`：更正注释 + 4 元数组→10 元多行数组（单消费点 `:192`；glue 路不动）。
- `IcebergCatalogFactoryTest.java`：新增 `buildCatalogPropertiesRestVendedResolvesRegionFromWidenedAliases`
  （region 仅经 `AWS_REGION`、仅经 `iceberg.rest.signing-region` → `client.region` 发出）；既有 `s3.region`
  用例保留为非回归钉。**无新 import。**

## Risk

- 单读点（`:192`）在 chosenS3 空臂；chosenS3-present（typed getter）与 glue（`resolveGlueRegion`）不受影响。
- 序照 `S3Properties` 声明序 → 冲突解析 parity。无 fe-core import（字面量），门禁清；无 unused import。
- 新增键均为合法 region 别名，空存储路无竞争值可误捕。

## Test

- Unit：见上，RED（`AWS_REGION` 大写不匹配窄集小写 `aws.region`；`iceberg.rest.signing-region` 不在窄集）→
  拓宽后 GREEN。`IcebergCatalogFactoryTest` 62/62 绿、0 checkstyle。
- E2E live-gated：真 vended-cred REST 目录（region 经 `AWS_REGION`）写提交成功、无「Unable to load region」，
  随 P6 docker 门禁。
