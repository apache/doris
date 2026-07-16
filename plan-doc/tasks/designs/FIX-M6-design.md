# FIX-M6 — iceberg s3tables 无显式凭证即硬失败 → 回退 SDK 默认凭证链

> 来源：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md` §3 M6。
> recon+对抗红队：`wf_40498e52-19f`（SOUND_WITH_CHANGES：核心正确，红队要求 = data-plane client.region companion 从可选**升为必做** + 测试 UnusedImports 防护）。依赖 **M7**（拓宽后的 region 别名集）。

## Problem

依赖 AWS 默认凭证链的 iceberg `s3tables` 目录（如 EC2 instance-profile：仅 `s3.region` + warehouse
table-bucket ARN，无静态 AK/SK、无 role、无 `credentials_provider_type`）首访即抛：
`Iceberg s3tables catalog requires S3-compatible storage properties (region + credentials)`。旧版
`IcebergS3TablesMetaStoreProperties` 支持此场景（无静态凭证时用 `DefaultCredentialsProvider` 默认链）。fail-loud 回归。

## Root Cause

`IcebergConnector.createS3TablesCatalog` 开头 `if (!chosenS3.isPresent()) throw ...`。`chosenS3` 来自
`chooseS3Compatible(context.getStorageProperties())`；fe-filesystem 仅在 `S3FileSystemProvider.supports()`
为 true（要求 `hasCredential && hasLocation`）时绑定 S3 存储。region+warehouse-only 的 EC2 目录 `hasCredential=false`
→ `supports()=false` → 无绑定存储 → `chosenS3` 空 → 建表首访抛（早于任何 AWS 调用）。下游 `buildS3TablesClient`
本已能处理无静态凭证（`buildAwsCredentialsProvider` 对 DEFAULT/PROVIDER_CHAIN 返回 `DefaultCredentialsProvider`），
故唯一阻断是 (a) 过早的 `!chosenS3.isPresent()` throw + (b) region 只从存储对象 `s3.getRegion()` 取、不看 raw props。

## Design

反转硬性要求：**region**（可来自绑定存储或 raw props）是 s3tables 唯一硬性要求；无绑定存储时凭证回退 SDK 默认链。

1. `IcebergCatalogFactory.resolveS3Region(props)` 新 `public static`：`firstNonBlank(props, S3_REGION_ALIASES)`——
   region 别名回退的单一真源，`appendS3FileIO` 重构为调它（纯重构；`S3_REGION_ALIASES` 已由 M7 拓宽）。
2. `IcebergConnector.resolveS3TablesRegion(chosenS3, props)` 新 `static` 包可见门：绑定存储 region 优先、否则
   `resolveS3Region(props)`；均空才 fail-loud（唯一硬失败=无 region）。静态包可见 → 离线单测无需活 `S3TablesClient`。
3. `createS3TablesCatalog` 删 `!chosenS3.isPresent()` throw + 独立 blank-region throw，改用 `resolveS3TablesRegion`；
   `buildS3TablesClient(Optional<storage>, String region)` 重签名，凭证=
   `chosenS3.map(s3 -> buildAwsCredentialsProvider(s3, props)).orElseGet(DefaultCredentialsProvider::create)`、
   region=`Region.of(region)`。绑定存储路径字节不变（同 region、同凭证 provider）。
4. **companion（红队升为必做）**：`buildS3TablesCatalogProperties` 无绑定存储臂也发 `client.region`（=`resolveS3Region(props)`），
   使 **data-plane** `S3FileIO` 认显式 `s3.region` 而非只靠 IMDS/`DefaultAwsRegionProviderChain`（镜像
   `appendS3FileIO` vended-cred 臂）。核心修复单独已解 EC2（data-plane 经 IMDS 取 region），companion 覆盖显式
   `s3.region` ≠ host region / 非-EC2 默认链主机。

铁律：全连接器局部（fe-connector-iceberg；fe-filesystem-s3 不动）；无 fe-core 改/import；无源名判别；无 Mockito。

## Implementation

- `IcebergCatalogFactory.java`：加 `resolveS3Region`；`appendS3FileIO` 改调它；`buildS3TablesCatalogProperties`
  ifPresent→if/else（空臂发 client.region）。
- `IcebergConnector.java`：`createS3TablesCatalog` 重写（删 throw）；加 `resolveS3TablesRegion`；`buildS3TablesClient`
  重签名 + 凭证 orElseGet 默认链；更新 3 处 javadoc。**无新 production import**（`DefaultCredentialsProvider`/
  `AwsCredentialsProvider`/`Region`/`Optional`/`StringUtils` 均已 import）。
- 测试：见下。**测试新增 2 import（`java.util.Optional` + `S3CompatibleFileSystemProperties`）均被用**（`prefersBoundStorageRegion`
  用 typed `Optional<S3CompatibleFileSystemProperties>` 局部，规避泛型不变性 + 满足 UnusedImports 门）。

## Risk

- **M6→M7 序**：M6 无 M7 拓宽的别名集则 `AWS_REGION`/`*.signing-region` 供的 region 解析不到；M7 已先落（同文件、非重叠行）。
- 行为变更（有意）：无存储不再致命，唯一 fail-loud=无 region；`s3TablesWithoutStorageFailsLoud` 断言措辞须 reframe。
- 正常目录零回归：静态凭证/role/provider-chain/绑定存储 s3tables 路径字节不变（同 region、同凭证 provider）。
- companion 令 `WithoutStorageOmitsS3FileIo` 的 WHY 过时（其 props 无 region 别名→client.region 仍 null，断言保持绿），
  已更正注释 + 加正向 companion 测试。

## Test

- Unit（`IcebergConnectorTest` + `IcebergCatalogFactoryTest`；无 Mockito；recording fake）：
  - reframe `s3TablesWithoutStorageFailsLoud`→`s3TablesWithoutStorageOrRegionFailsLoud`（无存储无 region→抛「requires a region」，**RED at HEAD**：HEAD 抛存储措辞不含该串）。
  - `+ resolveS3TablesRegion` 4 测：props 回退 / 拓宽别名(AWS_REGION) / 绑定存储优先 / 均空 fail-loud。
  - `+ buildS3TablesCatalogPropertiesPropagatesClientRegionWithoutBoundS3`（无存储 + s3.region→client.region，**RED at HEAD**）。
  - 结果：`IcebergCatalogFactoryTest` 63/63、`IcebergConnectorTest` 19/19、0 checkstyle。
- E2E live-gated（无本地 AWS）：真 S3/MinIO/instance-profile-emulated 目录，region+warehouse-only props CREATE CATALOG
  + list namespaces 不抛 `DorisConnectorException`；随 P6.6 docker plugin-zip 门禁（memory `hms-iceberg-delegation-needs-e2e`）。
