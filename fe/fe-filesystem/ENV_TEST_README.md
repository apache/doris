# Filesystem Environment Tests

本目录下的环境测试（Layer 2）需要真实的云存储/HDFS/Broker 服务才能运行。默认 CI 构建会自动跳过它们。

## 快速开始

### 使用辅助脚本

仓库根目录提供了 `run-fs-env-test.sh`，支持通过命令行参数或预设环境变量运行：

```bash
# S3 测试
./run-fs-env-test.sh s3 \
  --s3-endpoint=https://s3.us-east-1.amazonaws.com \
  --s3-region=us-east-1 \
  --s3-bucket=my-test-bucket \
  --s3-ak=AKIAIOSFODNN7EXAMPLE \
  --s3-sk=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

# Azure 测试
./run-fs-env-test.sh azure \
  --azure-account=myaccount \
  --azure-key=base64key== \
  --azure-container=testcontainer

# HDFS 测试（Simple Auth）
./run-fs-env-test.sh hdfs \
  --hdfs-host=namenode.example.com \
  --hdfs-port=8020

# Kerberos 测试
./run-fs-env-test.sh kerberos \
  --kdc-principal=hdfs/namenode@REALM \
  --kdc-keytab=/path/to/hdfs.keytab \
  --hdfs-host=namenode.example.com \
  --hdfs-port=8020

# COS（腾讯云）测试
./run-fs-env-test.sh cos \
  --cos-endpoint=https://cos.ap-guangzhou.myqcloud.com \
  --cos-region=ap-guangzhou \
  --cos-bucket=my-cos-bucket \
  --cos-ak=SecretId \
  --cos-sk=SecretKey

# OSS（阿里云）测试
./run-fs-env-test.sh oss \
  --oss-endpoint=https://oss-cn-hangzhou.aliyuncs.com \
  --oss-region=cn-hangzhou \
  --oss-bucket=my-oss-bucket \
  --oss-ak=AccessKeyId \
  --oss-sk=AccessKeySecret

# OBS（华为云）测试
./run-fs-env-test.sh obs \
  --obs-endpoint=https://obs.cn-north-4.myhuaweicloud.com \
  --obs-region=cn-north-4 \
  --obs-bucket=my-obs-bucket \
  --obs-ak=AK \
  --obs-sk=SK

# Broker 测试
./run-fs-env-test.sh broker \
  --broker-host=broker.example.com \
  --broker-port=8060

# 运行全部环境测试（需要所有环境变量已预设）
./run-fs-env-test.sh all
```

### 使用 Maven 直接运行

也可以先导出环境变量，然后使用 Maven 命令直接运行：

```bash
# 导出凭据
export DORIS_FS_TEST_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
export DORIS_FS_TEST_S3_REGION=us-east-1
export DORIS_FS_TEST_S3_BUCKET=my-test-bucket
export DORIS_FS_TEST_S3_AK=your-access-key
export DORIS_FS_TEST_S3_SK=your-secret-key

# 运行 S3 相关环境测试
cd fe
mvn test -pl fe-filesystem \
  -Dsurefire.excludedGroups= \
  -Dgroups=s3 \
  -Dcheckstyle.skip=true \
  -DfailIfNoTests=false \
  -Dmaven.build.cache.enabled=false \
  --also-make

# 运行 HDFS + Kerberos 环境测试
mvn test -pl fe-filesystem \
  -Dsurefire.excludedGroups= \
  -Dgroups="hdfs | kerberos" \
  -Dcheckstyle.skip=true \
  -DfailIfNoTests=false \
  -Dmaven.build.cache.enabled=false \
  --also-make
```

## 环境变量一览

| Tag | 环境变量 | 说明 |
|-----|---------|------|
| `s3` | `DORIS_FS_TEST_S3_ENDPOINT` | S3 兼容存储端点 |
| | `DORIS_FS_TEST_S3_REGION` | 区域 |
| | `DORIS_FS_TEST_S3_BUCKET` | 测试桶名 |
| | `DORIS_FS_TEST_S3_AK` | Access Key |
| | `DORIS_FS_TEST_S3_SK` | Secret Key |
| `azure` | `DORIS_FS_TEST_AZURE_ACCOUNT` | Azure Storage 账户名 |
| | `DORIS_FS_TEST_AZURE_KEY` | 账户密钥 |
| | `DORIS_FS_TEST_AZURE_CONTAINER` | 测试容器名 |
| `cos` | `DORIS_FS_TEST_COS_ENDPOINT` | COS 端点 |
| | `DORIS_FS_TEST_COS_REGION` | 区域（如 ap-guangzhou） |
| | `DORIS_FS_TEST_COS_BUCKET` | 测试桶名 |
| | `DORIS_FS_TEST_COS_AK` | SecretId |
| | `DORIS_FS_TEST_COS_SK` | SecretKey |
| `oss` | `DORIS_FS_TEST_OSS_ENDPOINT` | OSS 端点 |
| | `DORIS_FS_TEST_OSS_REGION` | 区域 |
| | `DORIS_FS_TEST_OSS_BUCKET` | 测试桶名 |
| | `DORIS_FS_TEST_OSS_AK` | Access Key |
| | `DORIS_FS_TEST_OSS_SK` | Secret Key |
| `obs` | `DORIS_FS_TEST_OBS_ENDPOINT` | OBS 端点 |
| | `DORIS_FS_TEST_OBS_REGION` | 区域 |
| | `DORIS_FS_TEST_OBS_BUCKET` | 测试桶名 |
| | `DORIS_FS_TEST_OBS_AK` | Access Key |
| | `DORIS_FS_TEST_OBS_SK` | Secret Key |
| `hdfs` | `DORIS_FS_TEST_HDFS_HOST` | NameNode 地址 |
| | `DORIS_FS_TEST_HDFS_PORT` | NameNode 端口 |
| `kerberos` | `DORIS_FS_TEST_KDC_PRINCIPAL` | Kerberos 主体 |
| | `DORIS_FS_TEST_KDC_KEYTAB` | keytab 文件路径 |
| | `DORIS_FS_TEST_HDFS_HOST` | 启用 Kerberos 的 HDFS 地址 |
| `broker` | `DORIS_FS_TEST_BROKER_HOST` | Broker 进程地址 |
| | `DORIS_FS_TEST_BROKER_PORT` | Broker 进程端口 |

## 测试用例概览

### T-E1: S3ObjStorage 环境测试（5 tests, tag: `s3`）
- `putAndHeadObject` — 上传小文件 → headObject 验证 size 和 etag
- `listObjects` — 上传多个文件 → listObjects 验证返回数量
- `copyAndDeleteObject` — 上传 → copy → 验证 → delete → 验证不存在
- `multipartUpload_completeSucceeds` — initiate → uploadPart × 2 → complete → headObject 验证
- `abortMultipartUpload_leavesNoObject` — initiate → abort → 对象不存在

### T-E2: S3FileSystem 环境测试（7 tests, tag: `s3`）
- `exists` — 已存在/不存在的对象
- `deleteRemovesObject` — 上传 → delete → exists false
- `renameMovesObject` — 上传 → rename → 旧不存在/新存在
- `listReturnsCorrectEntries` — 上传多个 → list 验证
- `inputOutputRoundTrip` — 写入 → 读取 → 内容一致（含 UTF-8 + emoji）
- `inputFileLength` — 上传已知大小 → length() 验证

### T-E3: Azure 环境测试（7 tests, tag: `azure`）
测试项同 T-E2，使用 `wasbs://` scheme。

### T-E3b/c/d: COS / OSS / OBS 环境测试（各 7 tests, tag: `cos` / `oss` / `obs`）
测试项同 T-E2，分别使用腾讯云/阿里云/华为云 SDK。

### T-E4: DFSFileSystem 环境测试（8 tests, tag: `hdfs`）
- `mkdirsAndExists` — 创建多级目录 → exists true
- `deleteRecursive` — 创建含文件的目录 → 递归删除
- `renameFile` / `renameDirectory` — 文件/目录重命名
- `listFiles` / `listDirectories` — 列举文件/子目录
- `inputOutputRoundTrip` — 写入 → 读取 → 内容一致
- `inputFileLength` — 验证文件大小

### T-E5: Kerberos 环境测试（4 tests, tag: `kerberos`）
- `loginSucceeds` — 使用真实 principal/keytab 登录
- `doAsExecutesAction` — 代理执行返回值验证
- `doAsPropagatesIOException` — IOException 正确传播
- `hdfsOperationWithKerberos` — Kerberos 模式下 HDFS exists 可正常工作

### T-E6: Broker 环境测试（4 tests, tag: `broker`）
- `existsReturnsFalseForMissing` — 不存在的路径 → false
- `writeAndRead` — 通过 outputFile 写入 → inputFile 读取 → 一致
- `deleteRemovesFile` — 写入 → delete → exists false
- `listReturnsFiles` — 写入多个 → list 验证

## 注意事项

1. **每次测试都会在目标存储中创建带唯一 UUID 前缀的临时数据**，`@AfterAll` 会尝试清理。如果测试异常中断，可能需要手动清理。

2. **S3 multipart upload 测试**会上传 ~5MB 数据，请确保测试桶有足够的空间和权限。

3. **环境测试默认不运行**（通过 Maven Surefire 的 `<excludedGroups>environment</excludedGroups>` 配置）。只有通过 `-Dsurefire.excludedGroups= -Dgroups=<tag>` 显式启用时才会运行。

4. **请勿将凭据提交到代码仓库中**。建议使用环境变量或 CI/CD 密钥管理。
