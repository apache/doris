# FIX-L7 — kerberos 每次 login/refresh 无条件 UGI.setConfiguration（丢 first-writer guard）

> 来源：reverify §1 表 L7（原 P3b-2）。🟡 低（metastore 路 parity;仅 fe-filesystem HDFS 数据路真变）。范围：`HadoopKerberosAuthenticator.initializeAuthConfig`。
> HEAD 复核基线：`e27602d4ab6`。

## Problem / Root Cause

`initializeAuthConfig`（:53-59）无条件 `UserGroupInformation.setConfiguration(hadoopConf)`。它由 `login()`（:115）调，
而 `login` 在**初次登录**（:65）和**每次刷票**（:83）都跑 → 每个 kerberos 目录的每次 login/refresh 都**覆写进程级全局
UGI 配置**（last-writer-wins）。多个 auth 方式不同的 HDFS 目录不能共存,且刷票期无谓 churn。

**consolidation commit `f7992b0a07e`(#64655) 删掉了 master 原有的 first-writer-wins guard**
（`shouldSkipSetConfiguration` + `UGI_INIT_LOCK` + 不匹配 WARN）。本条恢复其**意图**。

## Design（恢复 first-writer-wins，但不引入 getLoginUser 副作用）

master 原 guard 用 `UserGroupInformation.getLoginUser().getAuthenticationMethod()` 读全局当前 auth 方式。**但**
`getLoginUser()` 在 loginUser 未设时会**触发进程级登录**——而 Doris **有意从不设进程级 login user**（只 per-instance
`getUGIFromSubject`；见 `IcebergConnectorTransaction.java:233` 注释）。verbatim 移植会引入这个被刻意规避的副作用。

→ **等价意图、无副作用**：用静态字段记住**首个** setConfiguration 采用的 auth 方式，锁内 first-writer-wins：
```java
private static UserGroupInformation.AuthenticationMethod configuredAuthMethod = null;

public static void initializeAuthConfig(Configuration hadoopConf) {
    hadoopConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHORIZATION, "true");
    UserGroupInformation.AuthenticationMethod desired = SecurityUtil.getAuthenticationMethod(hadoopConf);
    synchronized (HadoopKerberosAuthenticator.class) {
        if (configuredAuthMethod == null) {
            UserGroupInformation.setConfiguration(hadoopConf);   // 首写者定全局
            configuredAuthMethod = desired;
            return;
        }
        if (configuredAuthMethod != desired) {
            LOG.warn("UGI already configured with authentication={} but this catalog requests {}; "
                    + "keeping existing JVM-global setting (first-writer-wins).", configuredAuthMethod, desired);
        }
    }
}
```
- `SecurityUtil.getAuthenticationMethod(hadoopConf)` 只读**该目录自己的 conf**(无全局副作用)。
- 首个 kerberos 目录：设全局 + 记录 auth 方式。
- 之后目录 / 刷票：`desired==configuredAuthMethod`（同 kerberos）→ 静默跳过（满足「refresh 不重跑」）；
  仅**真不匹配**才 WARN（master 意图）。
- `hadoopConf.set(HADOOP_SECURITY_AUTHORIZATION,"true")` 留在锁外（每目录须在**自己的 conf** 上置授权,供其自身 RPC）。

**只有 kerberos 认证器调 `initializeAuthConfig`**（simple 走 `HadoopSimpleAuthenticator`,不设 UGI 全局）→ 静态字段
在 kerberos 目录间跟踪首写者正确。

## Risk

- 单 kerberos 目录 / 多同 auth：首次设、之后静默跳 → 行为等价「设一次」,消除刷票 churn 与跨目录 stomp。
- 不引入 `getLoginUser()` 进程级登录副作用（对齐 Doris 设计）。
- fe-kerberos 独立模块,非连接器,import 门禁不适用。
- **偏离 verbatim master**：机制换静态字段（非 getLoginUser），已在设计说明理由;意图（first-writer-wins + 不匹配 WARN）完全一致。

## Test Plan

- build-compile（`SecurityUtil.getAuthenticationMethod` / `AuthenticationMethod` 签名 + 0 checkstyle）。
- 行为 UT：`fe-kerberos` 现有 UT 仅 `KerberosTicketUtilsTest`;驱动 `initializeAuthConfig` 需真 UGI 全局态 + kerberos conf
  （静态进程态,测试间污染）→ 不加,登记。以设计推理 + 对抗复审兜底。
- e2e live-gated：两 kerberos HDFS 目录,第二个不覆写全局(FE log 无「setConfiguration」churn;auth 不同时 WARN)。

## 备注

与 L8（doAs interrupt 一行）同 fe-kerberos 模块,合并编译 + 各自独立 commit。
