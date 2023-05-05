---
{
    "title": "常见问题",
    "language": "zh-CN"
}
---

<!-- 
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->


# 常见问题

1. 通过 Hive Metastore 访问 Iceberg 表报错：`failed to get schema` 或 `Storage schema reading not supported`

	在 Hive 的 lib/ 目录放上 `iceberg` 运行时有关的 jar 包。

	在 `hive-site.xml` 配置：
	
	```
	metastore.storage.schema.reader.impl=org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader
	```

	配置完成后需要重启Hive Metastore。

2. 连接 Kerberos 认证的 Hive Metastore 报错：`GSS initiate failed`

    通常是因为 Kerberos 认证信息填写不正确导致的，可以通过以下步骤排查：

    1. 1.2.1 之前的版本中，Doris 依赖的 libhdfs3 库没有开启 gsasl。请更新至 1.2.2 之后的版本。
    2. 确认对各个组件，设置了正确的 keytab 和 principal，并确认 keytab 文件存在于所有 FE、BE 节点上。

        1. `hadoop.kerberos.keytab`/`hadoop.kerberos.principal`：用于 Hadoop hdfs 访问，填写 hdfs 对应的值。
        2. `hive.metastore.kerberos.principal`：用于 hive metastore。

    3. 尝试将 principal 中的 ip 换成域名（不要使用默认的 `_HOST` 占位符）
    4. 确认 `/etc/krb5.conf` 文件存在于所有 FE、BE 节点上。
	
3. 访问 HDFS 3.x 时报错：`java.lang.VerifyError: xxx`

   1.2.1 之前的版本中，Doris 依赖的 Hadoop 版本为 2.8。需更新至 2.10.2。或更新 Doris 至 1.2.2 之后的版本。

4. 使用 KMS 访问 HDFS 时报错：`java.security.InvalidKeyException: Illegal key size`

   升级 JDK 版本到 >= Java 8 u162 的版本。或者下载安装 JDK 相应的 JCE Unlimited Strength Jurisdiction Policy Files。

5. 查询 ORC 格式的表，FE 报错 `Could not obtain block` 或 `Caused by: java.lang.NoSuchFieldError: types`

   对于 ORC 文件，在默认情况下，FE 会访问 HDFS 获取文件信息，进行文件切分。部分情况下，FE 可能无法访问到 HDFS。可以通过添加以下参数解决：

   `"hive.exec.orc.split.strategy" = "BI"`

   其他选项：HYBRID（默认），ETL。

6. 通过 JDBC Catalog 连接 SQLServer 报错：`unable to find valid certification path to requested target`

   请在 `jdbc_url` 中添加 `trustServerCertificate=true` 选项。

7. 通过 JDBC Catalog 连接 MySQL 数据库，中文字符乱码，或中文字符条件查询不正确

   请在 `jdbc_url` 中添加 `useUnicode=true&characterEncoding=utf-8`

   > 注：1.2.3 版本后，使用 JDBC Catalog 连接 MySQL 数据库，会自动添加这些参数。

8. 通过 JDBC Catalog 连接 MySQL 数据库报错：`Establishing SSL connection without server's identity verification is not recommended`

   请在 `jdbc_url` 中添加 `useSSL=true`

9. 连接 Hive Catalog 报错：`Caused by: java.lang.NullPointerException`

    如 fe.log 中有如下堆栈：

    ```
    Caused by: java.lang.NullPointerException
        at org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook.getFilteredObjects(AuthorizationMetaStoreFilterHook.java:78) ~[hive-exec-3.1.3-core.jar:3.1.3]
        at org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook.filterDatabases(AuthorizationMetaStoreFilterHook.java:55) ~[hive-exec-3.1.3-core.jar:3.1.3]
        at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getAllDatabases(HiveMetaStoreClient.java:1548) ~[doris-fe.jar:3.1.3]
        at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getAllDatabases(HiveMetaStoreClient.java:1542) ~[doris-fe.jar:3.1.3]
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_181]
    ```

    可以尝试在 `create catalog` 语句中添加 `"metastore.filter.hook" = "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl"` 解决。

10. 通过 Hive Catalog 连接 Hive 数据库报错：`RemoteException: SIMPLE authentication is not enabled.  Available:[TOKEN, KERBEROS]`

   如果在 `show databases` 和 `show tables` 都是没问题的情况下，查询的时候出现上面的错误，我们需要进行下面两个操作：
- fe/conf、be/conf 目录下需放置 core-site.xml 和 hdfs-site.xml
   - BE 节点执行 Kerberos 的 kinit 然后重启 BE ，然后再去执行查询即可.


11. 如果创建 Hive Catalog 后能正常`show tables`，但查询时报`java.net.UnknownHostException: xxxxx`

    可以在 CATALOG 的 PROPERTIES 中添加
    ```
    'fs.defaultFS' = 'hdfs://<your_nameservice_or_actually_HDFS_IP_and_port>'
    ```
