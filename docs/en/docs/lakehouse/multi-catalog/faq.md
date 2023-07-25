---
{
    "title": "FAQ",
    "language": "en"
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


# FAQ

1. What to do with errors such as  `failed to get schema` and  `Storage schema reading not supported`  when accessing Icerberg tables via Hive Metastore?

   To fix this, please place the Jar file package of `iceberg` runtime in the `lib/` directory of Hive.

   And configure as follows in  `hive-site.xml` :

   ```
   metastore.storage.schema.reader.impl=org.apache.hadoop.hive.metastore.SerDeStorageSchemaReader
   ```

   After configuring, please restart Hive Metastore.

2. What to do with the `GSS initiate failed` error when connecting to Hive Metastore with Kerberos authentication?

   Usually it is caused by incorrect Kerberos authentication information, you can troubleshoot by the following steps:

   1. In versions before  1.2.1, the libhdfs3 library that Doris depends on does not enable gsasl. Please update to a version later than 1.2.2.
   2. Confirm that the correct keytab and principal are set for each component, and confirm that the keytab file exists on all FE and BE nodes.

       1. `hadoop.kerberos.keytab`/`hadoop.kerberos.principal`: for Hadoop HDFS
       2. `hive.metastore.kerberos.principal`: for hive metastore.

   3. Try to replace the IP in the principal with a domain name (do not use the default `_HOST` placeholder)
   4. Confirm that the `/etc/krb5.conf` file exists on all FE and BE nodes.

3. What to do with the`java.lang.VerifyError: xxx` error when accessing HDFS 3.x?

   Doris 1.2.1 and the older versions rely on Hadoop 2.8. Please update Hadoop to 2.10.2 or update Doris to 1.2.2 or newer.

4. An error is reported when using KMS to access HDFS: `java.security.InvalidKeyException: Illegal key size`

    Upgrade the JDK version to a version >= Java 8 u162. Or download and install the JCE Unlimited Strength Jurisdiction Policy Files corresponding to the JDK.

5. When querying a table in ORC format, FE reports an error `Could not obtain block` or `Caused by: java.lang.NoSuchFieldError: types`

    For ORC files, by default, FE will access HDFS to obtain file information and split files. In some cases, FE may not be able to access HDFS. It can be solved by adding the following parameters:

    `"hive.exec.orc.split.strategy" = "BI"`

    Other options: HYBRID (default), ETL.

6. An error is reported when connecting to SQLServer through JDBC Catalog: `unable to find valid certification path to requested target`

    Please add `trustServerCertificate=true` option in `jdbc_url`.

7. When connecting to the MySQL database through the JDBC Catalog, the Chinese characters are garbled, or the Chinese character condition query is incorrect

    Please add `useUnicode=true&characterEncoding=utf-8` in `jdbc_url`

    > Note: After version 1.2.3, these parameters will be automatically added when using JDBC Catalog to connect to the MySQL database.

8. An error is reported when connecting to the MySQL database through the JDBC Catalog: `Establishing SSL connection without server's identity verification is not recommended`

    Please add `useSSL=true` in `jdbc_url`

9. An error is reported when connecting Hive Catalog: `Caused by: java.lang.NullPointerException`

    If there is stack trace in fe.log:

    ```
    Caused by: java.lang.NullPointerException
        at org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook.getFilteredObjects(AuthorizationMetaStoreFilterHook.java:78) ~[hive-exec-3.1.3-core.jar:3.1.3]
        at org.apache.hadoop.hive.ql.security.authorization.plugin.AuthorizationMetaStoreFilterHook.filterDatabases(AuthorizationMetaStoreFilterHook.java:55) ~[hive-exec-3.1.3-core.jar:3.1.3]
        at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getAllDatabases(HiveMetaStoreClient.java:1548) ~[doris-fe.jar:3.1.3]
        at org.apache.hadoop.hive.metastore.HiveMetaStoreClient.getAllDatabases(HiveMetaStoreClient.java:1542) ~[doris-fe.jar:3.1.3]
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[?:1.8.0_181]
    ```

    Try adding `"metastore.filter.hook" = "org.apache.hadoop.hive.metastore.DefaultMetaStoreFilterHookImpl"` in `create catalog` statement.

10. An error is reported when connecting to the Hive database through the Hive Catalog: `RemoteException: SIMPLE authentication is not enabled. Available: [TOKEN, KERBEROS]`

    If both `show databases` and `show tables` are OK, and the above error occurs when querying, we need to perform the following two operations:
    - Core-site.xml and hdfs-site.xml need to be placed in the fe/conf and be/conf directories
    - The BE node executes the kinit of Kerberos, restarts the BE, and then executes the query.

11. If the `show tables` is normal after creating the Hive Catalog, but the query report `java.net.UnknownHostException: xxxxx`

    Add a property in CATALOG:
    ```
    'fs.defaultFS' = 'hdfs://<your_nameservice_or_actually_HDFS_IP_and_port>'
    ```
12. The values of the partition fields in the hudi table can be found on hive, but they cannot be found on doris.

    Doris and hive currently query hudi differently. Doris needs to add partition fields to the avsc file of the hudi table structure. If not added, it will cause Doris to query partition_ Val is empty (even if home. datasource. live_sync. partition_fields=partition_val is set)

    ```
    {
        "type": "record",
        "name": "record",
        "fields": [{
            "name": "partition_val",
            "type": [
                "null",
                "string"
                ],
            "doc": "Preset partition field, empty string when not partitioned",
            "default": null
            },
            {
            "name": "name",
            "type": "string",
            "doc": "名称"
            },
            {
            "name": "create_time",
            "type": "string",
            "doc": "创建时间"
            }
        ]
    }
    ```

13. The table in orc format of Hive 1.x may encounter system column names such as `_col0`, `_col1`, `_col2`... in the underlying orc file schema, which need to be specified in the catalog configuration. Add `hive.version` to 1.x.x so that it will use the column names in the hive table for mapping.

    ```sql
    CREATE CATALOG hive PROPERTIES (
        'hive.version' = '1.x.x'
    );
    ```

14. When using JDBC Catalog to synchronize MySQL data to Doris, the date data synchronization error occurs. It is necessary to check whether the MySQL version corresponds to the MySQL driver package. For example, the driver com.mysql.cj.jdbc.Driver is required for MySQL8 and above.
