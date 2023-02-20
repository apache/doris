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

5. When querying a table in ORC format, FE reports an error `Could not obtain block`

    For ORC files, by default, FE will access HDFS to obtain file information and split files. In some cases, FE may not be able to access HDFS. It can be solved by adding the following parameters:

    `"hive.exec.orc.split.strategy" = "BI"`

    Other options: HYBRID (default), ETL.
