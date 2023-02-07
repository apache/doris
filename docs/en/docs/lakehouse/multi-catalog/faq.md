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

   In Doris 1.2.1 and the older versions, gsasl is disabled for libhdfs3, so please update to Doris 1.2.2 or newer.

3. What to do with the`java.lang.VerifyError: xxx` error when accessing HDFS 3.x?

   Doris 1.2.1 and the older versions rely on Hadoop 2.8. Please update Hadoop to 2.10.2 or update Doris to 1.2.2 or newer.
