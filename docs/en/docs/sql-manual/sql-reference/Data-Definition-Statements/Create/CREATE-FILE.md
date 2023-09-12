---
{
    "title": "CREATE-FILE",
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

## CREATE-FILE

### Name

CREATE FILE

### Description

This statement is used to create and upload a file to the Doris cluster.
This function is usually used to manage files that need to be used in some other commands, such as certificates, public and private keys, and so on.

This command can only be executed by users with `admin` privileges.
A certain file belongs to a certain database. This file can be used by any user with access rights to database.

A single file size is limited to 1MB.
A Doris cluster can upload up to 100 files.

grammar:

```sql
CREATE FILE "file_name" [IN database]
PROPERTIES("key"="value", ...)
````

illustrate:

- file_name: custom file name.
- database: The file belongs to a certain db, if not specified, the db of the current session is used.
- properties supports the following parameters:
    - url: Required. Specifies the download path for a file. Currently only unauthenticated http download paths are supported. After the command executes successfully, the file will be saved in doris and the url will no longer be needed.
    - catalog: Required. The classification name of the file can be customized. However, in some commands, files in the specified catalog are looked up. For example, in the routine import, when the data source is kafka, the file under the catalog name kafka will be searched.
    - md5: optional. md5 of the file. If specified, verification will be performed after the file is downloaded.

### Example

1. Create a file ca.pem , classified as kafka

   ```sql
   CREATE FILE "ca.pem"
   PROPERTIES
   (
       "url" = "https://test.bj.bcebos.com/kafka-key/ca.pem",
       "catalog" = "kafka"
   );
   ````

2. Create a file client.key, classified as my_catalog

   ```sql
   CREATE FILE "client.key"
   IN my_database
   PROPERTIES
   (
       "url" = "https://test.bj.bcebos.com/kafka-key/client.key",
       "catalog" = "my_catalog",
       "md5" = "b5bb901bf10f99205b39a46ac3557dd9"
   );
   ````

### Keywords

````text
CREATE, FILE
````

### Best Practice

1. This command can only be executed by users with amdin privileges. A certain file belongs to a certain database. This file can be used by any user with access rights to database.

2. File size and quantity restrictions.

   This function is mainly used to manage some small files such as certificates. So a single file size is limited to 1MB. A Doris cluster can upload up to 100 files.

