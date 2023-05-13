---
{
"title": "ALTER-RESOURCE",
"language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

## ALTER-RESOURCE

<version since="1.2.0"></version>

### Name

ALTER RESOURCE

### Description

This statement is used to modify an existing resource. Only the root or admin user can modify resources.
Syntax:
```sql
ALTER RESOURCE 'resource_name'
PROPERTIES ("key"="value", ...);
```

Note: The resource type does not support modification.

### Example

1. Modify the working directory of the Spark resource named spark0:

```sql
ALTER RESOURCE 'spark0' PROPERTIES ("working_dir" = "hdfs://127.0.0.1:10000/tmp/doris_new");
```
2. Modify the maximum number of connections to the S3 resource named remote_s3:

```sql
ALTER RESOURCE 'remote_s3' PROPERTIES ("s3.connection.maximum" = "100");
```

3. Modify information related to cold and hot separation S3 resources
- Support
  - `s3.access_key`  s3 ak
  - `s3.secret_key`  s3 sk
  - `s3.session_token` s3 token
  - `s3.connection.maximum` default 50
  - `s3.connection.timeout` default 1000ms
  - `s3.connection.request.timeout` default 3000ms
- Not Support
  - `s3.region`
  - `s3.bucket"`
  - `s3.root.path`
  - `s3.endpoint`

```sql
  ALTER RESOURCE "showPolicy_1_resource" PROPERTIES("s3.connection.maximum" = "1111");
```
### Keywords

```text
ALTER, RESOURCE
```

### Best Practice
