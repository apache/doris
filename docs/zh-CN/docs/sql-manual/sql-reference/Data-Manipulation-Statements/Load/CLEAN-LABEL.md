---
{
    "title": "CLEAN-LABEL",
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

## CLEAN-LABEL

### Name

CLEAN LABEL

### Description

用于手动清理历史导入作业的 Label。清理后，Label 可以重复使用。

语法:

```sql
CLEAN LABEL [label] FROM db;
```

### Example

1. 清理 db1 中，Label 为 label1 的导入作业。

	```sql
	CLEAN LABEL label1 FROM db1;
	```

2. 清理 db1 中所有历史 Label。

	```sql
	CLEAN LABEL FROM db1;
	```

### Keywords

    CLEAN, LABEL

### Best Practice

