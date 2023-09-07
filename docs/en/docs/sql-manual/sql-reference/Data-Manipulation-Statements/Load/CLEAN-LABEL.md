---
{
    "title": "CLEAN-LABEL",
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

## CLEAN-LABEL

### Name

CLEAN LABEL

### Description

For manual cleanup of historical load jobs. After cleaning, the Label can be reused.

Syntax:

```sql
CLEAN LABEL [label] FROM db;
```

### Example

1. Clean label label1 from database db1

	```sql
	CLEAN LABEL label1 FROM db1;
	```

2. Clean all labels from database db1

	```sql
	CLEAN LABEL FROM db1;
	```

### Keywords

    CLEAN, LABEL

### Best Practice

