---
{
    "title": "QUARTER",
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

## quarter
### description
#### Syntax

`INT quarter(DATETIME date)`

Returns the quarter to which the specified date belongs, as an INT

### Example

```
mysql> select quarter('2022-09-22 17:00:00');
+--------------------------------+
| quarter('2022-09-22 17:00:00') |
+--------------------------------+
|                              3 |
+--------------------------------+
```

### keywords

    quarter
