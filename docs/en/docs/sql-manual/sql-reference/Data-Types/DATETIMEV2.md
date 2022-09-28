---
{
    "title": "DATETIMEV2",
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

## DATETIMEV2
### Description
DATETIMEV2([P])
Date and time type.
The optional parameter P indicates the time precision and the value range is [0, 6], that is, it supports up to 6 decimal places (microseconds). 0 when not set.
Value range is ['0000-01-01 00:00:00[.000000]','9999-12-31 23:59:59[.999999]'].
The form of printing is 'YYYY-MM-DD HH:MM:SS.ffffff'

### note

Compared with the DATETIME type, DATETIMEV2 is more efficient and supports a time precision of up to microseconds.

### keywords
DATETIMEV2
