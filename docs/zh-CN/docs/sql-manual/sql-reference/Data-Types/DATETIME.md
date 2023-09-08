---
{
    "title": "DATETIME",
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

## DATETIME

<version since="1.2.0">

DATETIMEV2

</version>

### description

    DATETIME([P])
    日期时间类型，可选参数P表示时间精度，取值范围是[0, 6]，即最多支持6位小数（微秒）。不设置时为0。
    取值范围是['0000-01-01 00:00:00[.000000]', '9999-12-31 23:59:59[.999999]'].
    打印的形式是'yyyy-MM-dd HH:mm:ss.SSSSSS'

### note

    DATETIME支持了最多到微秒的时间精度。

### keywords

    DATETIME
