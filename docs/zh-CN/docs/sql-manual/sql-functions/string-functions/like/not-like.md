---
{
    "title": "NOT LIKE",
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

## not like
### description
#### syntax

`BOOLEAN not like(VARCHAR str, VARCHAR pattern)`

对字符串 str 进行模糊匹配，匹配上的则返回 false，没匹配上则返回 true。

like 匹配/模糊匹配，会与 % 和 _ 结合使用。

百分号 '%' 代表零个、一个或多个字符。

下划线 '_' 代表单个字符。

```
'a'      // 精准匹配，和 `=` 效果一致
'%a'     // 以a结尾的数据
'a%'     // 以a开头的数据
'%a%'    // 含有a的数据
'_a_'    // 三位且中间字母是 a 的数据
'_a'     // 两位且结尾字母是 a 的数据
'a_'     // 两位且开头字母是 a 的数据
'a__b'  // 四位且以字符a开头、b结尾的数据
```
### example

```
// table test
+-------+
| k1    |
+-------+
| b     |
| bb    |
| bab   |
| a     |
+-------+

// 返回 k1 字符串中不包含 a 的数据
mysql > select k1 from test where k1 not like '%a%';
+-------+
| k1    |
+-------+
| b     |
| bb    |
+-------+

// 返回 k1 字符串中不等于 a 的数据
mysql > select k1 from test where k1 not like 'a';
+-------+
| k1    |
+-------+
| b     |
| bb    |
| bab   |
+-------+
```

### keywords
    LIKE, NOT, NOT LIKE
