---
{
"title": "ORTHOGONAL_BITMAP_EXPR_CALCULATE",
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

## orthogonal_bitmap_expr_calculate
### description
#### Syntax

`BITMAP ORTHOGONAL_BITMAP_EXPR_CALCULATE(bitmap_column, column_to_filter, input_string)`
求表达式bitmap交并差集合计算函数, 第一个参数是Bitmap列，第二个参数是用来过滤的维度列，即计算的key列，第三个参数是计算表达式字符串，含义是依据key列进行bitmap交并差集表达式计算
表达式支持的计算符：& 代表交集计算，| 代表并集计算，- 代表差集计算, ^ 代表异或计算，\ 代表转义字符

### example

```sql
select orthogonal_bitmap_expr_calculate(user_id, tag, '(833736|999777)&(1308083|231207)&(1000|20000-30000)') from user_tag_bitmap where tag in (833736,999777,130808,231207,1000,20000,30000);
注：1000、20000、30000等整形tag，代表用户不同标签
```

```sql
select orthogonal_bitmap_expr_calculate(user_id, tag, '(A:a/b|B:2\\-4)&(C:1-D:12)&E:23') from user_str_tag_bitmap where tag in ('A:a/b', 'B:2-4', 'C:1', 'D:12', 'E:23');
 注：'A:a/b', 'B:2-4'等是字符串类型tag，代表用户不同标签, 其中'B:2-4'需要转义成'B:2\\-4'
```

### keywords

   ORTHOGONAL_BITMAP_EXPR_CALCULATE,BITMAP
