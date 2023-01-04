---
{
"title": "orthogonal_bitmap_expr_calculate",
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

## orthogonal_bitmap_expr_calculate
### description
#### Syntax

`BITMAP ORTHOGONAL_BITMAP_EXPR_CALCULATE(bitmap_column, column_to_filter, input_string)`
The first parameter is the Bitmap column, the second parameter is the dimension column used for filtering, that is, the calculated key column, and the third parameter is the calculation expression string, meaning that the bitmap intersection, union, and difference set expression is calculated according to the key column
The calculators supported by the expression:&represents intersection calculation, | represents union calculation, - represents difference calculation, ^ represents XOR calculation, and \ represents escape characters

### example

```
mysql> select orthogonal_bitmap_expr_calculate(user_id, tag, '((A|B)&(C)&(D-E))') from tag_map_tbl where tag in ('A', 'B', 'C', 'D', 'E');
```

### keywords

   ORTHOGONAL_BITMAP_EXPR_CALCULATE,BITMAP
