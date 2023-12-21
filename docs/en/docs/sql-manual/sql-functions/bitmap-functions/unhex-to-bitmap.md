---
{
    "title": "UNHEX_TO_BITMAP",
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

## unhex_to_bitmap
### description
#### Syntax

`BITMAP UNHEX_TO_BITMAP(expr)`

Usage scenario: The binary data of bitmap can be detected from doris, and the binary data can be transferred back to doris after calculation. 
 
The input is a string in hexadecimal format of bitmap binary data. The first two digits indicate the bitmap type: 0 is empty; 1 is a single element; 2 is multiple elements. The last eight digits are the data content in the small-endian format. 
Output is a bitmap object. 
When the input value is not in this range, NULL is returned.

### example

```
mysql> select bitmap_to_string(unhex_to_bitmap('0106000000'));
+-------------------------------------------------+
| bitmap_to_string(unhex_to_bitmap('0106000000')) |
+-------------------------------------------------+
| 6                                               |
+-------------------------------------------------+
```

### keywords

    UNHEX_TO_BITMAP,BITMAP
