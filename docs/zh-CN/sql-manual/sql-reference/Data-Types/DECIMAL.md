---
{
    "title": "DECIMAL",
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

## DECIMAL
### description
    DECIMAL(M[,D])
    高精度定点数，M代表一共有多少个有效数字(precision)，D代表小数点后最多有多少数字(scale)
    M的范围是[1,27], D的范围[1, 9], 另外，M必须要大于等于D的取值。默认的D取值为0

### keywords

    DECIMAL
