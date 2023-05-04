---
{
    "title": "DECIMAL",
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

## DECIMAL
### Description
DECIMAL (M [,D])

High-precision fixed-point, M stands for the total number of significant numbers (precision), D stands for the maximum number of decimal points (scale).
The range of M is [1, 27], the range of D is [0, 9], the integer part is [1, 18].

in addition, M must be greater than or equal to the value of D. 

The default value is DECIMAL(9, 0).

### note
We intend to delete this type in 2024. At this stage, Doris prohibits creating tables containing the `DECIMAL` type by default. If you need to use it, you need to add `disable_decimalv2 = false` in the FE's config and restart the FE.

### keywords
DECIMAL
