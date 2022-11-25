---
{
    "title": "JSONB",
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

## DATEV2
### description
    JSONB (JSON Binary) datatype.
        Use binary JSON format for storage and jsonb function to extract field.

### note
    There are some advantanges for JSONB over plain JSON STRING.
    1. JSON syntax will be validated on write to ensure data quality
    2. JSONB format is more efficient. Using jsonb_extract functions on JSONB format is 2-4 times faster than get_json_xx on JSON STRING format.

### example
    refer to jsob tutorial.

### keywords

    JSONB JSON
