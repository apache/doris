---
{
    "title": "PARSE_URL",
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

## parse_url
### description
#### Syntax

`VARCHAR  parse_url(VARCHAR url, VARCHAR  name)`


From the URL, the field corresponding to name is resolved. The name options are as follows: 'PROTOCOL', 'HOST', 'PATH', 'REF', 'AUTHORITY', 'FILE', 'USERINFO', 'PORT', 'QUERY', and the result is returned.

### example

```
mysql> SELECT parse_url ('https://doris.apache.org/', 'HOST');
+------------------------------------------------+
| parse_url('https://doris.apache.org/', 'HOST') |
+------------------------------------------------+
| doris.apache.org                               |
+------------------------------------------------+
```

If you want to get parameter in QUERY, you can use [extract_url_parameter](./extract_url_parameter.md).

### keywords
    PARSE URL
