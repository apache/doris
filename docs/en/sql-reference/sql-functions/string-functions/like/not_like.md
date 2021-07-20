---
{
    "title": "not like",
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

# not like
## description
### syntax

'BOOLEAN not like(VARCHAR str, VARCHAR pattern)'

Perform fuzzy matching on the string str, return false if it matches, and return true if it doesn't match.

like match/fuzzy match, will be used in combination with % and _.

'a'   // Precise matching, the same effect as `=`.
'%a'  // data ending with a
'a%'  // data starting with a
'%a%' // data containing a
'_a_' // Three digits and the middle letter is a
'_a'  // Two digits and the ending letter is a
'a_'  // Two digits and the initial letter is a

## example

```
// Return data that does not contain a in the k1 string
mysql> select k1 from test where k1 not like '%a%';
+-------+
| k1    |
+-------+
| b     |
| bb    |
+-------+

// Return the data that is not equal to a in the k1 string
mysql> select k1 from test where k1 not like 'a';
+-------+
| k1    |
+-------+
| bab   |
| b     |
+-------+
```

## keyword
LIKE, NOT, NOT LIKE
