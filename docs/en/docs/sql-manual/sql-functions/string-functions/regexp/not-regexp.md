---
{
    "title": "NOT REGEXP",
    "language": "en"
}
---

<!-- 
licensed to the apache software foundation (asf) under one
or more contributor license agreements.  see the notice file
distributed with this work for additional information
regarding copyright ownership.  the asf licenses this file
to you under the apache license, version 2.0 (the
"license"); you may not use this file except in compliance
with the license.  you may obtain a copy of the license at

  http://www.apache.org/licenses/license-2.0

unless required by applicable law or agreed to in writing,
software distributed under the license is distributed on an
"as is" basis, without warranties or conditions of any
kind, either express or implied.  see the license for the
specific language governing permissions and limitations
under the license.
-->

## not regexp
### description
#### syntax

`BOOLEAN not regexp(VARCHAR str, VARCHAR pattern)`

Perform regular matching on the string str, return false if it matches, and return true if it doesn't match. pattern is a regular expression.

### example

```
// Find all data in the k1 field that does not start with 'billie'
mysql> select k1 from test where k1 not regexp '^billie';
+--------------------+
| k1                 |
+--------------------+
| Emmy eillish       |
+--------------------+

// Find all the data in the k1 field that does not end with 'ok':
mysql> select k1 from test where k1 not regexp 'ok$';
+------------+
| k1         |
+------------+
| It's true  |
+------------+
```

### keywords
    REGEXP, NOT, NOT REGEXP
