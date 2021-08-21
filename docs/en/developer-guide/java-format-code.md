---
{
    "title": "Java Format Code",
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

# Java Format Code

The formatting of the Java part of the code in Doris is usually done automatically by the IDE. Only the general format rules are listed here. For developer, you need to set the corresponding code styles in different IDEs according to the format rules.

## Import Order

```
org.apache.doris
<blank line>
com
<blank line>
org
<blank line>
java
<blank line>
javax
<blank line>
```

* Do not use `import *`
* Do not use `import static`
