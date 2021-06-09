---
{
    "title": "DROP FUNCTION",
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

# DROP FUNCTION
##Description
### Syntax

```
DROP FUNCTION function_name
(angry type [...])
```

### Parameters

>` function_name': To delete the name of the function
>
>` arg_type`: To delete the parameter list of the function
>


Delete a custom function. The name of the function and the type of the parameter are exactly the same before they can be deleted.

## example

1. Delete a function

```
DROP FUNCTION my_add(INT, INT)
```
## keyword
DROP,FUNCTION
