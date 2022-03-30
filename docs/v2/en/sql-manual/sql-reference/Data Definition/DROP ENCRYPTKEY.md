---
{
    "title": "DROP ENCRYPTKEY",
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

# DROP ENCRYPTKEY

## Description

### Syntax

```
DROP ENCRYPTKEY key_name
```

### Parameters

> `key_name`: To delete the name of the key, you can include the name of the database. For example: `db1.my_key`.

Delete a custom key. A key can be deleted only if its name is identical.

Executing this command requires the user to have the `ADMIN` privileges.

## example

1. Delete a key.

```
DROP ENCRYPTKEY my_key;
```

## keyword

    DROP,ENCRYPTKEY
