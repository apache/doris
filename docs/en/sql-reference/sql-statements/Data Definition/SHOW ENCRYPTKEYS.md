---
{
    "title": "SHOW ENCRYPTKEYS",
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

# SHOW ENCRYPTKEYS

## Description

### Syntax

```
SHOW ENCRYPTKEYS [IN|FROM db] [LIKE 'key_pattern']
```

### Parameters

>`db`: the name of the database to query
>`key_pattern`: parameter used to filter key names  

View all custom keys under the database. If the user specifies a database, then view the corresponding database, otherwise query the current session's database directly.

You need to have `ADMIN` privileges for this database.

## Example

    ```
    mysql> SHOW ENCRYPTKEYS;
    +-------------------+-------------------+
    | EncryptKey Name   | EncryptKey String |
    +-------------------+-------------------+
    | example_db.my_key | ABCD123456789     |
    +-------------------+-------------------+
    1 row in set (0.00 sec)

    mysql> SHOW ENCRYPTKEYS FROM example_db LIKE "%my%";
    +-------------------+-------------------+
    | EncryptKey Name   | EncryptKey String |
    +-------------------+-------------------+
    | example_db.my_key | ABCD123456789     |
    +-------------------+-------------------+
    1 row in set (0.00 sec)
    ```

## keyword

    SHOW,ENCRYPTKEYS
