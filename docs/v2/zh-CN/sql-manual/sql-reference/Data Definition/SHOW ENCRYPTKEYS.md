---
{
    "title": "SHOW ENCRYPTKEYS",
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

# SHOW ENCRYPTKEYS

## Description

### Syntax

```
SHOW ENCRYPTKEYS [IN|FROM db] [LIKE 'key_pattern']
```

### Parameters

>`db`: 要查询的数据库名字
>`key_pattern`: 用来过滤密钥名称的参数  

查看数据库下所有的自定义的密钥。如果用户指定了数据库，那么查看对应数据库的，否则直接查询当前会话所在数据库。

需要对这个数据库拥有 `ADMIN` 权限

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
