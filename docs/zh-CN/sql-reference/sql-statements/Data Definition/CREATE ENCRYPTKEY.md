---
{
    "title": "CREATE ENCRYPTKEY",
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

# CREATE ENCRYPTKEY

## Description

### Syntax

```
CREATE ENCRYPTKEY key_name
    AS "key_string"
```

### Parameters

> `key_name`: 要创建密钥的名字, 可以包含数据库的名字。比如：`db1.my_key`。
>
> `key_string`: 要创建密钥的字符串

此语句创建一个自定义密钥。执行此命令需要用户拥有 `ADMIN` 权限。

如果 `key_name` 中包含了数据库名字，那么这个自定义密钥会创建在对应的数据库中，否则这个函数将会创建在当前会话所在的数据库。新密钥的名字不能够与对应数据库中已存在的密钥相同，否则会创建失败。

## Example

1. 创建一个自定义密钥

	```
	CREATE ENCRYPTKEY my_key AS "ABCD123456789";
	```
	
2. 使用自定义密钥

    使用自定义密钥需在密钥前添加关键字 `KEY`/`key`，与 `key_name` 空格隔开。
    
    ```
    mysql> SELECT HEX(AES_ENCRYPT("Doris is Great", KEY my_key));
    +------------------------------------------------+
    | hex(aes_encrypt('Doris is Great', key my_key)) |
    +------------------------------------------------+
    | D26DB38579D6A343350EDDC6F2AD47C6               |
    +------------------------------------------------+
    1 row in set (0.02 sec)

    mysql> SELECT AES_DECRYPT(UNHEX('D26DB38579D6A343350EDDC6F2AD47C6'), KEY my_key);
    +--------------------------------------------------------------------+
    | aes_decrypt(unhex('D26DB38579D6A343350EDDC6F2AD47C6'), key my_key) |
    +--------------------------------------------------------------------+
    | Doris is Great                                                     |
    +--------------------------------------------------------------------+
    1 row in set (0.01 sec)
    ```

## Keyword

    CREATE,ENCRYPTKEY
