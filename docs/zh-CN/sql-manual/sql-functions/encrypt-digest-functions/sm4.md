---
{
"title": "SM4",
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

## SM4_ENCRYPT

### description
SM4 加密函数
#### Syntax

`VARCHAR SM4_ENCRYPT(str,key_str[,init_vector])`

返回加密后的结果

### example

```
MySQL > select TO_BASE64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));
+--------------------------------+
| to_base64(sm4_encrypt('text')) |
+--------------------------------+
| aDjwRflBrDjhBZIOFNw3Tg==       |
+--------------------------------+
1 row in set (0.010 sec)

MySQL > set block_encryption_mode="SM4_128_CBC";
Query OK, 0 rows affected (0.001 sec)

MySQL > select to_base64(SM4_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));
+----------------------------------------------------------------------------------+
| to_base64(sm4_encrypt('text', 'F3229A0B371ED2D9441B830D21A390C3', '0123456789')) |
+----------------------------------------------------------------------------------+
| G7yqOKfEyxdagboz6Qf01A==                                                         |
+----------------------------------------------------------------------------------+
1 row in set (0.014 sec)
```

### keywords

SM4_ENCRYPT

## SM4_DECRYPT

### description
Aes 解密函数
#### Syntax

`VARCHAR AES_DECRYPT(str,key_str[,init_vector])`

返回解密后的结果

### example

```
MySQL [(none)]> select SM4_DECRYPT(FROM_BASE64('aDjwRflBrDjhBZIOFNw3Tg=='),'F3229A0B371ED2D9441B830D21A390C3');
+------------------------------------------------------+
| sm4_decrypt(from_base64('aDjwRflBrDjhBZIOFNw3Tg==')) |
+------------------------------------------------------+
| text                                                 |
+------------------------------------------------------+
1 row in set (0.009 sec)

MySQL> set block_encryption_mode="SM4_128_CBC";
Query OK, 0 rows affected (0.006 sec)

MySQL > select SM4_DECRYPT(FROM_BASE64('G7yqOKfEyxdagboz6Qf01A=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');
+--------------------------------------------------------------------------------------------------------+
| sm4_decrypt(from_base64('G7yqOKfEyxdagboz6Qf01A=='), 'F3229A0B371ED2D9441B830D21A390C3', '0123456789') |
+--------------------------------------------------------------------------------------------------------+
| text                                                                                                   |
+--------------------------------------------------------------------------------------------------------+
1 row in set (0.012 sec)
```

### keywords

    SM4_DECRYPT
