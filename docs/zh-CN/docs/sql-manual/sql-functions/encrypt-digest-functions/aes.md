---
{
"title": "AES",
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

## AES_ENCRYPT

### Name

AES_ENCRYPT

### Description

Aes 加密函数。该函数与 MySQL 中的 `AES_ENCRYPT` 函数行为一致。默认采用 AES_128_ECB 算法，padding 模式为 PKCS7。底层使用 OpenSSL 库进行加密。
Reference: https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt

### Compatibility

1. aes_decrypt/aes_encrypt/sm4_decrypt/sm4_encrypt 当没有提供初始向量时，block_encryption_mode 不生效，最终都会使用 AES_128_ECB 加解密，这和 MySQL 的行为不一致。
2. 增加 aes_decrypt_v2/aes_encrypt_v2/sm4_decrypt_v2/sm4_encrypt_v2 函数支持正确的行为，没有提供初始向量时，block_encryption_mode 可以生效，aes-192-ecb 和 aes-256-ecb 将正确加解密，其他块加密模式将报错。如果无需兼容旧数据，可直接使用v2函数。

#### Syntax

`AES_ENCRYPT(str, key_str[, init_vector])`

#### Arguments

- `str`: 待加密的内容
- `key_str`: 密钥
- `init_vector`: 初始向量。block_encryption_mode 默认值为 aes-128-ecb，它不需要初始向量，可选的块加密模式 CBC、CFB1、CFB8、CFB128 和 OFB 都需要一个初始向量。

#### Return Type

VARCHAR(*)

#### Remarks

AES_ENCRYPT 函数对于传入的密钥，并不是直接使用，而是会进一步做处理，具体步骤如下：
1. 根据使用的加密算法，确定密钥的字节数，比如使用 AES_128_ECB 算法，则密钥字节数为 `128 / 8 = 16`（如果使用 AES_256_ECB 算法，则密钥字节数为 `128 / 8 = 32`）；
2. 然后针对用户输入的密钥，第 `i` 位和第 `16*k+i` 位进行异或，如果用户输入的密钥不足 16 位，则后面补 0；
3. 最后，再使用新生成的密钥进行加密；

### Example

```sql
select to_base64(aes_encrypt('text','F3229A0B371ED2D9441B830D21A390C3'));
```

结果与在 MySQL 中执行的结果一致，如下：

```text
+--------------------------------+
| to_base64(aes_encrypt('text')) |
+--------------------------------+
| wr2JEDVXzL9+2XtRhgIloA==       |
+--------------------------------+
1 row in set (0.01 sec)
```

如果你想更换其他加密算法，可以

```sql
set block_encryption_mode="AES_256_CBC";
select to_base64(aes_encrypt('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));
```

结果如下：

```text
+-----------------------------------------------------+
| to_base64(aes_encrypt('text', '***', '0123456789')) |
+-----------------------------------------------------+
| tsmK1HzbpnEdR2//WhO+MA==                            |
+-----------------------------------------------------+
1 row in set (0.01 sec)
```

关于 `block_encryption_mode` 可选的值可以参见：[变量章节](../../../advanced/variables.md)。

### Keywords

AES_ENCRYPT

## AES_DECRYPT

### Name

AES_DECRYPT

### Description

Aes 解密函数。该函数与 MySQL 中的 `AES_DECRYPT` 函数行为一致。默认采用 AES_128_ECB 算法，padding 模式为 PKCS7。底层使用 OpenSSL 库进行加密。

#### Syntax

```
AES_DECRYPT(str,key_str[,init_vector])
```

#### Arguments

- `str`: 已加密的内容
- `key_str`: 密钥
- `init_vector`: 初始向量

#### Return Type

VARCHAR(*)

### Example

```sql
select aes_decrypt(from_base64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');
```

结果与在 MySQL 中执行的结果一致，如下：

```text
+------------------------------------------------------+
| aes_decrypt(from_base64('wr2JEDVXzL9+2XtRhgIloA==')) |
+------------------------------------------------------+
| text                                                 |
+------------------------------------------------------+
1 row in set (0.01 sec)
```

如果你想更换其他加密算法，可以

```sql
set block_encryption_mode="AES_256_CBC";
select AES_DECRYPT(FROM_BASE64('tsmK1HzbpnEdR2//WhO+MA=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');
```

结果如下：

```text
+---------------------------------------------------------------------------+
| aes_decrypt(from_base64('tsmK1HzbpnEdR2//WhO+MA=='), '***', '0123456789') |
+---------------------------------------------------------------------------+
| text                                                                      |
+---------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

关于 `block_encryption_mode` 可选的值可以参见：[变量章节](../../../advanced/variables.md)。

### Keywords

    AES_DECRYPT
