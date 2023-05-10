---
{
"title": "AES",
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

## AES_ENCRYPT

### Name

AES_ENCRYPT

### description

Encryption of data using the OpenSSL. This function is consistent with the `AES_ENCRYPT` function in MySQL. Using AES_128_ECB algorithm by default, and the padding mode is PKCS7.
Reference: https://dev.mysql.com/doc/refman/8.0/en/encryption-functions.html#function_aes-decrypt

### Compatibility

1. aes_decrypt/aes_encrypt/sm4_decrypt/sm4_encrypt When the initial vector is not provided, block_encryption_mode will not take effect, and AES_128_ECB will be used for encryption and decryption in the end, which is inconsistent with the behavior of MySQL.
2. Add aes_decrypt_v2/aes_encrypt_v2/sm4_decrypt_v2/sm4_encrypt_v2 functions to support correct behavior. When no initial vector is provided, block_encryption_mode can take effect, aes-192-ecb and aes-256-ecb will be correctly encrypted and decrypted, and other block encryption modes will report an error. If there is no need to be compatible with old data, the v2 function can be used directly.

#### Syntax

`AES_ENCRYPT(str, key_str[, init_vector])`

#### Arguments

- `str`: Content to be encrypted
- `key_str`: Secret key
- `init_vector`: Initialization Vector. The default value for the block_encryption_mode system variable is aes ecb mode, which does not require an initialization vector. The alternative permitted block encryption modes CBC, CFB1, CFB8, CFB128, and OFB all require an initialization vector.

#### Return Type

VARCHAR(*)

#### Remarks

The AES_ENCRYPT function is not used the user secret key directly, but will be further processed. The specific steps are as follows:
1. Determine the number of bytes of the SECRET KEY according to the encryption algorithm used. For example, if you using AES_128_ECB, then the number of bytes of SECRET KEY are `128 / 8 = 16`(if using AES_256_ECB, then SECRET KEY length are `128 / 8 = 32`);
2. Then XOR the `i` bit and the `16*k+i` bit of the SECRET KEY entered by the user. If the length of the SECRET KEY less than 16 bytes, 0 will be padded;
3. Finally, use the newly generated key for encryption;

### example

```sql
select to_base64(aes_encrypt('text','F3229A0B371ED2D9441B830D21A390C3'));
```

The results are consistent with those executed in MySQL.

```text
+--------------------------------+
| to_base64(aes_encrypt('text')) |
+--------------------------------+
| wr2JEDVXzL9+2XtRhgIloA==       |
+--------------------------------+
1 row in set (0.01 sec)
```

If you want to change other encryption algorithms, you can:

```sql
set block_encryption_mode="AES_256_CBC";
select to_base64(aes_encrypt('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));
```

Here is the result:

```text
+-----------------------------------------------------+
| to_base64(aes_encrypt('text', '***', '0123456789')) |
+-----------------------------------------------------+
| tsmK1HzbpnEdR2//WhO+MA==                            |
+-----------------------------------------------------+
1 row in set (0.01 sec)
```

For more information about `block_encryption_mode`, see also [variables](../../../advanced/variables.md).

### keywords

    AES_ENCRYPT

## AES_DECRYPT

### Name

AES_DECRYPT

### Description

Decryption of data using the OpenSSL. This function is consistent with the `AES_DECRYPT` function in MySQL. Using AES_128_ECB algorithm by default, and the padding mode is PKCS7.

#### Syntax

```
AES_DECRYPT(str,key_str[,init_vector])
```

#### Arguments

- `str`: Content that encrypted
- `key_str`: Secret key
- `init_vector`: Initialization Vector

#### Return Type

VARCHAR(*)

### example

```sql
select aes_decrypt(from_base64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');
```

The results are consistent with those executed in MySQL.

```text
+------------------------------------------------------+
| aes_decrypt(from_base64('wr2JEDVXzL9+2XtRhgIloA==')) |
+------------------------------------------------------+
| text                                                 |
+------------------------------------------------------+
1 row in set (0.01 sec)
```

If you want to change other encryption algorithms, you can:

```sql
set block_encryption_mode="AES_256_CBC";
select aes_decrypt(from_base64('tsmK1HzbpnEdR2//WhO+MA=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');
```

Here is the result:

```text
+---------------------------------------------------------------------------+
| aes_decrypt(from_base64('tsmK1HzbpnEdR2//WhO+MA=='), '***', '0123456789') |
+---------------------------------------------------------------------------+
| text                                                                      |
+---------------------------------------------------------------------------+
1 row in set (0.01 sec)
```

For more information about `block_encryption_mode`, see also [variables](../../../advanced/variables.md).

### keywords

    AES_DECRYPT
