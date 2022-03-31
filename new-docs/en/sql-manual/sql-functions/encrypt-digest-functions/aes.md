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

# AES_ENCRYPT

## description
encryption of data using the official AES
### Syntax

`VARCHAR AES_ENCRYPT(str,key_str[,init_vector])`

## example

```
MySQL > select to_base64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3'));
+--------------------------------+
| to_base64(aes_encrypt('text')) |
+--------------------------------+
| wr2JEDVXzL9+2XtRhgIloA==       |
+--------------------------------+
1 row in set (0.010 sec)

MySQL> set block_encryption_mode="AES_256_CBC";
Query OK, 0 rows affected (0.006 sec)

MySQL > select to_base64(AES_ENCRYPT('text','F3229A0B371ED2D9441B830D21A390C3', '0123456789'));
+----------------------------------------------------------------------------------+
| to_base64(aes_encrypt('text', 'F3229A0B371ED2D9441B830D21A390C3', '0123456789')) |
+----------------------------------------------------------------------------------+
| mvZT1KJw7N0RJf27aipUpg==                                                         |
+----------------------------------------------------------------------------------+
1 row in set (0.011 sec)
```

# AES_DECRYPT

## description
decryption of data using the official AES (### Syntax

`VARCHAR AES_DECRYPT(str,key_str[,init_vector])`

## example

```
MySQL > select AES_DECRYPT(FROM_BASE64('wr2JEDVXzL9+2XtRhgIloA=='),'F3229A0B371ED2D9441B830D21A390C3');
+------------------------------------------------------+
| aes_decrypt(from_base64('wr2JEDVXzL9+2XtRhgIloA==')) |
+------------------------------------------------------+
| text                                                 |
+------------------------------------------------------+
1 row in set (0.012 sec)

MySQL> set block_encryption_mode="AES_256_CBC";
Query OK, 0 rows affected (0.006 sec)

MySQL > select AES_DECRYPT(FROM_BASE64('mvZT1KJw7N0RJf27aipUpg=='),'F3229A0B371ED2D9441B830D21A390C3', '0123456789');
+--------------------------------------------------------------------------------------------------------+
| aes_decrypt(from_base64('mvZT1KJw7N0RJf27aipUpg=='), 'F3229A0B371ED2D9441B830D21A390C3', '0123456789') |
+--------------------------------------------------------------------------------------------------------+
| text                                                                                                   |
+--------------------------------------------------------------------------------------------------------+
1 row in set (0.012 sec)
```

## keyword

    AES_ENCRYPT, AES_DECRYPT