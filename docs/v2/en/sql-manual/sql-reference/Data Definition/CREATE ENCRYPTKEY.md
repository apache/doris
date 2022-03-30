---
{
    "title": "CREATE ENCRYPTKEY",
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

# CREATE ENCRYPTKEY

## Description

### Syntax

```
CREATE ENCRYPTKEY key_name
    AS "key_string"
```

### Parameters

> `key_name`: The name of the key to be created, which can include the name of the database. For example: `db1.my_key`.
>
> `key_string`: The string to create the key

This statement creates a custom key. Executing this command requires the user to have the `ADMIN` privileges.

If the database name is included in `key_name`, then this custom key will be created in the corresponding database, otherwise this function will be created in the database where the current session is located. The name of the new key cannot be the same as the key that already exists in the corresponding database, otherwise the creation will fail.

## Example

1. Create a custom key

    ```
    CREATE ENCRYPTKEY my_key as "ABCD123456789";
    ```

2. Using a custom key

    To use a custom key, add the keyword `KEY`/`key` before the key name, separated from `key_name` by a space.
    
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
