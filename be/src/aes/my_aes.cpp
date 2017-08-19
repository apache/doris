// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "my_aes.h"
#include "my_aes_impl.h"
#include <cstring>

/**
  Transforms an arbitrary long key into a fixed length AES key

  AES keys are of fixed length. This routine takes an arbitrary long key
  iterates over it in AES key length increment and XORs the bytes with the
  AES key buffer being prepared.
  The bytes from the last incomplete iteration are XORed to the start
  of the key until their depletion.
  Needed since crypto function routines expect a fixed length key.

  @param key        [in]       Key to use for real key creation
  @param key_length [in]       Length of the key
  @param rkey       [out]      Real key (used by OpenSSL/YaSSL)
  @param opmode     [out]      encryption mode
*/
namespace palo {
void my_aes_create_key(const unsigned char *key, uint key_length,
                       uint8 *rkey, enum my_aes_opmode opmode)
{
  const uint key_size= my_aes_opmode_key_sizes[opmode] / 8;
  uint8 *rkey_end;                              /* Real key boundary */
  uint8 *ptr;                                   /* Start of the real key*/
  uint8 *sptr;                                  /* Start of the working key */
  uint8 *key_end= ((uint8 *)key) + key_length;  /* Working key boundary*/

  rkey_end= rkey + key_size;

  memset(rkey, 0, key_size);          /* Set initial key  */

  for (ptr= rkey, sptr= (uint8 *)key; sptr < key_end; ptr++, sptr++)
  {
    if (ptr == rkey_end)
      /*  Just loop over tmp_key until we used all key */
      ptr= rkey;
    *ptr^= *sptr;
  }
}
}
