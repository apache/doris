
// Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; version 2 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

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
namespace doris {
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
