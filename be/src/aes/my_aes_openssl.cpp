//  Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.
// 
//  This program is free software; you can redistribute it and/or modify
//  it under the terms of the GNU General Public License as published by
//  the Free Software Foundation; version 2 of the License.
// 
//  This program is distributed in the hope that it will be useful,
//  but WITHOUT ANY WARRANTY; without even the implied warranty of
//  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//  GNU General Public License for more details.
// 
//  You should have received a copy of the GNU General Public License
//  along with this program; if not, write to the Free Software
//  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA 

#include "my_aes.h"
#include "my_aes_impl.h"
#include <string>
#include <assert.h>

#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/err.h>

#define DBUG_ASSERT(A) assert(A)
#define TRUE true
#define FALSE false
namespace palo {
/* keep in sync with enum my_aes_opmode in my_aes.h */
const char *my_aes_opmode_names[]=
{
  "aes-128-ecb",
  "aes-192-ecb",
  "aes-256-ecb",
  "aes-128-cbc",
  "aes-192-cbc",
  "aes-256-cbc",
  "aes-128-cfb1",
  "aes-192-cfb1",
  "aes-256-cfb1",
  "aes-128-cfb8",
  "aes-192-cfb8",
  "aes-256-cfb8",
  "aes-128-cfb128",
  "aes-192-cfb128",
  "aes-256-cfb128",
  "aes-128-ofb",
  "aes-192-ofb",
  "aes-256-ofb",
  NULL /* needed for the type enumeration */
};


/* keep in sync with enum my_aes_opmode in my_aes.h */
static uint my_aes_opmode_key_sizes_impl[]=
{
  128 /* aes-128-ecb */,
  192 /* aes-192-ecb */,
  256 /* aes-256-ecb */,
  128 /* aes-128-cbc */,
  192 /* aes-192-cbc */,
  256 /* aes-256-cbc */,
  128 /* aes-128-cfb1 */,
  192 /* aes-192-cfb1 */,
  256 /* aes-256-cfb1 */,
  128 /* aes-128-cfb8 */,
  192 /* aes-192-cfb8 */,
  256 /* aes-256-cfb8 */,
  128 /* aes-128-cfb128 */,
  192 /* aes-192-cfb128 */,
  256 /* aes-256-cfb128 */,
  128 /* aes-128-ofb */,
  192 /* aes-192-ofb */,
  256 /* aes-256-ofb */
};

uint *my_aes_opmode_key_sizes= my_aes_opmode_key_sizes_impl;



static const EVP_CIPHER *
aes_evp_type(const my_aes_opmode mode)
{
  switch (mode)
  {
  case my_aes_128_ecb:    return EVP_aes_128_ecb();
  case my_aes_128_cbc:    return EVP_aes_128_cbc();
  case my_aes_128_cfb1:   return EVP_aes_128_cfb1();
  case my_aes_128_cfb8:   return EVP_aes_128_cfb8();
  case my_aes_128_cfb128: return EVP_aes_128_cfb128();
  case my_aes_128_ofb:    return EVP_aes_128_ofb();
  case my_aes_192_ecb:    return EVP_aes_192_ecb();
  case my_aes_192_cbc:    return EVP_aes_192_cbc();
  case my_aes_192_cfb1:   return EVP_aes_192_cfb1();
  case my_aes_192_cfb8:   return EVP_aes_192_cfb8();
  case my_aes_192_cfb128: return EVP_aes_192_cfb128();
  case my_aes_192_ofb:    return EVP_aes_192_ofb();
  case my_aes_256_ecb:    return EVP_aes_256_ecb();
  case my_aes_256_cbc:    return EVP_aes_256_cbc();
  case my_aes_256_cfb1:   return EVP_aes_256_cfb1();
  case my_aes_256_cfb8:   return EVP_aes_256_cfb8();
  case my_aes_256_cfb128: return EVP_aes_256_cfb128();
  case my_aes_256_ofb:    return EVP_aes_256_ofb();
  default: return NULL;
  }
}


int my_aes_encrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest,
                   const unsigned char *key, uint32 key_length,
                   enum my_aes_opmode mode, const unsigned char *iv,
                   bool padding)
{
  EVP_CIPHER_CTX ctx;
  const EVP_CIPHER *cipher= aes_evp_type(mode);
  int u_len, f_len;
  /* The real key to be used for encryption */
  unsigned char rkey[MAX_AES_KEY_LENGTH / 8];
  my_aes_create_key(key, key_length, rkey, mode);

  if (!cipher || (EVP_CIPHER_iv_length(cipher) > 0 && !iv))
    return MY_AES_BAD_DATA;

  if (!EVP_EncryptInit(&ctx, cipher, rkey, iv))
    goto aes_error;                             /* Error */
  if (!EVP_CIPHER_CTX_set_padding(&ctx, padding))
    goto aes_error;                             /* Error */
  if (!EVP_EncryptUpdate(&ctx, dest, &u_len, source, source_length))
    goto aes_error;                             /* Error */

  if (!EVP_EncryptFinal(&ctx, dest + u_len, &f_len))
    goto aes_error;                             /* Error */

  EVP_CIPHER_CTX_cleanup(&ctx);
  return u_len + f_len;

aes_error:
  /* need to explicitly clean up the error if we want to ignore it */
  ERR_clear_error();
  EVP_CIPHER_CTX_cleanup(&ctx);
  return MY_AES_BAD_DATA;
}

int my_aes_decrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest,
                   const unsigned char *key, uint32 key_length,
                   enum my_aes_opmode mode, const unsigned char *iv,
                   bool padding)
{

  EVP_CIPHER_CTX ctx;
  const EVP_CIPHER *cipher= aes_evp_type(mode);
  int u_len, f_len;

  /* The real key to be used for decryption */
  unsigned char rkey[MAX_AES_KEY_LENGTH / 8];

  my_aes_create_key(key, key_length, rkey, mode);
  if (!cipher || (EVP_CIPHER_iv_length(cipher) > 0 && !iv))
    return MY_AES_BAD_DATA;

  EVP_CIPHER_CTX_init(&ctx);

  if (!EVP_DecryptInit(&ctx, aes_evp_type(mode), rkey, iv))
    goto aes_error;                             /* Error */
  if (!EVP_CIPHER_CTX_set_padding(&ctx, padding))
    goto aes_error;                             /* Error */
  if (!EVP_DecryptUpdate(&ctx, dest, &u_len, source, source_length))
    goto aes_error;                             /* Error */
  if (!EVP_DecryptFinal_ex(&ctx, dest + u_len, &f_len))
    goto aes_error;                             /* Error */

  EVP_CIPHER_CTX_cleanup(&ctx);
  return u_len + f_len;

aes_error:
  /* need to explicitly clean up the error if we want to ignore it */
  ERR_clear_error();
  EVP_CIPHER_CTX_cleanup(&ctx);
  return MY_AES_BAD_DATA;
}

int my_aes_get_size(uint32 source_length, my_aes_opmode opmode)
{
  const EVP_CIPHER *cipher= aes_evp_type(opmode);
  size_t block_size;

  block_size= EVP_CIPHER_block_size(cipher);

  return block_size > 1 ?
    block_size * (source_length / block_size) + block_size :
    source_length;
}

/**
  Return true if the AES cipher and block mode requires an IV

  SYNOPSIS
  my_aes_needs_iv()
  @param mode           encryption mode

  @retval TRUE   IV needed
  @retval FALSE  IV not needed
*/

my_bool my_aes_needs_iv(my_aes_opmode opmode)
{
  const EVP_CIPHER *cipher= aes_evp_type(opmode);
  int iv_length;

  iv_length= EVP_CIPHER_iv_length(cipher);
  DBUG_ASSERT(iv_length == 0 || iv_length == MY_AES_IV_SIZE);
  return iv_length != 0 ? TRUE : FALSE;
}
}
