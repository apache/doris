#ifndef MY_AES_INCLUDED
#define MY_AES_INCLUDED

/* Copyright (c) 2000, 2016, Oracle and/or its affiliates. All rights reserved.

 This program is free software; you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation; version 2 of the License.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public License for more details.

 You should have received a copy of the GNU General Public License
 along with this program; if not, write to the Free Software
 Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/* Header file for my_aes.c */
/* Wrapper to give simple interface for MySQL to AES standard encryption */

//C_MODE_START
#include <stdint.h>

/** AES IV size is 16 bytes for all supported ciphers except ECB */
#define MY_AES_IV_SIZE 16

/** AES block size is fixed to be 128 bits for CBC and ECB */
#define MY_AES_BLOCK_SIZE 16
typedef uint32_t uint32;
typedef bool my_bool;
typedef uint32_t uint;


/** Supported AES cipher/block mode combos */
enum my_aes_opmode
{
   my_aes_128_ecb,
   my_aes_192_ecb,
   my_aes_256_ecb,
   my_aes_128_cbc,
   my_aes_192_cbc,
   my_aes_256_cbc
#ifndef HAVE_YASSL
   ,my_aes_128_cfb1,
   my_aes_192_cfb1,
   my_aes_256_cfb1,
   my_aes_128_cfb8,
   my_aes_192_cfb8,
   my_aes_256_cfb8,
   my_aes_128_cfb128,
   my_aes_192_cfb128,
   my_aes_256_cfb128,
   my_aes_128_ofb,
   my_aes_192_ofb,
   my_aes_256_ofb
#endif
};

#define MY_AES_BEGIN my_aes_128_ecb
#ifdef HAVE_YASSL
#define MY_AES_END my_aes_256_cbc
#else
#define MY_AES_END my_aes_256_ofb
#endif

/* If bad data discovered during decoding */
#define MY_AES_BAD_DATA  -1

/** String representations of the supported AES modes. Keep in sync with my_aes_opmode */
extern const char *my_aes_opmode_names[];
namespace palo {
/**
  Encrypt a buffer using AES

  @param source         [in]  Pointer to data for encryption
  @param source_length  [in]  Size of encryption data
  @param dest           [out] Buffer to place encrypted data (must be large enough)
  @param key            [in]  Key to be used for encryption
  @param key_length     [in]  Length of the key. Will handle keys of any length
  @param mode           [in]  encryption mode
  @param iv             [in]  16 bytes initialization vector if needed. Otherwise NULL
  @param padding        [in]  if padding needed.
  @return              size of encrypted data, or negative in case of error
*/

int my_aes_encrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest,
		   const unsigned char *key, uint32 key_length,
                   enum my_aes_opmode mode, const unsigned char *iv,
                   bool padding = true);

/**
  Decrypt an AES encrypted buffer

  @param source         Pointer to data for decryption
  @param source_length  size of encrypted data
  @param dest           buffer to place decrypted data (must be large enough)
  @param key            Key to be used for decryption
  @param key_length     Length of the key. Will handle keys of any length
  @param mode           encryption mode
  @param iv             16 bytes initialization vector if needed. Otherwise NULL
  @param padding        if padding needed.
  @return size of original data.
*/


int my_aes_decrypt(const unsigned char *source, uint32 source_length,
                   unsigned char *dest,
                   const unsigned char *key, uint32 key_length,
                   enum my_aes_opmode mode, const unsigned char *iv,
                   bool padding = true);

/**
  Calculate the size of a buffer large enough for encrypted data

  @param source_length  length of data to be encrypted
  @param mode           encryption mode
  @return               size of buffer required to store encrypted data
*/

int my_aes_get_size(uint32 source_length, enum my_aes_opmode mode);

/**
  Return true if the AES cipher and block mode requires an IV

  SYNOPSIS
  my_aes_needs_iv()
  @param mode           encryption mode

  @retval TRUE   IV needed
  @retval FALSE  IV not needed
*/

my_bool my_aes_needs_iv(my_aes_opmode opmode);
}
//C_MODE_END

#endif /* MY_AES_INCLUDED */
