// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "aes_util.h"

#include <openssl/aes.h>
#include <openssl/err.h>
#include <openssl/evp.h>

#include <cstring>
#include <iostream>
#include <memory>
#include <string>

namespace doris {

static const int AES_MAX_KEY_LENGTH = 256;

const EVP_CIPHER* get_evp_type(const AesMode mode) {
    switch (mode) {
    case AES_128_ECB:
        return EVP_aes_128_ecb();
    case AES_128_CBC:
        return EVP_aes_128_cbc();
    case AES_192_ECB:
        return EVP_aes_192_ecb();
    case AES_192_CBC:
        return EVP_aes_192_cbc();
    case AES_256_ECB:
        return EVP_aes_256_ecb();
    case AES_256_CBC:
        return EVP_aes_256_cbc();
    default:
        return NULL;
    }
}

static uint aes_mode_key_sizes[] = {
        128 /* AES_128_ECB */, 192 /* AES_192_ECB */, 256 /* AES_256_ECB */,
        128 /* AES_128_CBC */, 192 /* AES_192_CBC */, 256 /* AES_256_CBC */
};

static void aes_create_key(const unsigned char* origin_key, uint32_t key_length,
                           uint8_t* encrypt_key, AesMode mode) {
    const uint key_size = aes_mode_key_sizes[mode] / 8;
    uint8_t* origin_key_end = ((uint8_t*)origin_key) + key_length; /* origin key boundary*/

    uint8_t* encrypt_key_end; /* encrypt key boundary */
    encrypt_key_end = encrypt_key + key_size;

    std::memset(encrypt_key, 0, key_size); /* initialize key  */

    uint8_t* ptr;        /* Start of the encrypt key*/
    uint8_t* origin_ptr; /* Start of the origin key */
    for (ptr = encrypt_key, origin_ptr = (uint8_t*)origin_key; origin_ptr < origin_key_end;
         ptr++, origin_ptr++) {
        if (ptr == encrypt_key_end) {
            /* loop over origin key until we used all key */
            ptr = encrypt_key;
        }
        *ptr ^= *origin_ptr;
    }
}

static int do_encrypt(EVP_CIPHER_CTX* aes_ctx, const EVP_CIPHER* cipher,
                      const unsigned char* source, uint32_t source_length,
                      const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* encrypt, int* length_ptr) {
    int ret = EVP_EncryptInit(aes_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(aes_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_EncryptUpdate(aes_ctx, encrypt, &u_len, source, source_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_EncryptFinal(aes_ctx, encrypt + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int AesUtil::encrypt(AesMode mode, const unsigned char* source, uint32_t source_length,
                     const unsigned char* key, uint32_t key_length, const unsigned char* iv,
                     bool padding, unsigned char* encrypt) {
    EVP_CIPHER_CTX aes_ctx;
    const EVP_CIPHER* cipher = get_evp_type(mode);
    /* The encrypt key to be used for encryption */
    unsigned char encrypt_key[AES_MAX_KEY_LENGTH / 8];
    aes_create_key(key, key_length, encrypt_key, mode);

    if (cipher == nullptr || (EVP_CIPHER_iv_length(cipher) > 0 && !iv)) {
        return AES_BAD_DATA;
    }
    EVP_CIPHER_CTX_init(&aes_ctx);
    int length = 0;
    int ret = do_encrypt(&aes_ctx, cipher, source, source_length, encrypt_key, iv, padding, encrypt,
                         &length);
    EVP_CIPHER_CTX_cleanup(&aes_ctx);
    if (ret == 0) {
        ERR_clear_error();
        return AES_BAD_DATA;
    } else {
        return length;
    }
}

static int do_decrypt(EVP_CIPHER_CTX* aes_ctx, const EVP_CIPHER* cipher,
                      const unsigned char* encrypt, uint32_t encrypt_length,
                      const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* decrypt_content, int* length_ptr) {
    int ret = EVP_DecryptInit(aes_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(aes_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_DecryptUpdate(aes_ctx, decrypt_content, &u_len, encrypt, encrypt_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_DecryptFinal_ex(aes_ctx, decrypt_content + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int AesUtil::decrypt(AesMode mode, const unsigned char* encrypt, uint32_t encrypt_length,
                     const unsigned char* key, uint32_t key_length, const unsigned char* iv,
                     bool padding, unsigned char* decrypt_content) {
    EVP_CIPHER_CTX aes_ctx;
    const EVP_CIPHER* cipher = get_evp_type(mode);

    /* The encrypt key to be used for decryption */
    unsigned char encrypt_key[AES_MAX_KEY_LENGTH / 8];
    aes_create_key(key, key_length, encrypt_key, mode);

    if (cipher == nullptr || (EVP_CIPHER_iv_length(cipher) > 0 && !iv)) {
        return AES_BAD_DATA;
    }

    EVP_CIPHER_CTX_init(&aes_ctx);
    int length = 0;
    int ret = do_decrypt(&aes_ctx, cipher, encrypt, encrypt_length, encrypt_key, iv, padding,
                         decrypt_content, &length);
    EVP_CIPHER_CTX_cleanup(&aes_ctx);
    if (ret > 0) {
        return length;
    } else {
        ERR_clear_error();
        return AES_BAD_DATA;
    }
}

} // namespace doris
