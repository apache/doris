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

#include "util/encryption_util.h"

#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <sys/types.h>

#include <cstring>
#include <string>

namespace doris {

static const int ENCRYPTION_MAX_KEY_LENGTH = 256;

const EVP_CIPHER* get_evp_type(const EncryptionMode mode) {
    switch (mode) {
    case AES_128_ECB:
        return EVP_aes_128_ecb();
    case AES_128_CBC:
        return EVP_aes_128_cbc();
    case AES_128_CFB:
        return EVP_aes_128_cfb();
    case AES_128_CFB1:
        return EVP_aes_128_cfb1();
    case AES_128_CFB8:
        return EVP_aes_128_cfb8();
    case AES_128_CFB128:
        return EVP_aes_128_cfb128();
    case AES_128_CTR:
        return EVP_aes_128_ctr();
    case AES_128_OFB:
        return EVP_aes_128_ofb();
    case AES_192_ECB:
        return EVP_aes_192_ecb();
    case AES_192_CBC:
        return EVP_aes_192_cbc();
    case AES_192_CFB:
        return EVP_aes_192_cfb();
    case AES_192_CFB1:
        return EVP_aes_192_cfb1();
    case AES_192_CFB8:
        return EVP_aes_192_cfb8();
    case AES_192_CFB128:
        return EVP_aes_192_cfb128();
    case AES_192_CTR:
        return EVP_aes_192_ctr();
    case AES_192_OFB:
        return EVP_aes_192_ofb();
    case AES_256_ECB:
        return EVP_aes_256_ecb();
    case AES_256_CBC:
        return EVP_aes_256_cbc();
    case AES_256_CFB:
        return EVP_aes_256_cfb();
    case AES_256_CFB1:
        return EVP_aes_256_cfb1();
    case AES_256_CFB8:
        return EVP_aes_256_cfb8();
    case AES_256_CFB128:
        return EVP_aes_256_cfb128();
    case AES_256_CTR:
        return EVP_aes_256_ctr();
    case AES_256_OFB:
        return EVP_aes_256_ofb();
    case SM4_128_CBC:
        return EVP_sm4_cbc();
    case SM4_128_ECB:
        return EVP_sm4_ecb();
    case SM4_128_CFB128:
        return EVP_sm4_cfb128();
    case SM4_128_OFB:
        return EVP_sm4_ofb();
    case SM4_128_CTR:
        return EVP_sm4_ctr();
    default:
        return nullptr;
    }
}

static uint mode_key_sizes[] = {
        128 /* AES_128_ECB */,
        192 /* AES_192_ECB */,
        256 /* AES_256_ECB */,
        128 /* AES_128_CBC */,
        192 /* AES_192_CBC */,
        256 /* AES_256_CBC */,
        128 /* AES_128_CFB */,
        192 /* AES_192_CFB */,
        256 /* AES_256_CFB */,
        128 /* AES_128_CFB1 */,
        192 /* AES_192_CFB1 */,
        256 /* AES_256_CFB1 */,
        128 /* AES_128_CFB8 */,
        192 /* AES_192_CFB8 */,
        256 /* AES_256_CFB8 */,
        128 /* AES_128_CFB128 */,
        192 /* AES_192_CFB128 */,
        256 /* AES_256_CFB128 */,
        128 /* AES_128_CTR */,
        192 /* AES_192_CTR */,
        256 /* AES_256_CTR */,
        128 /* AES_128_OFB */,
        192 /* AES_192_OFB */,
        256 /* AES_256_OFB */,
        128 /* SM4_128_ECB */,
        128 /* SM4_128_CBC */,
        128 /* SM4_128_CFB128 */,
        128 /* SM4_128_OFB */,
        128 /* SM4_128_CTR */
};

static void create_key(const unsigned char* origin_key, uint32_t key_length, uint8_t* encrypt_key,
                       EncryptionMode mode) {
    const uint key_size = mode_key_sizes[mode] / 8;
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

static int do_encrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher,
                      const unsigned char* source, uint32_t source_length,
                      const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* encrypt, int* length_ptr) {
    int ret = EVP_EncryptInit(cipher_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(cipher_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_EncryptUpdate(cipher_ctx, encrypt, &u_len, source, source_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_EncryptFinal(cipher_ctx, encrypt + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int EncryptionUtil::encrypt(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv_str, bool padding, unsigned char* encrypt) {
    const EVP_CIPHER* cipher = get_evp_type(mode);
    /* The encrypt key to be used for encryption */
    unsigned char encrypt_key[ENCRYPTION_MAX_KEY_LENGTH / 8];
    create_key(key, key_length, encrypt_key, mode);

    int iv_length = EVP_CIPHER_iv_length(cipher);
    if (cipher == nullptr || (iv_length > 0 && !iv_str)) {
        return AES_BAD_DATA;
    }
    char* init_vec = nullptr;
    std::string iv_default("DORISDORISDORIS_");

    if (iv_str) {
        init_vec = &iv_default[0];
        memcpy(init_vec, iv_str, strnlen(iv_str, EVP_MAX_IV_LENGTH));
        init_vec[iv_length] = '\0';
    }
    EVP_CIPHER_CTX* cipher_ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_reset(cipher_ctx);
    int length = 0;
    int ret = do_encrypt(cipher_ctx, cipher, source, source_length, encrypt_key,
                         reinterpret_cast<unsigned char*>(init_vec), padding, encrypt, &length);
    EVP_CIPHER_CTX_free(cipher_ctx);
    if (ret == 0) {
        ERR_clear_error();
        return AES_BAD_DATA;
    } else {
        return length;
    }
}

static int do_decrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher,
                      const unsigned char* encrypt, uint32_t encrypt_length,
                      const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* decrypt_content, int* length_ptr) {
    int ret = EVP_DecryptInit(cipher_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(cipher_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_DecryptUpdate(cipher_ctx, decrypt_content, &u_len, encrypt, encrypt_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_DecryptFinal_ex(cipher_ctx, decrypt_content + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int EncryptionUtil::decrypt(EncryptionMode mode, const unsigned char* encrypt,
                            uint32_t encrypt_length, const unsigned char* key, uint32_t key_length,
                            const char* iv_str, bool padding, unsigned char* decrypt_content) {
    const EVP_CIPHER* cipher = get_evp_type(mode);

    /* The encrypt key to be used for decryption */
    unsigned char encrypt_key[ENCRYPTION_MAX_KEY_LENGTH / 8];
    create_key(key, key_length, encrypt_key, mode);

    int iv_length = EVP_CIPHER_iv_length(cipher);
    if (cipher == nullptr || (iv_length > 0 && !iv_str)) {
        return AES_BAD_DATA;
    }
    char* init_vec = nullptr;
    std::string iv_default("DORISDORISDORIS_");

    if (iv_str) {
        init_vec = &iv_default[0];
        memcpy(init_vec, iv_str, strnlen(iv_str, EVP_MAX_IV_LENGTH));
        init_vec[iv_length] = '\0';
    }
    EVP_CIPHER_CTX* cipher_ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_reset(cipher_ctx);
    int length = 0;
    int ret = do_decrypt(cipher_ctx, cipher, encrypt, encrypt_length, encrypt_key,
                         reinterpret_cast<unsigned char*>(init_vec), padding, decrypt_content,
                         &length);
    EVP_CIPHER_CTX_free(cipher_ctx);
    if (ret > 0) {
        return length;
    } else {
        ERR_clear_error();
        return AES_BAD_DATA;
    }
}

} // namespace doris
