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

#include <algorithm>
#include <cstring>
#include <string>
#include <unordered_map>

namespace doris {

static const int ENCRYPTION_MAX_KEY_LENGTH = 256;

const EVP_CIPHER* get_evp_type(const EncryptionMode mode) {
    switch (mode) {
    case EncryptionMode::AES_128_ECB:
        return EVP_aes_128_ecb();
    case EncryptionMode::AES_128_CBC:
        return EVP_aes_128_cbc();
    case EncryptionMode::AES_128_CFB:
        return EVP_aes_128_cfb();
    case EncryptionMode::AES_128_CFB1:
        return EVP_aes_128_cfb1();
    case EncryptionMode::AES_128_CFB8:
        return EVP_aes_128_cfb8();
    case EncryptionMode::AES_128_CFB128:
        return EVP_aes_128_cfb128();
    case EncryptionMode::AES_128_CTR:
        return EVP_aes_128_ctr();
    case EncryptionMode::AES_128_OFB:
        return EVP_aes_128_ofb();
    case EncryptionMode::AES_192_ECB:
        return EVP_aes_192_ecb();
    case EncryptionMode::AES_192_CBC:
        return EVP_aes_192_cbc();
    case EncryptionMode::AES_192_CFB:
        return EVP_aes_192_cfb();
    case EncryptionMode::AES_192_CFB1:
        return EVP_aes_192_cfb1();
    case EncryptionMode::AES_192_CFB8:
        return EVP_aes_192_cfb8();
    case EncryptionMode::AES_192_CFB128:
        return EVP_aes_192_cfb128();
    case EncryptionMode::AES_192_CTR:
        return EVP_aes_192_ctr();
    case EncryptionMode::AES_192_OFB:
        return EVP_aes_192_ofb();
    case EncryptionMode::AES_256_ECB:
        return EVP_aes_256_ecb();
    case EncryptionMode::AES_256_CBC:
        return EVP_aes_256_cbc();
    case EncryptionMode::AES_256_CFB:
        return EVP_aes_256_cfb();
    case EncryptionMode::AES_256_CFB1:
        return EVP_aes_256_cfb1();
    case EncryptionMode::AES_256_CFB8:
        return EVP_aes_256_cfb8();
    case EncryptionMode::AES_256_CFB128:
        return EVP_aes_256_cfb128();
    case EncryptionMode::AES_256_CTR:
        return EVP_aes_256_ctr();
    case EncryptionMode::AES_256_OFB:
        return EVP_aes_256_ofb();
    case EncryptionMode::AES_128_GCM:
        return EVP_aes_128_gcm();
    case EncryptionMode::AES_192_GCM:
        return EVP_aes_192_gcm();
    case EncryptionMode::AES_256_GCM:
        return EVP_aes_256_gcm();
    case EncryptionMode::SM4_128_CBC:
        return EVP_sm4_cbc();
    case EncryptionMode::SM4_128_ECB:
        return EVP_sm4_ecb();
    case EncryptionMode::SM4_128_CFB128:
        return EVP_sm4_cfb128();
    case EncryptionMode::SM4_128_OFB:
        return EVP_sm4_ofb();
    case EncryptionMode::SM4_128_CTR:
        return EVP_sm4_ctr();
    default:
        return nullptr;
    }
}

static std::unordered_map<EncryptionMode, uint> mode_key_sizes = {
        {EncryptionMode::AES_128_ECB, 128},    {EncryptionMode::AES_192_ECB, 192},
        {EncryptionMode::AES_256_ECB, 256},    {EncryptionMode::AES_128_CBC, 128},
        {EncryptionMode::AES_192_CBC, 192},    {EncryptionMode::AES_256_CBC, 256},
        {EncryptionMode::AES_128_CFB, 128},    {EncryptionMode::AES_192_CFB, 192},
        {EncryptionMode::AES_256_CFB, 256},    {EncryptionMode::AES_128_CFB1, 128},
        {EncryptionMode::AES_192_CFB1, 192},   {EncryptionMode::AES_256_CFB1, 256},
        {EncryptionMode::AES_128_CFB8, 128},   {EncryptionMode::AES_192_CFB8, 192},
        {EncryptionMode::AES_256_CFB8, 256},   {EncryptionMode::AES_128_CFB128, 128},
        {EncryptionMode::AES_192_CFB128, 192}, {EncryptionMode::AES_256_CFB128, 256},
        {EncryptionMode::AES_128_CTR, 128},    {EncryptionMode::AES_192_CTR, 192},
        {EncryptionMode::AES_256_CTR, 256},    {EncryptionMode::AES_128_OFB, 128},
        {EncryptionMode::AES_192_OFB, 192},    {EncryptionMode::AES_256_OFB, 256},
        {EncryptionMode::AES_128_GCM, 128},    {EncryptionMode::AES_192_GCM, 192},
        {EncryptionMode::AES_256_GCM, 256},

        {EncryptionMode::SM4_128_ECB, 128},    {EncryptionMode::SM4_128_CBC, 128},
        {EncryptionMode::SM4_128_CFB128, 128}, {EncryptionMode::SM4_128_OFB, 128},
        {EncryptionMode::SM4_128_CTR, 128}};

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

static int do_gcm_encrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher,
                          const unsigned char* source, uint32_t source_length,
                          const unsigned char* encrypt_key, const unsigned char* iv, int iv_length,
                          unsigned char* encrypt, int* length_ptr, const unsigned char* aad,
                          uint32_t aad_length) {
    int ret = EVP_EncryptInit_ex(cipher_ctx, cipher, nullptr, nullptr, nullptr);
    if (ret != 1) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN, iv_length, nullptr);
    if (ret != 1) {
        return ret;
    }
    ret = EVP_EncryptInit_ex(cipher_ctx, nullptr, nullptr, encrypt_key, iv);
    if (ret != 1) {
        return ret;
    }
    if (aad) {
        int tmp_len = 0;
        ret = EVP_EncryptUpdate(cipher_ctx, nullptr, &tmp_len, aad, aad_length);
        if (ret != 1) {
            return ret;
        }
    }

    std::memcpy(encrypt, iv, iv_length);
    encrypt += iv_length;

    int u_len = 0;
    ret = EVP_EncryptUpdate(cipher_ctx, encrypt, &u_len, source, source_length);
    if (ret != 1) {
        return ret;
    }
    encrypt += u_len;

    int f_len = 0;
    ret = EVP_EncryptFinal_ex(cipher_ctx, encrypt, &f_len);
    if (ret != 1) {
        return ret;
    }
    encrypt += f_len;

    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_GET_TAG, EncryptionUtil::GCM_TAG_SIZE,
                              encrypt);
    *length_ptr = iv_length + u_len + f_len + EncryptionUtil::GCM_TAG_SIZE;
    return ret;
}

int EncryptionUtil::encrypt(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv_str, int iv_input_length, bool padding,
                            unsigned char* encrypt, const unsigned char* aad, uint32_t aad_length) {
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
        memcpy(init_vec, iv_str, std::min(iv_input_length, EVP_MAX_IV_LENGTH));
        init_vec[iv_length] = '\0';
    }
    EVP_CIPHER_CTX* cipher_ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_reset(cipher_ctx);
    int length = 0;
    int ret = 0;
    if (is_gcm_mode(mode)) {
        ret = do_gcm_encrypt(cipher_ctx, cipher, source, source_length, encrypt_key,
                             reinterpret_cast<unsigned char*>(init_vec), iv_length, encrypt,
                             &length, aad, aad_length);
    } else {
        ret = do_encrypt(cipher_ctx, cipher, source, source_length, encrypt_key,
                         reinterpret_cast<unsigned char*>(init_vec), padding, encrypt, &length);
    }

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

static int do_gcm_decrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher,
                          const unsigned char* encrypt, uint32_t encrypt_length,
                          const unsigned char* encrypt_key, int iv_length,
                          unsigned char* decrypt_content, int* length_ptr, const unsigned char* aad,
                          uint32_t aad_length) {
    if (encrypt_length < iv_length + EncryptionUtil::GCM_TAG_SIZE) {
        return -1;
    }
    int ret = EVP_DecryptInit_ex(cipher_ctx, cipher, nullptr, nullptr, nullptr);
    if (ret != 1) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_IVLEN, iv_length, nullptr);
    if (ret != 1) {
        return ret;
    }
    ret = EVP_DecryptInit_ex(cipher_ctx, nullptr, nullptr, encrypt_key, encrypt);
    if (ret != 1) {
        return ret;
    }
    encrypt += iv_length;
    if (aad) {
        int tmp_len = 0;
        ret = EVP_DecryptUpdate(cipher_ctx, nullptr, &tmp_len, aad, aad_length);
        if (ret != 1) {
            return ret;
        }
    }

    uint32_t real_encrypt_length = encrypt_length - iv_length - EncryptionUtil::GCM_TAG_SIZE;
    int u_len = 0;
    ret = EVP_DecryptUpdate(cipher_ctx, decrypt_content, &u_len, encrypt, real_encrypt_length);
    if (ret != 1) {
        return ret;
    }
    encrypt += real_encrypt_length;
    decrypt_content += u_len;

    void* tag = const_cast<void*>(reinterpret_cast<const void*>(encrypt));
    ret = EVP_CIPHER_CTX_ctrl(cipher_ctx, EVP_CTRL_GCM_SET_TAG, EncryptionUtil::GCM_TAG_SIZE, tag);
    if (ret != 1) {
        return ret;
    }

    int f_len = 0;
    ret = EVP_DecryptFinal_ex(cipher_ctx, decrypt_content, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int EncryptionUtil::decrypt(EncryptionMode mode, const unsigned char* encrypt,
                            uint32_t encrypt_length, const unsigned char* key, uint32_t key_length,
                            const char* iv_str, int iv_input_length, bool padding,
                            unsigned char* decrypt_content, const unsigned char* aad,
                            uint32_t aad_length) {
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
        memcpy(init_vec, iv_str, std::min(iv_input_length, EVP_MAX_IV_LENGTH));
        init_vec[iv_length] = '\0';
    }
    EVP_CIPHER_CTX* cipher_ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_reset(cipher_ctx);
    int length = 0;
    int ret = 0;
    if (is_gcm_mode(mode)) {
        ret = do_gcm_decrypt(cipher_ctx, cipher, encrypt, encrypt_length, encrypt_key, iv_length,
                             decrypt_content, &length, aad, aad_length);
    } else {
        ret = do_decrypt(cipher_ctx, cipher, encrypt, encrypt_length, encrypt_key,
                         reinterpret_cast<unsigned char*>(init_vec), padding, decrypt_content,
                         &length);
    }
    EVP_CIPHER_CTX_free(cipher_ctx);
    if (ret > 0) {
        return length;
    } else {
        ERR_clear_error();
        return AES_BAD_DATA;
    }
}

} // namespace doris
