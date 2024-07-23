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

#include "common/encryption_util.h"

#include <gen_cpp/cloud.pb.h>
#include <glog/logging.h>
#include <math.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <sys/types.h>

#include <cstring>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "common/config.h"
#include "common/kms.h"
#include "common/logging.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

namespace doris::cloud {
namespace config {
extern std::string encryption_key;
}; // namespace config

enum class EncryptionMode {
    AES_128_ECB,
    AES_256_ECB,
    AES_128_CBC,
    AES_256_CBC,
    AES_128_CFB,
    AES_256_CFB,
    AES_128_CFB1,
    AES_256_CFB1,
    AES_128_CFB8,
    AES_256_CFB8,
    AES_128_CFB128,
    AES_256_CFB128,
    AES_128_CTR,
    AES_256_CTR,
    AES_128_OFB,
    AES_256_OFB,
};

enum EncryptionState { AES_SUCCESS = 0, AES_BAD_DATA = -1 };

class EncryptionUtil {
public:
    static int encrypt(EncryptionMode mode, const unsigned char* source, uint32_t source_length,
                       const unsigned char* key, uint32_t key_length, const char* iv_str,
                       int iv_input_length, bool padding, unsigned char* encrypt);

    static int decrypt(EncryptionMode mode, const unsigned char* encrypt, uint32_t encrypt_length,
                       const unsigned char* key, uint32_t key_length, const char* iv_str,
                       int iv_input_length, bool padding, unsigned char* decrypt_content);
};

// aes encrypt/dencrypt
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
    default:
        return nullptr;
    }
}

static uint mode_key_sizes[] = {
        128 /* AES_128_ECB */,  256 /* AES_256_ECB */,    128 /* AES_128_CBC */,
        256 /* AES_256_CBC */,  128 /* AES_128_CFB */,    256 /* AES_256_CFB */,
        128 /* AES_128_CFB1 */, 256 /* AES_256_CFB1 */,   128 /* AES_128_CFB8 */,
        256 /* AES_256_CFB8 */, 128 /* AES_128_CFB128 */, 256 /* AES_256_CFB128 */,
        128 /* AES_128_CTR */,  256 /* AES_256_CTR */,    128 /* AES_128_OFB */,
        256 /* AES_256_OFB */,
};

static void create_key(const unsigned char* origin_key, uint32_t key_length, uint8_t* encrypt_key,
                       EncryptionMode mode) {
    const uint key_size = mode_key_sizes[int(mode)] / 8;
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
                            const char* iv_str, int iv_input_length, bool padding,
                            unsigned char* encrypt) {
    const EVP_CIPHER* cipher = get_evp_type(mode);
    /* The encrypt key to be used for encryption */
    unsigned char encrypt_key[ENCRYPTION_MAX_KEY_LENGTH / 8];
    create_key(key, key_length, encrypt_key, mode);

    int iv_length = EVP_CIPHER_iv_length(cipher);
    if (cipher == nullptr || (iv_length > 0 && !iv_str)) {
        return AES_BAD_DATA;
    }
    char* init_vec = nullptr;
    std::string iv_default("SELECTDBCLOUD___");

    if (iv_str) {
        init_vec = iv_default.data();
        memcpy(init_vec, iv_str, std::min(iv_input_length, EVP_MAX_IV_LENGTH));
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
                            const char* iv_str, int iv_input_length, bool padding,
                            unsigned char* decrypt_content) {
    const EVP_CIPHER* cipher = get_evp_type(mode);

    /* The encrypt key to be used for decryption */
    unsigned char encrypt_key[ENCRYPTION_MAX_KEY_LENGTH / 8];
    create_key(key, key_length, encrypt_key, mode);

    int iv_length = EVP_CIPHER_iv_length(cipher);
    if (cipher == nullptr || (iv_length > 0 && !iv_str)) {
        return AES_BAD_DATA;
    }
    char* init_vec = nullptr;
    std::string iv_default("SELECTDBCLOUD___");

    if (iv_str) {
        init_vec = iv_default.data();
        memcpy(init_vec, iv_str, std::min(iv_input_length, EVP_MAX_IV_LENGTH));
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

// base64 endcode/decode

static char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

static const char base64_pad = '=';

static short decoding_table[256] = {
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -1, -1, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 62,
        -2, -2, -2, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -2, -2, -2, -2, -2, -2, -2, 0,
        1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        23, 24, 25, -2, -2, -2, -2, -2, -2, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2};

static int mod_table[] = {0, 2, 1};

size_t base64_encode(const unsigned char* data, size_t length, unsigned char* encoded_data) {
    size_t output_length = (size_t)(4.0 * ceil((double)length / 3.0));

    if (encoded_data == nullptr) {
        return 0;
    }

    for (uint32_t i = 0, j = 0; i < length;) {
        uint32_t octet_a = i < length ? data[i++] : 0;
        uint32_t octet_b = i < length ? data[i++] : 0;
        uint32_t octet_c = i < length ? data[i++] : 0;
        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (int i = 0; i < mod_table[length % 3]; i++) {
        encoded_data[output_length - 1 - i] = '=';
    }

    return output_length;
}

size_t base64_decode(const char* data, size_t length, char* decoded_data) {
    const char* current = data;
    size_t ch = 0;
    size_t i = 0;
    size_t j = 0;
    size_t k = 0;

    // run through the whole string, converting as we go
    while ((ch = *current++) != '\0' && length-- > 0) {
        if (ch >= 256 || ch < 0) {
            return -1;
        }

        if (ch == base64_pad) {
            if (*current != '=' && (i % 4) == 1) {
                return -1;
            }
            continue;
        }

        ch = decoding_table[ch];
        // a space or some other separator character, we simply skip over
        if (ch == -1) {
            continue;
        } else if (ch == -2) {
            return -1;
        }

        switch (i % 4) {
        case 0:
            decoded_data[j] = ch << 2;
            break;
        case 1:
            decoded_data[j++] |= ch >> 4;
            decoded_data[j] = (ch & 0x0f) << 4;
            break;
        case 2:
            decoded_data[j++] |= ch >> 2;
            decoded_data[j] = (ch & 0x03) << 6;
            break;
        case 3:
            decoded_data[j++] |= ch;
            break;
        default:
            break;
        }

        i++;
    }

    k = j;
    /* mop things up if we ended on a boundary */
    if (ch == base64_pad) {
        switch (i % 4) {
        case 1:
            return 0;
        case 2:
            k++;
        case 3:
            decoded_data[k] = 0;
        default:
            break;
        }
    }

    decoded_data[j] = '\0';

    return j;
}

// encrypt/dencrypt with base64

static std::unordered_map<std::string, EncryptionMode> to_encryption_mode {
        {"AES_128_ECB", EncryptionMode::AES_128_ECB},
        {"AES_256_ECB", EncryptionMode::AES_256_ECB},
        {"AES_128_CBC", EncryptionMode::AES_128_CBC},
        {"AES_256_CBC", EncryptionMode::AES_256_CBC},
        {"AES_128_CFB", EncryptionMode::AES_128_CFB},
        {"AES_256_CFB", EncryptionMode::AES_256_CFB},
        {"AES_128_CFB1", EncryptionMode::AES_128_CFB1},
        {"AES_256_CFB1", EncryptionMode::AES_256_CFB1},
        {"AES_128_CFB8", EncryptionMode::AES_128_CFB8},
        {"AES_256_CFB8", EncryptionMode::AES_256_CFB8},
        {"AES_128_CFB128", EncryptionMode::AES_128_CFB128},
        {"AES_256_CFB128", EncryptionMode::AES_256_CFB128},
        {"AES_128_CTR", EncryptionMode::AES_128_CTR},
        {"AES_256_CTR", EncryptionMode::AES_256_CTR},
        {"AES_128_OFB", EncryptionMode::AES_128_OFB},
        {"AES_256_OFB", EncryptionMode::AES_256_OFB},
};

static inline int encrypt_to_base64_impl(std::string_view source, EncryptionMode mode,
                                         const std::string& key, std::string* encrypt) {
    /*
     * Buffer for ciphertext. Ensure the buffer is long enough for the
     * ciphertext which may be longer than the plaintext, depending on the
     * algorithm and mode.
     */
    int cipher_len = source.length() + 16;
    std::string cipher_text(cipher_len, '0');
    int cipher_text_len = EncryptionUtil::encrypt(
            mode, (unsigned char*)source.data(), source.length(), (unsigned char*)key.c_str(),
            key.length(), nullptr, 0, true, (unsigned char*)cipher_text.data());
    if (cipher_text_len < 0) {
        return -1;
    }

    int encoded_len = (size_t)(4.0 * ceil(cipher_text_len / 3.0));
    std::string encoded_text(encoded_len, '0');
    int encoded_text_len = base64_encode((unsigned char*)cipher_text.data(), cipher_text_len,
                                         (unsigned char*)encoded_text.data());
    if (encoded_text_len < 0) {
        return -1;
    }
    encrypt->assign((char*)encoded_text.data(), encoded_text_len);
    return 0;
}

static int encrypt_to_base64(std::string_view source, const std::string& encrypt_method,
                             const std::string& key, std::string* encrypt) {
    if (source.empty()) {
        *encrypt = "";
        return 0;
    }
    auto iter = to_encryption_mode.find(encrypt_method);
    if (iter == to_encryption_mode.end()) {
        return -1;
    }

    return encrypt_to_base64_impl(source, iter->second, key, encrypt);
}

static inline int decrypt_with_base64_impl(std::string_view encrypt, EncryptionMode mode,
                                           const std::string& key, std::string* source) {
    // base64
    std::unique_ptr<char[]> decoded_text(new char[encrypt.length()]);
    int decoded_text_len = base64_decode(encrypt.data(), encrypt.length(), decoded_text.get());
    if (decoded_text_len < 0) {
        return -1;
    }

    std::unique_ptr<char[]> plain_text(new char[decoded_text_len]);
    int plain_text_len = EncryptionUtil::decrypt(
            mode, (unsigned char*)decoded_text.get(), decoded_text_len, (unsigned char*)key.c_str(),
            key.length(), nullptr, 0, true, (unsigned char*)plain_text.get());
    if (plain_text_len < 0) {
        return -1;
    }
    source->assign(plain_text.get(), plain_text_len);
    return 0;
}

static int decrypt_with_base64(std::string_view encrypt, const std::string& encrypt_method,
                               const std::string& key, std::string* source) {
    if (encrypt.empty()) {
        *source = "";
        return 0;
    }
    auto iter = to_encryption_mode.find(encrypt_method);
    if (iter == to_encryption_mode.end()) {
        return -1;
    }
    return decrypt_with_base64_impl(encrypt, iter->second, key, source);
}

int encrypt_ak_sk(AkSkRef plain_ak_sk, const std::string& encryption_method,
                  const std::string& encryption_key, AkSkPair* cipher_ak_sk) {
    std::string encrypt_ak;
    std::string encrypt_sk;
    if (encrypt_to_base64(plain_ak_sk.second, encryption_method, encryption_key, &encrypt_sk) !=
        0) {
        *cipher_ak_sk = {"", ""};
        return -1;
    }
    *cipher_ak_sk = {std::string(plain_ak_sk.first), std::move(encrypt_sk)};
    return 0;
}

int decrypt_ak_sk(AkSkRef cipher_ak_sk, const std::string& encryption_method,
                  const std::string& encryption_key, AkSkPair* plain_ak_sk) {
    std::string ak;
    std::string sk;
    if (decrypt_with_base64(cipher_ak_sk.second, encryption_method, encryption_key, &sk) != 0) {
        *plain_ak_sk = {"", ""};
        return -1;
    }
    *plain_ak_sk = {std::string(cipher_ak_sk.first), std::move(sk)};
    return 0;
}

int decrypt_ak_sk_helper(std::string_view cipher_ak, std::string_view cipher_sk,
                         const EncryptionInfoPB& encryption_info, AkSkPair* plain_ak_sk_pair) {
    std::string key;
    int ret = get_encryption_key_for_ak_sk(encryption_info.key_id(), &key);
    { TEST_SYNC_POINT_CALLBACK("decrypt_ak_sk:get_encryption_key", &key, &ret); }
    if (ret != 0) {
        LOG(WARNING) << "failed to get encryption key version_id: " << encryption_info.key_id();
        return -1;
    }
    ret = decrypt_ak_sk({cipher_ak, cipher_sk}, encryption_info.encryption_method(), key,
                        plain_ak_sk_pair);
    if (ret != 0) {
        LOG(WARNING) << "failed to decrypt";
        return -1;
    }
    return 0;
}

/**
 * @brief Generates a random root key. If a root key already exists, returns immediately. 
 * 
 * @param txn_kv 
 * @param kms_client 
 * @param plaintext store the plaintext of the root key
 * @param encoded_ciphertext store the base64-encoded ciphertext of the root key.
 * @return int 0 for success to generate, 1 for not need to generate, -1 for failure.
 */
static int generate_random_root_key(TxnKv* txn_kv, KmsClient* kms_client, std::string* plaintext,
                                    std::string* encoded_ciphertext) {
    /**
    * 1. If KMS is enabled, use KMS to generate a new key.
    * 2. If KMS is not enabled, try using the encryption_key from the configuration, which must be in Base64 format.
    * 3. If no key is found in the configuration, generate a random key in memory.
    */
    std::string key = system_meta_service_encryption_key_info_key();
    std::string val;
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG_WARNING("failed to create txn").tag("err", err);
        return -1;
    }
    err = txn->get(key, &val);
    if (err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_WARNING("failed to get key of encryption_key_info").tag("err", err);
        return -1;
    }

    if (err == TxnErrorCode::TXN_OK) {
        if (config::enable_kms && config::focus_add_kms_data_key) {
            EncryptionKeyInfoPB key_info;
            if (!key_info.ParseFromString(val)) {
                LOG_WARNING("failed to parse encryption_root_key");
                return -1;
            }
            for (const auto& item : key_info.items()) {
                if (item.has_kms_info()) {
                    return 1;
                }
            }
            LOG(INFO) << "focus to create kms data key";
        } else {
            LOG(INFO) << "not need to generate root key";
            return 1;
        }
    }

    // 1. use kms to generate a new key
    if (config::enable_kms) {
        if (kms_client == nullptr) {
            LOG_WARNING("no kms client");
            return -1;
        }
        std::string encoded_root_key_ciphertext;
        std::string encoded_root_key_plaintext;
        if (kms_client->generate_data_key(&encoded_root_key_ciphertext,
                                          &encoded_root_key_plaintext) != 0) {
            LOG_WARNING("failed to generate data key");
            return -1;
        }
        if (encoded_root_key_ciphertext.empty() || encoded_root_key_plaintext.empty()) {
            LOG_WARNING("empty data key generated");
            return -1;
        }

        // decode plaintext
        std::string root_key_plaintext(encoded_root_key_plaintext.length(), '0');
        int decoded_len =
                base64_decode(encoded_root_key_plaintext.c_str(),
                              encoded_root_key_plaintext.length(), root_key_plaintext.data());
        if (decoded_len < 0) {
            LOG_WARNING("failed to decode plaintext of kms");
            return -1;
        }
        root_key_plaintext.assign(root_key_plaintext.data(), decoded_len);

        *plaintext = std::move(root_key_plaintext);
        *encoded_ciphertext = std::move(encoded_root_key_ciphertext);
        return 0;
    }

    // 2. try using the encryption_key from the configuration
    if (!cloud::config::encryption_key.empty()) {
        std::string decoded_string(cloud::config::encryption_key.length(), '0');
        int decoded_text_len =
                base64_decode(cloud::config::encryption_key.c_str(),
                              cloud::config::encryption_key.length(), decoded_string.data());
        if (decoded_text_len < 0) {
            LOG_WARNING("fail to decode encryption_key in config");
            return -1;
        }
        decoded_string.assign(decoded_string.data(), decoded_text_len);
        *plaintext = std::move(decoded_string);
        *encoded_ciphertext = cloud::config::encryption_key;
        return 0;
    }

    // 3. otherwise, generate a random data key in memory
    std::mt19937 rnd(time(nullptr));
    std::uniform_int_distribution<char> dist(std::numeric_limits<char>::min(),
                                             std::numeric_limits<char>::max());
    std::string root_key_plaintext(32, '0');
    for (char& i : root_key_plaintext) {
        i = (char)dist(rnd);
    }

    // encode in base64
    int key_len = root_key_plaintext.length();
    int encoded_len = (size_t)(4.0 * ceil(key_len / 3.0));
    std::string encoded_text(encoded_len, '0');
    int encoded_text_len = base64_encode((unsigned char*)root_key_plaintext.data(), key_len,
                                         (unsigned char*)encoded_text.data());
    if (encoded_text_len < 0) {
        LOG_WARNING("failed to encode encryption_key");
        return -1;
    }
    std::string encoded_root_key_ciphertext;
    encoded_root_key_ciphertext.assign(encoded_text.data(), encoded_text_len);

    *plaintext = std::move(root_key_plaintext);
    *encoded_ciphertext = std::move(encoded_root_key_ciphertext);
    return 0;
}

// Todo: Does not need to be locked now, only generated when the process is initialized
std::map<int64_t, std::string> global_encryption_key_info_map; // key_id->encryption_key

static int get_current_root_keys(TxnKv* txn_kv, std::map<int64_t, std::string>* keys) {
    std::unique_ptr<KmsClient> kms_client;
    if (config::enable_kms) {
        if (config::kms_info_encryption_key.empty() || config::kms_info_encryption_method.empty() ||
            config::kms_ak.empty() || config::kms_sk.empty()) {
            LOG_WARNING("incorrect kms conf")
                    .tag("encryption_key", config::kms_info_encryption_key)
                    .tag("encryption_method", config::kms_info_encryption_method)
                    .tag("ak", config::kms_ak)
                    .tag("sk", config::kms_sk);
            return -1;
        }
        std::string decoded_encryption_key(config::kms_info_encryption_key.length(), '0');
        int decoded_key_len = cloud::base64_decode(config::kms_info_encryption_key.c_str(),
                                                   config::kms_info_encryption_key.length(),
                                                   decoded_encryption_key.data());
        decoded_encryption_key.assign(decoded_encryption_key.data(), decoded_key_len);
        AkSkPair out;
        if (decrypt_ak_sk({config::kms_ak, config::kms_sk}, config::kms_info_encryption_method,
                          decoded_encryption_key, &out) != 0) {
            LOG_WARNING("failed to decrypt kms info");
            return -1;
        }

        KmsConf conf {out.first,          out.second,      config::kms_endpoint,
                      config::kms_region, config::kms_cmk, config::kms_provider};

        auto ret = create_kms_client(std::move(conf), &kms_client);
        if (ret != 0) {
            LOG_WARNING("failed to create kms client").tag("ret", ret);
            return -1;
        }
        ret = kms_client->init();
        if (ret != 0) {
            LOG_WARNING("failed to init kms client").tag("ret", ret);
            return -1;
        }
    }

    // To avoid transaction timeouts, it is necessary to first generate a root key
    std::string root_key_plaintext;
    std::string encoded_root_key_ciphertext;
    int ret = generate_random_root_key(txn_kv, kms_client.get(), &root_key_plaintext,
                                       &encoded_root_key_ciphertext);
    if (ret == -1) {
        LOG_WARNING("failed to generate random root key");
        return -1;
    }

    while (true) {
        std::string key = system_meta_service_encryption_key_info_key();
        std::string val;
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to create txn").tag("ret", ret);
            return -1;
        }
        err = txn->get(key, &val);
        if (ret != 0 && ret != 1) {
            LOG_WARNING("failed to get key of encryption_key_info").tag("ret", ret);
            return -1;
        }

        bool need_to_focus_add_kms_data_key = true;
        EncryptionKeyInfoPB key_info;
        if (err == TxnErrorCode::TXN_OK) {
            if (!key_info.ParseFromString(val)) {
                LOG_WARNING("failed to parse encryption_root_key");
                return -1;
            }

            LOG_INFO("get server encryption_root_key").tag("key_info", proto_to_json(key_info));

            for (const auto& item : key_info.items()) {
                std::string encoded_root_key_plaintext;
                if (item.has_kms_info()) {
                    need_to_focus_add_kms_data_key = false;
                    // use kms to decrypt
                    if (kms_client == nullptr) {
                        LOG_WARNING("no kms client");
                        return -1;
                    }
                    if (item.kms_info().endpoint() != kms_client->conf().endpoint ||
                        item.kms_info().region() != kms_client->conf().region) {
                        LOG_WARNING("kms info is not match")
                                .tag("kms endpoint", kms_client->conf().endpoint)
                                .tag("kms region", kms_client->conf().region)
                                .tag("saved endpoint", item.kms_info().endpoint())
                                .tag("saved region", item.kms_info().region());
                        return -1;
                    }

                    auto ret = kms_client->decrypt(item.key(), &encoded_root_key_plaintext);
                    if (ret != 0) {
                        LOG_WARNING("failed to decrypt encryption_root_key");
                        return -1;
                    }
                } else {
                    encoded_root_key_plaintext = item.key(); // Todo: do not copy
                }

                std::string root_key_plaintext(encoded_root_key_plaintext.length(), '0');
                int decoded_text_len = base64_decode(encoded_root_key_plaintext.c_str(),
                                                     encoded_root_key_plaintext.length(),
                                                     root_key_plaintext.data());
                if (decoded_text_len < 0) {
                    LOG_WARNING("failed to decode encryption_root_key");
                    return -1;
                }
                root_key_plaintext.assign(root_key_plaintext.data(), decoded_text_len);
                keys->insert({item.key_id(), std::move(root_key_plaintext)});
            }
            if (config::enable_kms && config::focus_add_kms_data_key &&
                need_to_focus_add_kms_data_key) {
                // Todo: need to restart other ms to update global_encryption_key_info_map now
                LOG(INFO) << "focus to add kms data key";
            } else {
                return 0;
            }
        }

        // encryption_root_key not found, need to save a new root key into fdb
        if (root_key_plaintext.empty() || encoded_root_key_ciphertext.empty()) {
            LOG_WARNING("empty new root key");
            return -1;
        }

        int32_t new_key_id = key_info.items().size() + 1;
        auto* item = key_info.add_items();
        item->set_key_id(new_key_id);
        item->set_key(encoded_root_key_ciphertext);
        if (config::enable_kms) {
            item->mutable_kms_info()->set_endpoint(config::kms_endpoint);
            item->mutable_kms_info()->set_region(config::kms_region);
            item->mutable_kms_info()->set_cmk(config::kms_cmk);
        }

        val = key_info.SerializeAsString();
        if (val.empty()) {
            LOG_WARNING("failed to serialize");
            return -1;
        }
        txn->put(key, val);
        LOG_INFO("put server encryption_key")
                .tag("encryption_key", encoded_root_key_ciphertext)
                .tag("key_id", new_key_id);
        err = txn->commit();
        if (err == TxnErrorCode::TXN_CONFLICT) {
            LOG_WARNING("commit encryption_key is conflicted, retry it later");
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            continue;
        } else if (err != TxnErrorCode::TXN_OK) {
            LOG_WARNING("failed to commit encryption_key");
            return -1;
        }
        keys->insert({new_key_id, std::move(root_key_plaintext)});
        return 0;
    }
    return 0;
}

int init_global_encryption_key_info_map(TxnKv* txn_kv) {
    if (get_current_root_keys(txn_kv, &global_encryption_key_info_map) != 0) {
        return -1;
    }
    DCHECK(!global_encryption_key_info_map.empty());
    return 0;
}

} // namespace doris::cloud
