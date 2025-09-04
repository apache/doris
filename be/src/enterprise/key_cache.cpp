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

#include "enterprise/key_cache.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <openssl/rand.h>

#include <memory>

#include "common/status.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/crc32c.h"
#include "util/thrift_rpc_helper.h"
#include "util/url_coding.h"

namespace doris {

constexpr int AES_256_KEY_LEN = 32;
constexpr int AES_IV_LEN = 16;
constexpr int SM4_128_KEY_LEN = 16;
constexpr int SM4_IV_LEN = 16;

bool fromThrift(const TEncryptionKey& tk, std::shared_ptr<EncryptionKeyPB> ek) {
    const auto& pstr = tk.key_pb;
    if (!ek->ParseFromString(pstr)) {
        return false;
    }

    return ek->has_id() && ek->has_version() && ek->has_parent_version() && ek->has_algorithm() &&
           ek->has_type() && ek->has_ciphertext_base64() && ek->has_plaintext() &&
           ek->has_crc32() && ek->has_ctime() && ek->has_mtime();
}

std::shared_ptr<EncryptionKeyPB> KeyCache::get_master_key(std::string key_id, int version,
                                                          EncryptionAlgorithmPB algorithm) {
    {
        std::shared_lock lock(_mutex);
        if (algorithm == EncryptionAlgorithmPB::AES_256_CTR) {
            auto it = _aes256_master_keys.find(version);
            if (it != _aes256_master_keys.end()) {
                return it->second;
            }
        }

        if (algorithm == EncryptionAlgorithmPB::SM4_128_CTR) {
            auto it = _sm4_master_keys.find(version);
            if (it != _sm4_master_keys.end()) {
                return it->second;
            }
        }
    }

    refresh_all_data_keys();

    {
        std::shared_lock lock(_mutex);
        if (algorithm == EncryptionAlgorithmPB::AES_256_CTR) {
            auto it = _aes256_master_keys.find(version);
            if (it != _aes256_master_keys.end()) {
                return it->second;
            }
        }

        if (algorithm == EncryptionAlgorithmPB::SM4_128_CTR) {
            auto it = _sm4_master_keys.find(version);
            if (it != _sm4_master_keys.end()) {
                return it->second;
            }
        }
    }

    return nullptr;
}

std::shared_ptr<EncryptionKeyPB> KeyCache::get_latest_master_key(EncryptionAlgorithmPB algorithm) {
    {
        std::shared_lock lock(_mutex);
        if (algorithm == EncryptionAlgorithmPB::AES_256_CTR) {
            if (!_aes256_master_keys.empty()) {
                auto it = _aes256_master_keys.rbegin();
                return it->second;
            }
        } else if (algorithm == EncryptionAlgorithmPB::SM4_128_CTR) {
            if (!_sm4_master_keys.empty()) {
                auto it = _sm4_master_keys.rbegin();
                return it->second;
            }
        } else {
            CHECK(false) << "invalid encryption algorithm: " << algorithm;
        }
    }

    refresh_all_data_keys();

    {
        std::shared_lock lock(_mutex);
        if (algorithm == EncryptionAlgorithmPB::AES_256_CTR) {
            if (!_aes256_master_keys.empty()) {
                auto it = _aes256_master_keys.rbegin();
                return it->second;
            }
        } else if (algorithm == EncryptionAlgorithmPB::SM4_128_CTR) {
            if (!_sm4_master_keys.empty()) {
                auto it = _sm4_master_keys.rbegin();
                return it->second;
            }
        } else {
            CHECK(false) << "invalid encryption algorithm: " << algorithm;
        }
    }

    return nullptr;
}

std::shared_ptr<EncryptionKeyPB> KeyCache::generate_data_key(EncryptionAlgorithmPB algorithm) {
    VLOG(3) << "generate data key: " << algorithm;
    auto master_key = get_latest_master_key(algorithm);
    if (!master_key) {
        return nullptr;
    }

    size_t key_len = 0;
    size_t iv_len = 0;
    const EVP_CIPHER* cipher = nullptr;

    switch (algorithm) {
    case EncryptionAlgorithmPB::AES_256_CTR:
        key_len = AES_256_KEY_LEN;
        iv_len = AES_IV_LEN;
        cipher = EVP_aes_256_ctr();
        break;
    case EncryptionAlgorithmPB::SM4_128_CTR:
        key_len = SM4_128_KEY_LEN;
        iv_len = SM4_IV_LEN;
        cipher = EVP_sm4_ctr();
        break;
    default:
        CHECK(false) << "invalid encryption algorithm: " << algorithm;
        return nullptr;
    }

    auto data_key = std::make_shared<EncryptionKeyPB>();
    data_key->set_parent_id(master_key->id());
    data_key->set_parent_version(master_key->version());
    data_key->set_version(1);
    data_key->set_type(EncryptionKeyTypePB::DATA_KEY);
    data_key->set_algorithm(algorithm);

    auto* mut_plain_text = data_key->mutable_plaintext();
    mut_plain_text->resize(key_len);

    std::string iv;
    iv.resize(iv_len);

    if (!RAND_bytes(reinterpret_cast<unsigned char*>(mut_plain_text->data()),
                    mut_plain_text->size())) {
        LOG(WARNING) << "failed to generate random plaintext key";
        return nullptr;
    }

    if (!RAND_bytes(reinterpret_cast<unsigned char*>(iv.data()), iv.size())) {
        LOG(WARNING) << "failed to generate random iv";
        return nullptr;
    }

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return nullptr;
    }

    if (EVP_EncryptInit_ex(ctx, cipher, nullptr,
                           reinterpret_cast<const unsigned char*>(master_key->plaintext().data()),
                           reinterpret_cast<const unsigned char*>(iv.data())) != 1) {
        LOG(WARNING) << "EVP_EncryptInit_ex failed";
        EVP_CIPHER_CTX_free(ctx);
        return nullptr;
    }

    std::string ciphertext;
    ciphertext.resize(key_len);
    int outlen = 0;
    if (EVP_EncryptUpdate(ctx, reinterpret_cast<unsigned char*>(ciphertext.data()), &outlen,
                          reinterpret_cast<const unsigned char*>(data_key->plaintext().data()),
                          data_key->plaintext().size()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        return nullptr;
    }

    if (static_cast<size_t>(outlen) != key_len) {
        LOG(WARNING) << "outlen len: " << outlen << " != key len: " << key_len;
        EVP_CIPHER_CTX_free(ctx);
        return nullptr;
    }

    EVP_CIPHER_CTX_free(ctx);

    data_key->set_id(std::to_string(time(nullptr)));
    auto now = static_cast<int64_t>(time(nullptr)) * 1000;
    data_key->set_ctime(now);
    data_key->set_mtime(now);
    base64_encode(iv, data_key->mutable_iv_base64());
    base64_encode(ciphertext, data_key->mutable_ciphertext_base64());

    data_key->set_crc32(crc32c::Value(data_key->plaintext().data(), data_key->plaintext().size()));

    return data_key;
}

Status KeyCache::decrypt_data_key(std::shared_ptr<EncryptionKeyPB>& data_key_cipher) {
    auto master_key =
            get_master_key(data_key_cipher->parent_id(), data_key_cipher->parent_version(),
                           data_key_cipher->algorithm());
    if (!master_key) {
        LOG(WARNING) << "failed to get master key, parent key id: " << data_key_cipher->parent_id()
                     << " parent key version: " << data_key_cipher->version()
                     << " data key algorithm: " << data_key_cipher->algorithm();
        return Status::InternalError("failed to get master key");
    }

    size_t key_len = 0;
    size_t iv_len = 0;
    const EVP_CIPHER* cipher = nullptr;

    switch (data_key_cipher->algorithm()) {
    case EncryptionAlgorithmPB::AES_256_CTR:
        key_len = AES_256_KEY_LEN;
        iv_len = AES_IV_LEN;
        cipher = EVP_aes_256_ctr();
        break;
    case EncryptionAlgorithmPB::SM4_128_CTR:
        key_len = SM4_128_KEY_LEN;
        iv_len = SM4_IV_LEN;
        cipher = EVP_sm4_ctr();
        break;
    default:
        LOG(ERROR) << "invalid encryption algorithm: " << data_key_cipher->algorithm();
        return Status::InternalError("invalid encryption algorithm {}",
                                     data_key_cipher->algorithm());
    }

    std::string ciphertext;
    ciphertext.resize(key_len);
    base64_decode(data_key_cipher->ciphertext_base64(), &ciphertext);

    std::string iv;
    iv.resize(iv_len);
    base64_decode(data_key_cipher->iv_base64(), &iv);

    data_key_cipher->mutable_plaintext()->assign(key_len, 0);

    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return Status::InternalError("failed to new cipher ctx");
    }

    if (EVP_DecryptInit_ex(ctx, cipher, nullptr,
                           reinterpret_cast<const unsigned char*>(master_key->plaintext().data()),
                           reinterpret_cast<const unsigned char*>(iv.data())) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        LOG(WARNING) << "failed to decryptInit, master key plaintext len: "
                     << master_key->plaintext().length() << " iv length: " << iv.length();
        return Status::InternalError("failed to decryptInit");
    }
    EVP_CIPHER_CTX_set_padding(ctx, 0);

    int outlen = 0;
    if (EVP_DecryptUpdate(
                ctx, reinterpret_cast<unsigned char*>(data_key_cipher->mutable_plaintext()->data()),
                &outlen, reinterpret_cast<const unsigned char*>(ciphertext.data()), key_len) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        LOG(WARNING) << "failed to decryptUpdate, data key ciphertext len: " << key_len;
        return Status::InternalError("failed to decryptUpdate");
    }
    if (static_cast<size_t>(outlen) != key_len) {
        EVP_CIPHER_CTX_free(ctx);
        LOG(ERROR) << "data key ciphertext len: " << key_len
                   << " does not equals data key plaintext len: " << outlen;
        return Status::InternalError("data key ciphertext length is not equal to plaintext length");
    }

    EVP_CIPHER_CTX_free(ctx);

    uint32_t crc = crc32c::Value(data_key_cipher->plaintext().data(), key_len);
    if (crc != data_key_cipher->crc32()) {
        LOG(ERROR) << "decrypted plaintext crc: " << crc
                   << " does not equals original crc: " << data_key_cipher->crc32();
        return Status::InternalError("decrypted plaintext crc is not equal to original crc");
    }

    return Status::OK();
}

Status KeyCache::get_master_keys() {
    TGetEncryptionKeysRequest request;
    request.version = 1;

    TNetworkAddress master_addr = ExecEnv::GetInstance()->cluster_info()->master_fe_addr;
    TGetEncryptionKeysResult result;
    {
        RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
                master_addr.hostname, master_addr.port,
                [&request, &result](FrontendServiceConnection& client) {
                    client->getEncryptionKeys(result, request);
                }));
    }

    LOG(INFO) << "get master keys status: " << result.status;
    Status status(Status::create(result.status));
    if (!status.ok()) {
        LOG(WARNING) << "failed to get master key, status: " << status;
        return status;
    }

    for (TEncryptionKey t_key : result.master_keys) {
        std::unique_lock lock(_mutex);
        auto key = std::make_shared<EncryptionKeyPB>();
        bool ret = fromThrift(t_key, key);
        if (!ret) {
            return Status::InternalError("failed to convert from thrift to pb");
        }
        LOG(INFO) << "get a master key, key id: " << key->id() << " key version: " << key->version()
                  << " key algorithm: " << key->algorithm();
        if (key->algorithm() == EncryptionAlgorithmPB::AES_256_CTR) {
            _aes256_master_keys[key->version()] = key;
        } else if (key->algorithm() == EncryptionAlgorithmPB::SM4_128_CTR) {
            _sm4_master_keys[key->version()] = key;
        } else {
            CHECK(false) << "invalid encryption algorithm: " << key->algorithm()
                         << " key id: " << key->id() << " key version: " << key->version();
        }
    }
    return Status::OK();
}

void KeyCache::refresh_all_data_keys() {
    Status st = get_master_keys();
    if (!st.ok()) {
        return;
    }
}

} // namespace doris
