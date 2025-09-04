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

#include "enterprise/encryption_common.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <openssl/evp.h>
#include <openssl/rand.h>

#include <cstddef>
#include <memory>

#include "common/status.h"
#include "enterprise/key_cache.h"
#include "gutil/endian.h"

namespace doris::io {

KeyCache key_cache;

Result<std::unique_ptr<EncryptionInfo>> EncryptionInfo::create(EncryptionAlgorithmPB algorithm) {
    auto data_key = key_cache.generate_data_key(algorithm);
    if (data_key == nullptr) {
        return ResultError(Status::InternalError("generate data key error"));
    }
    auto info = std::make_unique<EncryptionInfo>();
    info->data_key = std::move(data_key);
    // generate random nonce
    RAND_bytes(info->iv_base.data(), sizeof(uint64_t));
    return info;
}

std::string EncryptionInfo::serialize() const {
    FileEncryptionInfoPB pb;
    pb.set_data_iv_nonce(iv_base.data(), iv_base.size());
    auto* key_info = pb.mutable_data_key_info();
    key_info->set_parent_id(data_key->parent_id());
    key_info->set_parent_version(data_key->parent_version());
    key_info->set_algorithm(data_key->algorithm());
    key_info->set_ciphertext_base64(data_key->ciphertext_base64());
    key_info->set_iv_base64(data_key->iv_base64());
    key_info->set_crc32(data_key->crc32());
    return pb.SerializeAsString();
}

Result<std::unique_ptr<EncryptionInfo>> EncryptionInfo::load(const FileEncryptionInfoPB& info_pb) {
    auto data_key = std::make_shared<EncryptionKeyPB>();
    *data_key = info_pb.data_key_info();

    Status st = key_cache.decrypt_data_key(data_key);
    if (!st.ok()) {
        return ResultError(st);
    }

    auto info = std::make_unique<EncryptionInfo>();
    info->data_key = std::move(data_key);
    info->iv_base = {info_pb.data_iv_nonce().begin(), info_pb.data_iv_nonce().end()};
    return info;
}

const EVP_CIPHER* EncryptionInfo::evp_cipher() const {
    switch (data_key->algorithm()) {
    case EncryptionAlgorithmPB::AES_256_CTR:
        return EVP_aes_256_ctr();
    case EncryptionAlgorithmPB::SM4_128_CTR:
        return EVP_sm4_ctr();
    default:
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Only AES_256_CTR and SM4_128_CTR are supported currently");
    }
}

std::vector<uint8_t> generate_iv(const std::vector<uint8_t>& iv_base, size_t offset) {
    // TODO(tsy): return a new allocated iv, keep iv_base immutable
    DCHECK_EQ(iv_base.size(), 16);
    auto iv = iv_base;
    auto* iv_data = iv.data();
    auto* counter = reinterpret_cast<uint64_t*>(iv_data + 8);
    *counter = gbswap_64(offset / ENCRYPT_BLOCK_SIZE);
    return iv;
}

} // namespace doris::io
