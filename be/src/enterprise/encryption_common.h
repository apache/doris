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

#pragma once

#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <openssl/rand.h>

#include <cstdint>
#include <span>
#include <string>

#include "common/status.h"
#include "enterprise/key_cache.h"
#include "gen_cpp/olap_file.pb.h"

namespace doris::io {

// ABCDEABC in fixed LE encoded file
static constexpr uint64_t MAGIC_CODE = 0x4342414544434241ULL;
static constexpr uint8_t VERSION = 0;

static constexpr uint8_t ENCRYPT_BLOCK_SIZE = 16;

static constexpr uint64_t ENCRYPT_FOOTER_LENGTH = 256;

extern KeyCache key_cache;

struct EncryptionInfo {
    EncryptionInfo() : iv_base(16) {}

    std::shared_ptr<EncryptionKeyPB> data_key;
    bool padding = false;
    std::vector<uint8_t> iv_base;

    static Result<std::unique_ptr<EncryptionInfo>> create(EncryptionAlgorithmPB algorithm);

    static Result<std::unique_ptr<EncryptionInfo>> load(const FileEncryptionInfoPB& info_pb);

    std::string serialize() const;

    std::span<const uint8_t> plain_key() const {
        return {reinterpret_cast<const uint8_t*>(data_key->plaintext().data()),
                data_key->plaintext().size()};
    }

    const EVP_CIPHER* evp_cipher() const;
};

std::vector<uint8_t> generate_iv(const std::vector<uint8_t>& iv_base, size_t offset);

} // namespace doris::io
