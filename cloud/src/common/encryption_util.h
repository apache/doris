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

#include <stdint.h>

#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>

namespace doris::cloud {
namespace config {
extern std::string encryption_method;
} // namespace config
class TxnKv;
class EncryptionInfoPB;

using AkSkPair = std::pair<std::string, std::string>;
using AkSkRef = std::pair<std::string_view, std::string_view>;
/**
 * @brief Encrypt ak/sk pair
 *
 * @param plain_ak_sk plain ak/sk pair
 * @param encryption_method encryption_method
 * @param encryption_key encryption_key
 * @param cipher_ak_sk output cipher ak/sk pair in base64 format
 * @return 0 for success, negative for error
 */
int encrypt_ak_sk(AkSkRef plain_ak_sk, const std::string& encryption_method,
                  const std::string& encryption_key, AkSkPair* cipher_ak_sk);

/**
 * @brief Decrypt ak/sk pair
 * 
 * @param cipher_ak_sk cipher ak/sk pair in base64 format
 * @param encryption_method encryption_method
 * @param encryption_key encryption_key
 * @param plain_ak_sk output plain ak/sk pair
 * @return 0 for success, negative for error
 */
int decrypt_ak_sk(AkSkRef cipher_ak_sk, const std::string& encryption_method,
                  const std::string& encryption_key, AkSkPair* plain_ak_sk);

extern std::map<int64_t, std::string> global_encryption_key_info_map;

// Todo: Should we need to refresh it
int init_global_encryption_key_info_map(TxnKv* txn_kv);

/**
 * @brief Get the encryption key for ak sk by key_id
 * 
 * @param version_id 
 * @param encryption_key output encryption_key 
 * @return 0 for success, negative for error 
 */
inline static int get_encryption_key_for_ak_sk(int64_t key_id, std::string* encryption_key) {
    if (global_encryption_key_info_map.count(key_id)) {
        *encryption_key = global_encryption_key_info_map.at(key_id);
        return 0;
    }
    return -1;
}

int decrypt_ak_sk_helper(std::string_view cipher_ak, std::string_view cipher_sk,
                         const EncryptionInfoPB& encryption_info, AkSkPair* plain_ak_sk_pair);

/**
 * @brief Get the newest encryption key for ak sk
 * 
 * @param key_id
 * @param encryption_key 
 * @return 0 for success, negative for error  
 */
inline static int get_newest_encryption_key_for_ak_sk(int64_t* key_id,
                                                      std::string* encryption_key) {
    if (global_encryption_key_info_map.empty()) {
        return -1;
    }
    auto it = global_encryption_key_info_map.crbegin();
    *key_id = it->first;
    *encryption_key = it->second;
    return 0;
}

inline static const std::string& get_encryption_method_for_ak_sk() {
    return config::encryption_method;
}

size_t base64_decode(const char* data, size_t length, char* decoded_data);

} // namespace doris::cloud
