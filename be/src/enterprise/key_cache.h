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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/olap_file.pb.h>

#include <map>
#include <memory>
#include <shared_mutex>
#include <string>

#include "common/status.h"

namespace doris {

enum class EncryptionAlgorithm { AES256 = 0, SM4 = 1 };
enum class EncryptionKeyType { MASTER_KEY = 0, DATA_KEY = 1 };

class KeyCache {
public:
    std::shared_ptr<EncryptionKeyPB> generate_data_key(EncryptionAlgorithmPB algorithm);
    Status decrypt_data_key(std::shared_ptr<EncryptionKeyPB>& data_key_cipher);

    void refresh_all_data_keys();
    Status get_master_keys();

private:
    std::shared_ptr<EncryptionKeyPB> get_master_key(std::string key_id, int version,
                                                    EncryptionAlgorithmPB algorithm);
    std::shared_ptr<EncryptionKeyPB> get_latest_master_key(EncryptionAlgorithmPB algorithm);

    std::map<int, std::shared_ptr<EncryptionKeyPB>> _aes256_master_keys;
    std::map<int, std::shared_ptr<EncryptionKeyPB>> _sm4_master_keys;
    mutable std::shared_mutex _mutex;
};

} // namespace doris
