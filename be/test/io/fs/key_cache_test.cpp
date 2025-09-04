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

#include <gen_cpp/olap_file.pb.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <filesystem>
#include <memory>

#include "common/status.h"
#include "enterprise/encryption_common.h"
#include "gtest/gtest_pred_impl.h"

namespace doris::io {

class KeyCacheTest : public testing::Test {
public:
    virtual void SetUp() {}

    virtual void TearDown() {}
};

TEST_F(KeyCacheTest, TestGenerateDataKey) {
    std::shared_ptr<EncryptionKeyPB> master_key = std::make_shared<EncryptionKeyPB>();
    master_key->set_plaintext(std::string(32, '\x0A'));
    master_key->set_version(1);
    master_key->set_id("master_key_id");
    key_cache._aes256_master_keys[1] = master_key;

    std::shared_ptr<EncryptionKeyPB> data_key =
            key_cache.generate_data_key(EncryptionAlgorithmPB::AES_256_CTR);
    EXPECT_EQ(data_key->parent_id(), "master_key_id");
    EXPECT_EQ(data_key->parent_version(), 1);

    std::shared_ptr<EncryptionKeyPB> cipher_data_key = std::make_shared<EncryptionKeyPB>();
    cipher_data_key->set_parent_id(data_key->parent_id());
    cipher_data_key->set_parent_version(data_key->parent_version());
    cipher_data_key->set_iv_base64(data_key->iv_base64());
    cipher_data_key->set_ciphertext_base64(data_key->ciphertext_base64());
    cipher_data_key->set_crc32(data_key->crc32());
    cipher_data_key->set_algorithm(data_key->algorithm());

    Status st = key_cache.decrypt_data_key(cipher_data_key);
    EXPECT_TRUE(st.OK());
    EXPECT_EQ(cipher_data_key->plaintext(), data_key->plaintext());
}

} // namespace doris::io
