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

#include "information_schema/schema_encryption_keys_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include "core/block/block.h"

namespace doris {

class ScheamEncryptionKeysScannerTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ScheamEncryptionKeysScannerTest, test_get_next_block_internal) {
    SchemaEncryptionKeysScanner scanner;
    auto& keys = scanner._master_keys;
    keys.emplace_back();
    EncryptionKeyPB key_with_sensitive_values;
    key_with_sensitive_values.set_iv_base64("sensitive iv");
    key_with_sensitive_values.set_ciphertext_base64("sensitive cipher");
    keys.push_back(key_with_sensitive_values);

    auto data_block = Block::create_unique();
    scanner._init_block(data_block.get());

    auto st = scanner._fill_block_impl(data_block.get());
    ASSERT_EQ(Status::OK(), st);
    ASSERT_EQ(2, data_block->rows());

    const auto& iv_column = data_block->safe_get_by_position(6).column;
    EXPECT_EQ("", (*iv_column)[0].get<TYPE_STRING>());
    EXPECT_EQ("******", (*iv_column)[1].get<TYPE_STRING>());

    const auto& cipher_column = data_block->safe_get_by_position(7).column;
    EXPECT_EQ("", (*cipher_column)[0].get<TYPE_STRING>());
    EXPECT_EQ("******", (*cipher_column)[1].get<TYPE_STRING>());
}

} // namespace doris
