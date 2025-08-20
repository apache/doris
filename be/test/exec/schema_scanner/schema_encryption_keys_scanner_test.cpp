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

#include "exec/schema_scanner/schema_encryption_keys_scanner.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include "vec/core/block.h"

namespace doris {

class ScheamEncryptionKeysScannerTest : public testing::Test {
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(ScheamEncryptionKeysScannerTest, test_get_next_block_internal) {
    SchemaEncryptionKeysScanner scnanner;
    auto& keys = scnanner._master_keys;
    EncryptionKeyPB key;
    keys.push_back(key);

    auto data_block = vectorized::Block::create_unique();
    scanner._init_block(data_block.get());

    auto st = scanner._fill_block_impl(data_block.get());
}

} // namespace doris
