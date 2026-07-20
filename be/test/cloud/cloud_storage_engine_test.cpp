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

#include "cloud/cloud_storage_engine.h"

#include <gtest/gtest.h>

namespace doris {

class CloudStorageEngineTest : public testing::Test {};

TEST_F(CloudStorageEngineTest, CheckStorageVaultOnlyOnFirstSync) {
    CloudStorageEngine engine(EngineOptions {});
    bool previous_value = config::enable_check_storage_vault;
    config::enable_check_storage_vault = true;

    EXPECT_TRUE(engine._should_check_storage_vault());
    EXPECT_FALSE(engine._should_check_storage_vault());

    config::enable_check_storage_vault = previous_value;
}

} // namespace doris
