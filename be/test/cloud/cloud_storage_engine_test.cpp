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

#include "util/s3_util.h"

namespace doris {

class CloudStorageEngineTest : public testing::Test {};

TEST_F(CloudStorageEngineTest, CheckNewAndChangedKeylessStorageVault) {
    bool previous_value = config::enable_check_storage_vault;
    config::enable_check_storage_vault = true;

    S3ClientConf hmac_conf {
            .endpoint = std::string(GCS_XML_ENDPOINT),
            .region = "us-central1",
            .ak = "access-key",
            .sk = "secret-key",
            .bucket = "bucket",
            .provider = io::ObjStorageType::GCP,
    };
    S3ClientConf workload_identity_conf = hmac_conf;
    workload_identity_conf.ak.clear();
    workload_identity_conf.sk.clear();
    workload_identity_conf.cred_provider_type = CredProviderType::GcpWorkloadIdentity;

    EXPECT_TRUE(CloudStorageEngine::_should_check_storage_vault(nullptr, workload_identity_conf));
    EXPECT_TRUE(
            CloudStorageEngine::_should_check_storage_vault(&hmac_conf, workload_identity_conf));
    EXPECT_FALSE(CloudStorageEngine::_should_check_storage_vault(&workload_identity_conf,
                                                                 workload_identity_conf));
    EXPECT_FALSE(CloudStorageEngine::_should_check_storage_vault(nullptr, hmac_conf));

    S3ClientConf role_conf = hmac_conf;
    role_conf.ak.clear();
    role_conf.sk.clear();
    role_conf.role_arn = "arn:aws:iam::123456789012:role/test-role";
    EXPECT_TRUE(CloudStorageEngine::_should_check_storage_vault(nullptr, role_conf));

    config::enable_check_storage_vault = false;
    EXPECT_FALSE(CloudStorageEngine::_should_check_storage_vault(nullptr, workload_identity_conf));

    config::enable_check_storage_vault = previous_value;
}

} // namespace doris
