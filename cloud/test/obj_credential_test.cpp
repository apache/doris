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

#include "common/auth/obj_credential.h"

#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

namespace doris::cloud {
namespace {

constexpr char ACCOUNT[] = "original@my-project.iam.gserviceaccount.com";
constexpr char NEW_ACCOUNT[] = "updated@my-project.iam.gserviceaccount.com";

ObjectStoreInfoPB make_gcp_vault() {
    ObjectStoreInfoPB obj;
    obj.set_provider(ObjectStoreInfoPB::GCP);
    obj.set_bucket("test-bucket");
    obj.set_endpoint("storage.googleapis.com");
    obj.set_region("us-east1");
    auto* credential = obj.mutable_credential()->mutable_gcp_credential();
    credential->set_credential_provider_type(GcpCredentialPB::COMPUTE_ENGINE);
    credential->set_impersonation_service_account(ACCOUNT);
    return obj;
}

TEST(ObjCredentialTest, AlterTargetPreservesSourceAndStorageFields) {
    auto stored = make_gcp_vault();
    auto expected = stored;
    expected.mutable_credential()->mutable_gcp_credential()->set_impersonation_service_account(
            NEW_ACCOUNT);
    ObjectStoreInfoPB update;
    update.mutable_credential()->mutable_gcp_credential()->set_impersonation_service_account(
            NEW_ACCOUNT);
    auto error = apply_obj_credential(update, &stored);
    ASSERT_FALSE(error.has_value()) << error.value_or("");
    EXPECT_EQ(stored.SerializeAsString(), expected.SerializeAsString());
}

TEST(ObjCredentialTest, AlterSourcePreservesTarget) {
    for (auto source : {GcpCredentialPB::COMPUTE_ENGINE, GcpCredentialPB::DEFAULT}) {
        auto stored = make_gcp_vault();
        ObjectStoreInfoPB update;
        update.mutable_credential()->mutable_gcp_credential()->set_credential_provider_type(source);
        auto error = apply_obj_credential(update, &stored);
        ASSERT_FALSE(error.has_value()) << error.value_or("");
        EXPECT_EQ(stored.credential().gcp_credential().credential_provider_type(), source);
        EXPECT_EQ(stored.credential().gcp_credential().impersonation_service_account(), ACCOUNT);
    }
}

TEST(ObjCredentialTest, ExplicitEmptyTargetClearsOnlyImpersonation) {
    auto stored = make_gcp_vault();
    ObjectStoreInfoPB update;
    update.mutable_credential()->mutable_gcp_credential()->set_impersonation_service_account("");
    auto error = apply_obj_credential(update, &stored);
    ASSERT_FALSE(error.has_value()) << error.value_or("");
    EXPECT_EQ(stored.credential().gcp_credential().credential_provider_type(),
              GcpCredentialPB::COMPUTE_ENGINE);
    EXPECT_FALSE(stored.credential().gcp_credential().has_impersonation_service_account());
}

TEST(ObjCredentialTest, SwitchFromStaticDefaultsSourceAndRemovesLegacyCredentials) {
    for (auto account : {NEW_ACCOUNT, ""}) {
        for (bool explicit_source : {false, true}) {
            auto stored = make_gcp_vault();
            stored.clear_credential();
            stored.set_ak("encrypted-ak");
            stored.set_sk("encrypted-sk");
            stored.mutable_encryption_info()->set_key_id(1);
            const auto original = stored.SerializeAsString();
            ObjectStoreInfoPB update;
            auto* credential = update.mutable_credential()->mutable_gcp_credential();
            credential->set_impersonation_service_account(account);
            if (explicit_source) {
                credential->set_credential_provider_type(GcpCredentialPB::COMPUTE_ENGINE);
            }
            auto error = apply_obj_credential(update, &stored);
            if (!explicit_source && std::string(account).empty()) {
                ASSERT_TRUE(error.has_value());
                EXPECT_EQ(stored.SerializeAsString(), original);
                continue;
            }
            ASSERT_FALSE(error.has_value()) << error.value_or("");
            EXPECT_TRUE(stored.credential().gcp_credential().has_credential_provider_type());
            EXPECT_EQ(stored.credential().gcp_credential().credential_provider_type(),
                      explicit_source ? GcpCredentialPB::COMPUTE_ENGINE : GcpCredentialPB::DEFAULT);
            EXPECT_EQ(stored.credential().gcp_credential().impersonation_service_account(),
                      account);
            EXPECT_FALSE(stored.has_ak());
            EXPECT_FALSE(stored.has_sk());
            EXPECT_FALSE(stored.has_encryption_info());
            EXPECT_FALSE(stored.has_cred_provider_type());
            EXPECT_FALSE(stored.has_role_arn());
            EXPECT_FALSE(stored.has_external_id());
        }
    }
}

TEST(ObjCredentialTest, RejectedPatchDoesNotMutateStoredCredential) {
    for (bool native : {false, true}) {
        auto stored = make_gcp_vault();
        if (!native) {
            stored.clear_credential();
            stored.set_ak("encrypted-ak");
            stored.set_sk("encrypted-sk");
            stored.mutable_encryption_info()->set_key_id(1);
        }
        const auto original = stored.SerializeAsString();
        ObjectStoreInfoPB update;
        update.mutable_credential()->mutable_gcp_credential()->set_impersonation_service_account(
                "invalid-account");
        EXPECT_TRUE(apply_obj_credential(update, &stored).has_value());
        EXPECT_EQ(stored.SerializeAsString(), original);

        update.mutable_credential()->mutable_gcp_credential()->set_impersonation_service_account(
                NEW_ACCOUNT);
        update.set_ak("conflicting-ak");
        EXPECT_TRUE(apply_obj_credential(update, &stored).has_value());
        EXPECT_EQ(stored.SerializeAsString(), original);
    }
}

TEST(ObjCredentialTest, RejectsEmptyPatchAndCrossProviderUpdate) {
    auto stored = make_gcp_vault();
    auto original = stored.SerializeAsString();
    ObjectStoreInfoPB update;
    update.mutable_credential()->mutable_gcp_credential();
    EXPECT_TRUE(apply_obj_credential(update, &stored).has_value());
    EXPECT_EQ(stored.SerializeAsString(), original);

    stored.set_provider(ObjectStoreInfoPB::S3);
    original = stored.SerializeAsString();
    update.mutable_credential()->mutable_gcp_credential()->set_impersonation_service_account(
            NEW_ACCOUNT);
    EXPECT_TRUE(apply_obj_credential(update, &stored).has_value());
    EXPECT_EQ(stored.SerializeAsString(), original);
}

} // namespace
} // namespace doris::cloud
