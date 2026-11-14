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

#include "recycler/s3_accessor.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <butil/guid.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <azure/storage/blobs/blob_options.hpp>
#include <chrono>
#include <initializer_list>
#include <unordered_set>
#include <utility>

#include "common/auth/obj_credential.h"
#include "common/config.h"
#include "common/configbase.h"
#include "common/defer.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "cpp/obj-storage/client/obj_storage_client_builder.h"
#include "cpp/sync_point.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!cloud::init_glog("s3_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    LOG(INFO) << "s3_accessor_test starting";
    initialize_obj_storage_sdks(cloud::config::aws_log_level, cloud::config::azure_log_level);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {
namespace {

class S3AccessorTest : public testing::Test {
    void SetUp() override {
        if (cloud::config::test_s3_ak.empty()) {
            GTEST_SKIP() << "empty test_s3_ak, skip S3AccessorTest";
        }
    }
};

std::optional<ObjStorageResourceConfig> test_resource(ObjectStoreInfoPB::Provider provider) {
    ObjectStoreInfoPB vault;
    vault.set_ak(config::test_s3_ak);
    vault.set_sk(config::test_s3_sk);
    vault.set_endpoint(config::test_s3_endpoint);
    vault.set_region(config::test_s3_region);
    vault.set_bucket(config::test_s3_bucket);
    vault.set_prefix(config::test_s3_prefix + "/S3AccessorTest/" + butil::GenerateGUID());
    vault.set_provider(provider);
    return parse_recycler_storage_config(std::move(vault));
}

void test_s3_accessor(S3Accessor& accessor) {
    std::string file1 = "data/10000/1_0.dat";

    int ret = accessor.delete_directory("");
    ASSERT_NE(ret, 0);
    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);

    ret = accessor.put_file(file1, "");
    ASSERT_EQ(ret, 0);

    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 0);

    std::unique_ptr<ListIterator> iter;
    ret = accessor.list_directory("data", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(iter);
    ASSERT_TRUE(iter->is_valid());
    ASSERT_TRUE(iter->has_next());
    ASSERT_EQ(iter->next()->path, file1);
    ASSERT_FALSE(iter->has_next());

    ret = accessor.list_directory("data/", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(iter->is_valid());
    ASSERT_TRUE(iter->has_next());
    ASSERT_EQ(iter->next()->path, file1);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());

    ret = accessor.list_directory("data/100", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());

    ret = accessor.delete_file(file1);
    ASSERT_EQ(ret, 0);
    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 1);
    ret = accessor.list_directory("", &iter);
    ASSERT_NE(ret, 0);
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
    ret = accessor.delete_file(file1);
    EXPECT_EQ(ret, 0);

    std::vector<std::string> files;
    for (int dir = 10000; dir < 10005; ++dir) {
        for (int suffix = 0; suffix < 5; ++suffix) {
            files.push_back(fmt::format("data/{}/1/{}.dat", dir, suffix));
        }
    }

    for (auto&& file : files) {
        ret = accessor.put_file(file, "");
        ASSERT_EQ(ret, 0);
    }

    std::unordered_set<std::string> list_files;
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    std::vector<std::string> to_delete_files;
    to_delete_files.reserve(5);
    for (int i = 0; i < 5; ++i) {
        to_delete_files.push_back(std::move(files.back()));
        files.pop_back();
    }
    ret = accessor.delete_files(to_delete_files);
    ASSERT_EQ(ret, 0);

    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        EXPECT_LT(now - file->mtime_s, 60);
        list_files.insert(std::move(file->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    std::string to_delete_dir = "data/10001";
    ret = accessor.delete_directory(to_delete_dir);
    ASSERT_EQ(ret, 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& file) { return file.starts_with(to_delete_dir); }),
                files.end());
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    std::string to_delete_prefix = "data/10003/";
    ret = accessor.delete_prefix(to_delete_prefix);
    ASSERT_EQ(ret, 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& file) { return file.starts_with(to_delete_prefix); }),
                files.end());
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto file = iter->next(); file.has_value(); file = iter->next()) {
        list_files.insert(std::move(file->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
}

} // namespace

TEST_F(S3AccessorTest, s3) {
    std::shared_ptr<S3Accessor> accessor;
    auto config = test_resource(ObjectStoreInfoPB::S3);
    ASSERT_TRUE(config.has_value());
    ASSERT_EQ(S3Accessor::create(std::move(*config), &accessor), 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "S3ObjStorageClient::list_objects",
            [](auto&& args) {
                auto* req = try_any_cast<Aws::S3::Model::ListObjectsV2Request*>(args[0]);
                EXPECT_EQ(req->GetMaxKeys(), 1000);
                req->SetMaxKeys(7);
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_batch_size",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args[0]);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_objects_recursively_",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());

    test_s3_accessor(*accessor);
}

TEST_F(S3AccessorTest, azure) {
    std::shared_ptr<S3Accessor> accessor;
    auto config = test_resource(ObjectStoreInfoPB::AZURE);
    ASSERT_TRUE(config.has_value());
    ASSERT_EQ(S3Accessor::create(std::move(*config), &accessor), 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "AzureObjStorageClient::list_objects",
            [](auto&& args) {
                auto* req = try_any_cast<Azure::Storage::Blobs::ListBlobsOptions*>(args[0]);
                ASSERT_TRUE(req->PageSizeHint.HasValue());
                EXPECT_EQ(req->PageSizeHint.Value(), 5000);
                req->PageSizeHint = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_batch_size",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args[0]);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_objects_recursively_",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());

    test_s3_accessor(*accessor);
}

TEST_F(S3AccessorTest, gcs) {
    std::shared_ptr<S3Accessor> accessor;
    auto config = test_resource(ObjectStoreInfoPB::GCP);
    ASSERT_TRUE(config.has_value());
    ASSERT_EQ(S3Accessor::create(std::move(*config), &accessor), 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "S3ObjStorageClient::list_objects",
            [](auto&& args) {
                auto* req = try_any_cast<Aws::S3::Model::ListObjectsV2Request*>(args[0]);
                req->SetMaxKeys(7);
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_batch_size",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args[0]);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_objects_recursively_",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());

    test_s3_accessor(*accessor);
}

TEST_F(S3AccessorTest, path_style_test) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_prefix("doris-debug-instance-prefix");
    obj_info.set_provider(ObjectStoreInfoPB_Provider_S3);
    obj_info.set_ak("dummy_ak");
    obj_info.set_sk("dummy_sk");
    obj_info.set_endpoint("dummy-bucket");
    obj_info.set_region("cn-north-1");
    obj_info.set_bucket("dummy-bucket");
    config::max_s3_client_retry = 0;

    auto* sp = SyncPoint::get_instance();
    sp->enable_processing();
    std::vector<SyncPoint::CallbackGuard> guards;

    std::string base_domain = "s3.cn-north-1.amazonaws.com.cn";
    std::string domain_ip = "54.222.51.71"; // first ip of base_domain
    // to test custom_domain,  add ${domain_ip} ${custom_domain} to /etc/hosts
    // otherwise the related cases will fail
    std::string custom_domain = "gavin.s3.aws.com";
    // clang-format off
    // http code 403 means there is nothing wrong the given domain in objinfo
    //                 domain, use_path_style, http_code
    std::vector<std::tuple<std::string, bool, int>> inputs {
        {base_domain               , false , 403}, // works
        {base_domain               , true  , 403}, // works
        {"http://" + base_domain   , false , 403}, // works
        {"http://" + base_domain   , true  , 403}, // works
        {"https://" + base_domain  , false , 403}, // works
        {"https://" + base_domain  , true  , 403}, // works
        {"http://" + domain_ip     , false , 301}, // works, ip with virtual addressing
        {"http://" + domain_ip     , true  , 301}, // works, ip with path style
        {custom_domain             , false , -1} , // custom_domain could not resolve with virtual addressing
        {custom_domain             , true  , 403}, // custom_domain working with path style
        {"http://" + custom_domain , false , -1} , // custom_domain could not resolve with virtual addressing
        {"https://" + custom_domain, true  , -1},  // certificate issue, custom_domain does not attached with any certs
        // {"https://54.222.51.71" , false , -1} , // certificate issue
        // {"https://54.222.51.71" , true  , -1} , // certificate issue
    };

    int case_idx = 0;
    sp->set_call_back("S3ObjClient::delete_object",
            [&case_idx, &inputs](auto&& args) {
                auto* res = try_any_cast<Aws::S3::Model::DeleteObjectOutcome*>(args[0]);
                EXPECT_EQ(std::get<2>(inputs[case_idx]), static_cast<int>(res->GetError().GetResponseCode())) << "<<<<<<<<<<<<<<<<<<<<< " << case_idx;
                case_idx++;
            },
            &guards.emplace_back());
    // clang-format on

    for (auto& i : inputs) {
        obj_info.set_endpoint(std::get<0>(i));
        obj_info.set_use_path_style(std::get<1>(i));
        auto s3_conf = parse_recycler_storage_config(obj_info);
        ASSERT_TRUE(s3_conf);
        EXPECT_EQ(std::get<AwsObjStorageConfig>(s3_conf->client).use_path_style,
                  obj_info.use_path_style())
                << case_idx;
        std::shared_ptr<S3Accessor> accessor;
        int ret = S3Accessor::create(*s3_conf, &accessor);
        EXPECT_EQ(ret, 0) << "<<<<<<<<<<<<<<<<<<<<< " << case_idx;
        ret = accessor->init();
        EXPECT_EQ(ret, 0) << "<<<<<<<<<<<<<<<<<<<<< " << case_idx;
        // this function call will trigger syncpoint callback to increment case_idx
        accessor->delete_file("abc"); // try to delete a nonexisted file, ignore the result
        // EXPECT_EQ(ret, exp) << "<<<<<<<<<<<<<<<<<<<<< " << case_idx << " domain " << std::get<0>(i);
    }
}

TEST(S3AccessorConfigTest, PreservesS3CompatibleVaultProvider) {
    ObjectStoreInfoPB info;
    info.set_bucket("bucket");
    info.set_endpoint("endpoint");
    info.set_region("region");
    for (const auto& [provider, expected] :
         std::initializer_list<std::pair<ObjectStoreInfoPB::Provider, ObjStorageProvider>> {
                 {ObjectStoreInfoPB::S3, ObjStorageProvider::AWS},
                 {ObjectStoreInfoPB::OSS, ObjStorageProvider::OSS},
                 {ObjectStoreInfoPB::COS, ObjStorageProvider::COS},
                 {ObjectStoreInfoPB::OBS, ObjStorageProvider::OBS},
                 {ObjectStoreInfoPB::BOS, ObjStorageProvider::BOS},
                 {ObjectStoreInfoPB::GCP, ObjStorageProvider::GCP},
                 {ObjectStoreInfoPB::TOS, ObjStorageProvider::TOS}}) {
        info.set_provider(provider);
        const auto config = parse_recycler_storage_config(info);
        ASSERT_TRUE(config.has_value());
        EXPECT_EQ(storage_provider(config->client), expected);
    }
}

TEST(S3AccessorConfigTest, InfersProviderForLegacyVault) {
    ObjectStoreInfoPB info;
    info.set_endpoint("oss-cn-hangzhou.aliyuncs.com");
    info.set_bucket("bucket");
    info.set_region("cn-hangzhou");
    auto config = parse_recycler_storage_config(info);
    ASSERT_TRUE(config.has_value());
    EXPECT_TRUE(std::holds_alternative<OssObjStorageConfig>(config->client));
    info.set_provider(ObjectStoreInfoPB::S3);
    config = parse_recycler_storage_config(info);
    ASSERT_TRUE(config.has_value());
    EXPECT_TRUE(std::holds_alternative<AwsObjStorageConfig>(config->client));
}

TEST(S3AccessorConfigTest, DecryptsTheCredentialSelectedForTheStage) {
    constexpr int64_t key_id = 791;
    const std::string master_key(32, 'a');
    ASSERT_TRUE(global_encryption_key_info_map.emplace(key_id, master_key).second);
    DORIS_CLOUD_DEFER {
        global_encryption_key_info_map.erase(key_id);
    };

    ObjectStoreInfoPB vault;
    vault.set_provider(ObjectStoreInfoPB::S3);
    vault.set_bucket("bucket");
    vault.set_endpoint("objects.example.org");
    vault.set_region("region");
    vault.set_sse_enabled(true);
    vault.mutable_encryption_info()->set_key_id(key_id);
    vault.mutable_encryption_info()->set_encryption_method("AES_256_ECB");
    AkSkPair ciphertext;
    ASSERT_EQ(encrypt_ak_sk({"stage-ak", "stage-sk"}, "AES_256_ECB", master_key, &ciphertext), 0);
    vault.set_ak(ciphertext.first);
    vault.set_sk(ciphertext.second);
    auto resource = parse_recycler_storage_config(vault);
    ASSERT_TRUE(resource);
    const auto& aws = std::get<AwsObjStorageConfig>(resource->client);
    EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(aws.auth.source).access_key, "stage-ak");
    EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(aws.auth.source).secret_key, "stage-sk");
    EXPECT_TRUE(resource->sse_enabled);
    EXPECT_FALSE(resource->is_internal_bucket);

    RamUserPB ram_user;
    EXPECT_FALSE(parse_recycler_storage_config(vault, &ram_user));
    ram_user.mutable_encryption_info()->CopyFrom(vault.encryption_info());
    ASSERT_EQ(encrypt_ak_sk({"ram-ak", "ram-sk"}, "AES_256_ECB", master_key, &ciphertext), 0);
    ram_user.set_ak(ciphertext.first);
    ram_user.set_sk(ciphertext.second);
    // A BUCKET_ACL stage is independent of the stage's own credential and decryption key.
    vault.mutable_encryption_info()->set_key_id(key_id + 1);
    EXPECT_FALSE(parse_recycler_storage_config(vault));
    resource = parse_recycler_storage_config(vault, &ram_user);
    ASSERT_TRUE(resource);
    const auto& source = std::get<AwsObjStorageConfig>(resource->client).auth.source;
    EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(source).access_key, "ram-ak");
    EXPECT_EQ(std::get<AccessKeyObjStorageAuth>(source).secret_key, "ram-sk");
}

TEST(S3AccessorConfigTest, AppliesRecyclerPoolAndLegacyRoleDefaults) {
    ObjectStoreInfoPB vault;
    vault.set_provider(ObjectStoreInfoPB::OSS);
    vault.set_bucket("bucket");
    vault.set_endpoint("https://objects.example.org");
    vault.set_region("region");
    vault.set_role_arn("arn:aws:iam::123456789012:role/old-vault");
    auto resource = parse_recycler_storage_config(vault);
    ASSERT_TRUE(resource);
    const auto& oss = std::get<OssObjStorageConfig>(resource->client);
    const auto& auth = std::get<S3CredentialChain>(oss.auth);
    EXPECT_EQ(std::get<AwsCredentialChainType>(auth.source),
              AwsCredentialChainType::InstanceProfile);
    ASSERT_TRUE(auth.assume_role);
    EXPECT_EQ(auth.assume_role->role_arn, vault.role_arn());
    EXPECT_FALSE(auth.assume_role->request_timeout);
    const auto workers = static_cast<int64_t>(config::recycle_pool_parallelism) +
                         config::instance_recycler_worker_pool_size;
    EXPECT_EQ(oss.connection.max_connections,
              std::max<int64_t>(default_s3_max_connections(), workers));
    EXPECT_EQ(oss.connection.connect_timeout, std::chrono::seconds(5));
    EXPECT_EQ(oss.connection.request_timeout, std::chrono::seconds(30));
    EXPECT_EQ(oss.connection.max_retries, config::max_s3_client_retry);
}

TEST(S3AccessorConfigTest, GcpCredentialFromObjectStoreInfo) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB::GCP);
    obj_info.set_endpoint("storage.googleapis.com");
    obj_info.set_region("us-east1");
    obj_info.set_bucket("test-bucket");
    auto* gcp_credential = obj_info.mutable_credential()->mutable_gcp_credential();
    gcp_credential->set_credential_provider_type(GcpCredentialPB::COMPUTE_ENGINE);
    gcp_credential->set_impersonation_service_account("target@my-project.iam.gserviceaccount.com");

    auto s3_conf = parse_recycler_storage_config(obj_info);
    ASSERT_TRUE(s3_conf.has_value());
    ASSERT_TRUE(std::holds_alternative<GcpObjStorageConfig>(s3_conf->client));
    const auto& auth = std::get<GcpObjStorageConfig>(s3_conf->client).auth;
    ASSERT_TRUE(std::holds_alternative<GcpNativeObjStorageAuth>(auth));
    const auto& credential = std::get<GcpNativeObjStorageAuth>(auth);
    EXPECT_EQ(credential.source, GcpCredentialSource::ComputeEngine);
    EXPECT_EQ(credential.impersonation_service_account,
              "target@my-project.iam.gserviceaccount.com");
}

TEST(S3AccessorConfigTest, ValidateGcpStorageVaultCredential) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB::GCP);
    obj_info.set_endpoint("storage.googleapis.com");
    obj_info.set_region("us-east1");
    obj_info.set_bucket("test-bucket");
    obj_info.mutable_credential()->mutable_gcp_credential()->set_credential_provider_type(
            GcpCredentialPB::DEFAULT);

    EXPECT_FALSE(validate_obj_credential(obj_info).has_value());

    obj_info.set_ak("ak");
    obj_info.set_sk("sk");
    auto error = validate_obj_credential(obj_info);
    ASSERT_TRUE(error.has_value());
    EXPECT_NE(error->find("cannot be combined"), std::string::npos);
}

class S3AccessorRoleTest : public testing::Test {
    static void SetUpTestSuite() {
        if (!std::getenv("AWS_ROLE_ARN") || !std::getenv("AWS_EXTERNAL_ID") ||
            !std::getenv("AWS_ENDPOINT") || !std::getenv("AWS_REGION") ||
            !std::getenv("AWS_BUCKET") || !std::getenv("AWS_PREFIX")) {
            return;
        }

        role_arn = std::getenv("AWS_ROLE_ARN");
        external_id = std::getenv("AWS_EXTERNAL_ID");
        endpoint = std::getenv("AWS_ENDPOINT");
        region = std::getenv("AWS_REGION");
        bucket = std::getenv("AWS_BUCKET");
        prefix = std::getenv("AWS_PREFIX");
    }

    void SetUp() override {
        if (role_arn.empty() || external_id.empty() || endpoint.empty() || region.empty() ||
            bucket.empty() || prefix.empty()) {
            GTEST_SKIP() << "Skipping S3 test, because AWS environment not set";
        }
    }

public:
    static std::string endpoint;
    static std::string region;
    static std::string bucket;
    static std::string prefix;
    static std::string role_arn;
    static std::string external_id;
};

std::string S3AccessorRoleTest::endpoint;
std::string S3AccessorRoleTest::region;
std::string S3AccessorRoleTest::bucket;
std::string S3AccessorRoleTest::prefix;
std::string S3AccessorRoleTest::role_arn;
std::string S3AccessorRoleTest::external_id;

TEST_F(S3AccessorRoleTest, s3) {
    std::shared_ptr<S3Accessor> accessor;
    ObjectStoreInfoPB vault;
    vault.set_endpoint(endpoint);
    vault.set_region(region);
    vault.set_bucket(bucket);
    vault.set_prefix(prefix + "/S3AccessorRoleTest/" + butil::GenerateGUID());
    vault.set_cred_provider_type(CredProviderTypePB::INSTANCE_PROFILE);
    vault.set_role_arn(role_arn);
    vault.set_external_id(external_id);
    vault.set_provider(ObjectStoreInfoPB::S3);
    auto resource = parse_recycler_storage_config(std::move(vault));
    ASSERT_TRUE(resource.has_value());
    ASSERT_EQ(S3Accessor::create(std::move(*resource), &accessor), 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "S3ObjStorageClient::list_objects",
            [](auto&& args) {
                auto* req = try_any_cast<Aws::S3::Model::ListObjectsV2Request*>(args[0]);
                req->SetMaxKeys(7);
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_batch_size",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args[0]);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "ObjStorageClient::delete_objects_recursively_",
            [](auto&& args) {
                auto* delete_batch_size = try_any_cast<size_t*>(args);
                *delete_batch_size = 7;
            },
            &guards.emplace_back());

    test_s3_accessor(*accessor);
}
} // namespace doris::cloud
