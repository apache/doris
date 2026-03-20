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

#include "recycler/oss_accessor.h"

#include <butil/guid.h>
#include <gen_cpp/cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <unordered_set>

#include "common/config.h"
#include "common/configbase.h"
#include "common/logging.h"
#include "cpp/sync_point.h"

using namespace doris;

int main(int argc, char** argv) {
    const std::string conf_file = "doris_cloud.conf";
    if (!cloud::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!cloud::init_glog("oss_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    LOG(INFO) << "oss_accessor_test starting";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {
namespace {

// ---------------------------------------------------------------------------
// Shared test helper — mirrors test_s3_accessor() in s3_accessor_test.cpp
// Exercises every StorageVaultAccessor operation in a deterministic sequence.
// ---------------------------------------------------------------------------
void test_oss_accessor(OSSAccessor& accessor) {
    std::string file1 = "data/10000/1_0.dat";

    // delete_directory with empty path must fail (guard against whole-bucket wipe)
    int ret = accessor.delete_directory("");
    ASSERT_NE(ret, 0);

    // clean slate
    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);

    // put + exists
    ret = accessor.put_file(file1, "");
    ASSERT_EQ(ret, 0);
    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 0);

    // list_directory — with and without trailing slash
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

    // list_directory with non-matching prefix returns empty
    ret = accessor.list_directory("data/100", &iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());

    // delete_file + existence check
    ret = accessor.delete_file(file1);
    ASSERT_EQ(ret, 0);
    ret = accessor.exists(file1);
    ASSERT_EQ(ret, 1); // 1 = not found

    // list_directory with empty path must fail
    ret = accessor.list_directory("", &iter);
    ASSERT_NE(ret, 0);

    // list_all on empty vault
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());

    // delete_file on already-deleted key is idempotent
    ret = accessor.delete_file(file1);
    EXPECT_EQ(ret, 0);

    // -----------------------------------------------------------------------
    // Bulk operations: 5 dirs × 5 files = 25 objects
    // -----------------------------------------------------------------------
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

    // list_all — verify all 25 files present, mtime is recent
    std::unordered_set<std::string> list_files;
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    for (auto f = iter->next(); f.has_value(); f = iter->next()) {
        EXPECT_LT(now - f->mtime_s, 60) << "mtime should be within last 60s: " << f->path;
        list_files.insert(std::move(f->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    // delete_files (batch) — remove 5 files from the back
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
    for (auto f = iter->next(); f.has_value(); f = iter->next()) {
        list_files.insert(std::move(f->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    // delete_directory — remove entire data/10001/ subtree
    std::string to_delete_dir = "data/10001";
    ret = accessor.delete_directory(to_delete_dir);
    ASSERT_EQ(ret, 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& file) { return file.starts_with(to_delete_dir); }),
                files.end());
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto f = iter->next(); f.has_value(); f = iter->next()) {
        list_files.insert(std::move(f->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    // delete_prefix — remove data/10003/ using prefix deletion
    std::string to_delete_prefix = "data/10003/";
    ret = accessor.delete_prefix(to_delete_prefix);
    ASSERT_EQ(ret, 0);
    files.erase(std::remove_if(files.begin(), files.end(),
                               [&](auto&& file) { return file.starts_with(to_delete_prefix); }),
                files.end());
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    list_files.clear();
    for (auto f = iter->next(); f.has_value(); f = iter->next()) {
        list_files.insert(std::move(f->path));
    }
    ASSERT_EQ(list_files.size(), files.size());
    for (auto&& file : files) {
        EXPECT_TRUE(list_files.contains(file));
    }

    // final cleanup
    ret = accessor.delete_all();
    ASSERT_EQ(ret, 0);
    ret = accessor.list_all(&iter);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(iter->has_next());
    ASSERT_FALSE(iter->next());
}

} // namespace

// ---------------------------------------------------------------------------
// OSSAccessorTest — AK/SK static credentials (mirrors S3AccessorTest::s3)
// Skipped when test_oss_ak config is empty.
// ---------------------------------------------------------------------------
class OSSAccessorTest : public testing::Test {
    void SetUp() override {
        if (cloud::config::test_oss_ak.empty()) {
            GTEST_SKIP() << "empty test_oss_ak, skip OSSAccessorTest";
        }
    }
};

TEST_F(OSSAccessorTest, oss_simple_credentials) {
    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(
            OSSConf {
                    .endpoint = config::test_oss_endpoint,
                    .bucket = config::test_oss_bucket,
                    .prefix = config::test_oss_prefix + "/OSSAccessorTest/" + butil::GenerateGUID(),
                    .region = config::test_oss_region,
                    .access_key_id = config::test_oss_ak,
                    .access_key_secret = config::test_oss_sk,
                    .provider_type = OSSCredProviderType::SIMPLE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    // Use SyncPoint to force small page size — exercises pagination logic
    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "OSSListIterator::max_keys",
            [](auto&& args) {
                auto* max_keys = try_any_cast<int*>(args[0]);
                *max_keys = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "OSSAccessor::delete_objects_batch_size",
            [](auto&& args) {
                auto* batch_size = try_any_cast<size_t*>(args[0]);
                *batch_size = 7;
            },
            &guards.emplace_back());

    test_oss_accessor(*accessor);
}

// ---------------------------------------------------------------------------
// OSSAccessorTest — from_obj_store_info with SIMPLE provider
// Mirrors S3AccessorTest::path_style_test for conf construction path.
// ---------------------------------------------------------------------------
TEST_F(OSSAccessorTest, oss_conf_from_obj_store_info) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_ak(config::test_oss_ak);
    obj_info.set_sk(config::test_oss_sk);
    obj_info.set_endpoint(config::test_oss_endpoint);
    obj_info.set_region(config::test_oss_region);
    obj_info.set_bucket(config::test_oss_bucket);
    obj_info.set_prefix(config::test_oss_prefix + "/OSSAccessorTest/conf/" + butil::GenerateGUID());

    auto conf = OSSConf::from_obj_store_info(obj_info);
    ASSERT_TRUE(conf.has_value());
    EXPECT_FALSE(conf->endpoint.empty());
    EXPECT_FALSE(conf->bucket.empty());
    EXPECT_FALSE(conf->region.empty());
    EXPECT_FALSE(conf->access_key_id.empty());
    EXPECT_FALSE(conf->access_key_secret.empty());

    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(*conf, &accessor);
    ASSERT_EQ(ret, 0);

    // Basic smoke test: put → exists → delete → not-exists
    const std::string test_file = "smoke/conf_test.dat";
    ret = accessor->put_file(test_file, "hello");
    ASSERT_EQ(ret, 0);
    ret = accessor->exists(test_file);
    ASSERT_EQ(ret, 0);
    ret = accessor->delete_file(test_file);
    ASSERT_EQ(ret, 0);
    ret = accessor->exists(test_file);
    ASSERT_EQ(ret, 1);

    ret = accessor->delete_all();
    ASSERT_EQ(ret, 0);
}

// ---------------------------------------------------------------------------
// OSSAccessorTest — skip_aksk flag (used for logging paths)
// ---------------------------------------------------------------------------
TEST_F(OSSAccessorTest, oss_conf_skip_aksk) {
    ObjectStoreInfoPB obj_info;
    obj_info.set_provider(ObjectStoreInfoPB_Provider_OSS);
    obj_info.set_ak(config::test_oss_ak);
    obj_info.set_sk(config::test_oss_sk);
    obj_info.set_endpoint(config::test_oss_endpoint);
    obj_info.set_region(config::test_oss_region);
    obj_info.set_bucket(config::test_oss_bucket);
    obj_info.set_prefix(config::test_oss_prefix);

    auto conf = OSSConf::from_obj_store_info(obj_info, /*skip_aksk=*/true);
    ASSERT_TRUE(conf.has_value());
    EXPECT_TRUE(conf->access_key_id.empty()) << "skip_aksk=true must not populate access_key_id";
    EXPECT_TRUE(conf->access_key_secret.empty())
            << "skip_aksk=true must not populate access_key_secret";
    EXPECT_FALSE(conf->endpoint.empty());
    EXPECT_FALSE(conf->bucket.empty());
}

// ---------------------------------------------------------------------------
// OSSAccessorRoleTest — AssumeRole via ECS instance profile + STS
// Mirrors S3AccessorRoleTest::s3 exactly.
// Environment variables:
//   OSS_ROLE_ARN, OSS_EXTERNAL_ID, OSS_ENDPOINT, OSS_REGION, OSS_BUCKET,
//   OSS_PREFIX
// ---------------------------------------------------------------------------
class OSSAccessorRoleTest : public testing::Test {
    static void SetUpTestSuite() {
        if (!std::getenv("OSS_ROLE_ARN") || !std::getenv("OSS_EXTERNAL_ID") ||
            !std::getenv("OSS_ENDPOINT") || !std::getenv("OSS_REGION") ||
            !std::getenv("OSS_BUCKET") || !std::getenv("OSS_PREFIX")) {
            return;
        }
        role_arn = std::getenv("OSS_ROLE_ARN");
        external_id = std::getenv("OSS_EXTERNAL_ID");
        endpoint = std::getenv("OSS_ENDPOINT");
        region = std::getenv("OSS_REGION");
        bucket = std::getenv("OSS_BUCKET");
        prefix = std::getenv("OSS_PREFIX");
    }

    void SetUp() override {
        if (role_arn.empty() || external_id.empty() || endpoint.empty() || region.empty() ||
            bucket.empty() || prefix.empty()) {
            GTEST_SKIP() << "Skipping OSS AssumeRole test, OSS environment variables not set";
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

std::string OSSAccessorRoleTest::endpoint;
std::string OSSAccessorRoleTest::region;
std::string OSSAccessorRoleTest::bucket;
std::string OSSAccessorRoleTest::prefix;
std::string OSSAccessorRoleTest::role_arn;
std::string OSSAccessorRoleTest::external_id;

TEST_F(OSSAccessorRoleTest, oss_assume_role) {
    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(
            OSSConf {
                    .endpoint = endpoint,
                    .bucket = bucket,
                    .prefix = prefix + "/OSSAccessorRoleTest/" + butil::GenerateGUID(),
                    .region = region,
                    .role_arn = role_arn,
                    .external_id = external_id,
                    .provider_type = OSSCredProviderType::INSTANCE_PROFILE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "OSSListIterator::max_keys",
            [](auto&& args) {
                auto* max_keys = try_any_cast<int*>(args[0]);
                *max_keys = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "OSSAccessor::delete_objects_batch_size",
            [](auto&& args) {
                auto* batch_size = try_any_cast<size_t*>(args[0]);
                *batch_size = 7;
            },
            &guards.emplace_back());

    test_oss_accessor(*accessor);
}

// AssumeRole without external_id (single-account scenario)
TEST_F(OSSAccessorRoleTest, oss_assume_role_no_external_id) {
    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(
            OSSConf {
                    .endpoint = endpoint,
                    .bucket = bucket,
                    .prefix = prefix + "/OSSAccessorRoleTestNoExtId/" + butil::GenerateGUID(),
                    .region = region,
                    .role_arn = role_arn,
                    .external_id = "", // intentionally empty
                    .provider_type = OSSCredProviderType::INSTANCE_PROFILE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    // Smoke: put → exists → delete
    const std::string test_file = "smoke/no_ext_id.dat";
    ret = accessor->put_file(test_file, "");
    ASSERT_EQ(ret, 0);
    ret = accessor->exists(test_file);
    ASSERT_EQ(ret, 0);
    ret = accessor->delete_file(test_file);
    ASSERT_EQ(ret, 0);
    ret = accessor->delete_all();
    ASSERT_EQ(ret, 0);
}

// ---------------------------------------------------------------------------
// OSSAccessorInstanceProfileTest — ECS instance profile (no AK/SK, no role)
// Environment variables: OSS_ENDPOINT, OSS_REGION, OSS_BUCKET, OSS_PREFIX
// ---------------------------------------------------------------------------
class OSSAccessorInstanceProfileTest : public testing::Test {
    static void SetUpTestSuite() {
        if (!std::getenv("OSS_ENDPOINT") || !std::getenv("OSS_REGION") ||
            !std::getenv("OSS_BUCKET") || !std::getenv("OSS_PREFIX")) {
            return;
        }
        endpoint = std::getenv("OSS_ENDPOINT");
        region = std::getenv("OSS_REGION");
        bucket = std::getenv("OSS_BUCKET");
        prefix = std::getenv("OSS_PREFIX");
    }

    void SetUp() override {
        if (endpoint.empty() || region.empty() || bucket.empty() || prefix.empty()) {
            GTEST_SKIP() << "Skipping OSS instance profile test, OSS environment variables not set";
        }
    }

public:
    static std::string endpoint;
    static std::string region;
    static std::string bucket;
    static std::string prefix;
};

std::string OSSAccessorInstanceProfileTest::endpoint;
std::string OSSAccessorInstanceProfileTest::region;
std::string OSSAccessorInstanceProfileTest::bucket;
std::string OSSAccessorInstanceProfileTest::prefix;

TEST_F(OSSAccessorInstanceProfileTest, oss_instance_profile) {
    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(
            OSSConf {
                    .endpoint = endpoint,
                    .bucket = bucket,
                    .prefix = prefix + "/OSSAccessorIPTest/" + butil::GenerateGUID(),
                    .region = region,
                    .provider_type = OSSCredProviderType::INSTANCE_PROFILE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "OSSListIterator::max_keys",
            [](auto&& args) {
                auto* max_keys = try_any_cast<int*>(args[0]);
                *max_keys = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "OSSAccessor::delete_objects_batch_size",
            [](auto&& args) {
                auto* batch_size = try_any_cast<size_t*>(args[0]);
                *batch_size = 7;
            },
            &guards.emplace_back());

    test_oss_accessor(*accessor);
}

// ---------------------------------------------------------------------------
// OSSAccessorDeletePrefixWithExpirationTest
// Verifies that delete_prefix with expiration_time only removes old objects.
// ---------------------------------------------------------------------------
class OSSAccessorDeletePrefixExpirationTest : public testing::Test {
    void SetUp() override {
        if (cloud::config::test_oss_ak.empty()) {
            GTEST_SKIP() << "empty test_oss_ak, skip OSSAccessorDeletePrefixExpirationTest";
        }
    }
};

TEST_F(OSSAccessorDeletePrefixExpirationTest, delete_prefix_with_expiration) {
    std::shared_ptr<OSSAccessor> accessor;
    int ret = OSSAccessor::create(
            OSSConf {
                    .endpoint = config::test_oss_endpoint,
                    .bucket = config::test_oss_bucket,
                    .prefix = config::test_oss_prefix + "/OSSDeletePrefixExpTest/" +
                              butil::GenerateGUID(),
                    .region = config::test_oss_region,
                    .access_key_id = config::test_oss_ak,
                    .access_key_secret = config::test_oss_sk,
                    .provider_type = OSSCredProviderType::SIMPLE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    // Put two files under the same prefix
    ret = accessor->put_file("expire/old.dat", "old_content");
    ASSERT_EQ(ret, 0);
    ret = accessor->put_file("expire/new.dat", "new_content");
    ASSERT_EQ(ret, 0);

    // Use a far-future expiration time (year 2099) — nothing should be deleted
    int64_t far_future = std::chrono::duration_cast<std::chrono::seconds>(
                                 std::chrono::system_clock::time_point::max().time_since_epoch())
                                 .count() /
                         2; // safely in the future
    ret = accessor->delete_prefix("expire/", far_future);
    ASSERT_EQ(ret, 0);

    // Both files must still exist
    ret = accessor->exists("expire/old.dat");
    EXPECT_EQ(ret, 0) << "old.dat should still exist (expiration time in future)";
    ret = accessor->exists("expire/new.dat");
    EXPECT_EQ(ret, 0) << "new.dat should still exist (expiration time in future)";

    // Use epoch=0 (past) — all files under the prefix should be deleted
    ret = accessor->delete_prefix("expire/", 1 /*epoch second 1, far in past*/);
    ASSERT_EQ(ret, 0);

    ret = accessor->exists("expire/old.dat");
    EXPECT_EQ(ret, 1) << "old.dat should be deleted (expired)";
    ret = accessor->exists("expire/new.dat");
    EXPECT_EQ(ret, 1) << "new.dat should be deleted (expired)";

    ret = accessor->delete_all();
    ASSERT_EQ(ret, 0);
}

} // namespace doris::cloud
