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

#include <azure/storage/blobs/blob_options.hpp>
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

    if (cloud::config::test_s3_ak.empty()) {
        std::cout << "empty test_s3_ak, skip S3AccessorTest" << std::endl;
        return 0;
    }

    if (!cloud::init_glog("s3_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {
namespace {

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

TEST(S3AccessorTest, s3) {
    std::shared_ptr<S3Accessor> accessor;
    int ret = S3Accessor::create(
            S3Conf {
                    .ak = config::test_s3_ak,
                    .sk = config::test_s3_sk,
                    .endpoint = config::test_s3_endpoint,
                    .region = config::test_s3_region,
                    .bucket = config::test_s3_bucket,
                    .prefix = config::test_s3_prefix + "/S3AccessorTest/" + butil::GenerateGUID(),
                    .provider = S3Conf::S3,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "S3ObjListIterator",
            [](auto&& args) {
                auto* req = try_any_cast<Aws::S3::Model::ListObjectsV2Request*>(args[0]);
                req->SetMaxKeys(7);
            },
            &guards.emplace_back());
    sp->set_call_back(
            "S3ObjClient::delete_objects",
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

TEST(S3AccessorTest, azure) {
    std::shared_ptr<S3Accessor> accessor;
    int ret = S3Accessor::create(
            S3Conf {
                    .ak = config::test_s3_ak,
                    .sk = config::test_s3_sk,
                    .endpoint = config::test_s3_endpoint,
                    .region = config::test_s3_region,
                    .bucket = config::test_s3_bucket,
                    .prefix = config::test_s3_prefix + "/S3AccessorTest/" + butil::GenerateGUID(),
                    .provider = S3Conf::AZURE,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "AzureListIterator",
            [](auto&& args) {
                auto* req = try_any_cast<Azure::Storage::Blobs::ListBlobsOptions*>(args[0]);
                req->PageSizeHint = 7;
            },
            &guards.emplace_back());
    sp->set_call_back(
            "AzureObjClient::delete_objects",
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

TEST(S3AccessorTest, gcs) {
    std::shared_ptr<S3Accessor> accessor;
    int ret = S3Accessor::create(
            S3Conf {
                    .ak = config::test_s3_ak,
                    .sk = config::test_s3_sk,
                    .endpoint = config::test_s3_endpoint,
                    .region = config::test_s3_region,
                    .bucket = config::test_s3_bucket,
                    .prefix = config::test_s3_prefix + "/S3AccessorTest/" + butil::GenerateGUID(),
                    .provider = S3Conf::GCS,
            },
            &accessor);
    ASSERT_EQ(ret, 0);

    auto* sp = SyncPoint::get_instance();
    std::vector<SyncPoint::CallbackGuard> guards;
    sp->set_call_back(
            "S3ObjListIterator",
            [](auto&& args) {
                auto* req = try_any_cast<Aws::S3::Model::ListObjectsV2Request*>(args[0]);
                req->SetMaxKeys(7);
            },
            &guards.emplace_back());
    sp->set_call_back(
            "S3ObjClient::delete_objects",
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

TEST(S3AccessorTest, path_style_test) {
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
        auto s3_conf = S3Conf::from_obj_store_info(obj_info);
        EXPECT_EQ(s3_conf->use_virtual_addressing, !obj_info.use_path_style()) << case_idx;
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

} // namespace doris::cloud
