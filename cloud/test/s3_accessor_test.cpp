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

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>

#include "common/configbase.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "mock_accessor.h"

using namespace doris;

std::unique_ptr<cloud::MockS3Accessor> _mock_fs;

class S3ClientInterface {
public:
    S3ClientInterface() = default;
    virtual ~S3ClientInterface() = default;
    virtual Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(
            const Aws::S3::Model::ListObjectsV2Request& req) = 0;
    virtual Aws::S3::Model::DeleteObjectsOutcome DeleteObjects(
            const Aws::S3::Model::DeleteObjectsRequest& req) = 0;
    virtual Aws::S3::Model::DeleteObjectOutcome DeleteObject(
            const Aws::S3::Model::DeleteObjectRequest& req) = 0;
    virtual Aws::S3::Model::PutObjectOutcome PutObject(
            const Aws::S3::Model::PutObjectRequest& req) = 0;
    virtual Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& req) = 0;
    virtual Aws::S3::Model::GetBucketLifecycleConfigurationOutcome GetBucketLifecycleConfiguration(
            const Aws::S3::Model::GetBucketLifecycleConfigurationRequest& req) = 0;
    virtual Aws::S3::Model::GetBucketVersioningOutcome GetBucketVersioning(
            const Aws::S3::Model::GetBucketVersioningRequest& req) = 0;
};

static bool list_object_v2_with_expire_time = false;
static int64_t expire_time = 0;
static bool set_bucket_lifecycle = false;
static bool set_bucket_versioning_status_error = false;

class S3Client : public S3ClientInterface {
public:
    S3Client() = default;
    ~S3Client() override = default;
    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(
            const Aws::S3::Model::ListObjectsV2Request& req) override {
        auto prefix = req.GetPrefix();
        auto continuation_token =
                req.ContinuationTokenHasBeenSet() ? req.GetContinuationToken() : "";
        bool truncated = true;
        std::vector<cloud::ObjectMeta> files;
        size_t num = 0;
        do {
            _mock_fs->list(prefix, &files);
            if (num == files.size()) {
                truncated = false;
                break;
            }
            num = files.size();
            auto path1 = files.back().path;
            prefix = path1.back() += 1;
        } while (files.size() <= 1000);
        Aws::S3::Model::ListObjectsV2Result result;
        result.SetIsTruncated(truncated);
        std::vector<Aws::S3::Model::Object> objects;
        std::for_each(files.begin(), files.end(), [&](const cloud::ObjectMeta& file) {
            Aws::S3::Model::Object obj;
            obj.SetKey(file.path);
            Aws::Utils::DateTime date;
            if (list_object_v2_with_expire_time) {
                date = Aws::Utils::DateTime(expire_time);
            }
            obj.SetLastModified(date);
            objects.emplace_back(std::move(obj));
        });
        result.SetContents(std::move(objects));
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(result));
    }

    Aws::S3::Model::DeleteObjectsOutcome DeleteObjects(
            const Aws::S3::Model::DeleteObjectsRequest& req) override {
        Aws::S3::Model::DeleteObjectsResult result;
        const auto& deletes = req.GetDelete();
        for (const auto& obj : deletes.GetObjects()) {
            _mock_fs->delete_object(obj.GetKey());
        }
        return Aws::S3::Model::DeleteObjectsOutcome(std::move(result));
    }

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(
            const Aws::S3::Model::DeleteObjectRequest& req) override {
        Aws::S3::Model::DeleteObjectResult result;
        _mock_fs->delete_object(req.GetKey());
        return Aws::S3::Model::DeleteObjectOutcome(std::move(result));
    }

    Aws::S3::Model::PutObjectOutcome PutObject(
            const Aws::S3::Model::PutObjectRequest& req) override {
        Aws::S3::Model::PutObjectResult result;
        const auto& key = req.GetKey();
        _mock_fs->put_object(key, "");
        return Aws::S3::Model::PutObjectOutcome(std::move(result));
    }

    Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& req) override {
        Aws::S3::Model::HeadObjectResult result;
        const auto& key = req.GetKey();
        auto v = _mock_fs->exist(key);
        if (v == 1) {
            auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::RESOURCE_NOT_FOUND, false);
            err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);

            return Aws::S3::Model::HeadObjectOutcome(std::move(err));
        }
        return Aws::S3::Model::HeadObjectOutcome(std::move(result));
    }

    Aws::S3::Model::GetBucketLifecycleConfigurationOutcome GetBucketLifecycleConfiguration(
            const Aws::S3::Model::GetBucketLifecycleConfigurationRequest& req) override {
        Aws::S3::Model::GetBucketLifecycleConfigurationResult result;
        Aws::Vector<Aws::S3::Model::LifecycleRule> rules;
        if (set_bucket_lifecycle) {
            Aws::S3::Model::LifecycleRule rule;
            Aws::S3::Model::NoncurrentVersionExpiration expiration;
            expiration.SetNoncurrentDays(1000);
            rule.SetNoncurrentVersionExpiration(expiration);
            rules.emplace_back(std::move(rule));
        }
        result.SetRules(std::move(rules));
        return Aws::S3::Model::GetBucketLifecycleConfigurationOutcome(std::move(result));
    }

    Aws::S3::Model::GetBucketVersioningOutcome GetBucketVersioning(
            const Aws::S3::Model::GetBucketVersioningRequest& req) override {
        Aws::S3::Model::GetBucketVersioningResult result;
        if (set_bucket_versioning_status_error) {
            result.SetStatus(Aws::S3::Model::BucketVersioningStatus::Suspended);
        } else {
            result.SetStatus(Aws::S3::Model::BucketVersioningStatus::Enabled);
        }
        return Aws::S3::Model::GetBucketVersioningOutcome(std::move(result));
    }
};

static bool return_error_for_error_s3_client = false;
static bool delete_objects_return_part_error = false;

class ErrorS3Client : public S3ClientInterface {
public:
    ErrorS3Client() : _correct_impl(std::make_unique<S3Client>()) {}
    ~ErrorS3Client() override = default;
    Aws::S3::Model::ListObjectsV2Outcome ListObjectsV2(
            const Aws::S3::Model::ListObjectsV2Request& req) override {
        if (!return_error_for_error_s3_client) {
            return _correct_impl->ListObjectsV2(req);
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(err));
    }

    Aws::S3::Model::DeleteObjectsOutcome DeleteObjects(
            const Aws::S3::Model::DeleteObjectsRequest& req) override {
        if (!delete_objects_return_part_error) {
            Aws::S3::Model::DeleteObjectsResult result;
            Aws::Vector<Aws::S3::Model::Error> errors;
            Aws::S3::Model::Error error;
            errors.emplace_back(std::move(error));
            result.SetErrors(std::move(errors));
            return Aws::S3::Model::DeleteObjectsOutcome(std::move(result));
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        // return -1
        return Aws::S3::Model::DeleteObjectsOutcome(std::move(err));
    }

    Aws::S3::Model::DeleteObjectOutcome DeleteObject(
            const Aws::S3::Model::DeleteObjectRequest& req) override {
        if (!return_error_for_error_s3_client) {
            return _correct_impl->DeleteObject(req);
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        // return -1
        return Aws::S3::Model::DeleteObjectOutcome(std::move(err));
    }

    Aws::S3::Model::PutObjectOutcome PutObject(
            const Aws::S3::Model::PutObjectRequest& req) override {
        if (!return_error_for_error_s3_client) {
            return _correct_impl->PutObject(req);
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);
        return Aws::S3::Model::PutObjectOutcome(std::move(err));
    }

    Aws::S3::Model::HeadObjectOutcome HeadObject(
            const Aws::S3::Model::HeadObjectRequest& req) override {
        if (!return_error_for_error_s3_client) {
            return _correct_impl->HeadObject(req);
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR);
        return Aws::S3::Model::HeadObjectOutcome(std::move(err));
    }

    Aws::S3::Model::GetBucketLifecycleConfigurationOutcome GetBucketLifecycleConfiguration(
            const Aws::S3::Model::GetBucketLifecycleConfigurationRequest& req) override {
        if (!return_error_for_error_s3_client) {
            return _correct_impl->GetBucketLifecycleConfiguration(req);
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR);
        return Aws::S3::Model::GetBucketLifecycleConfigurationOutcome(std::move(err));
    }

    Aws::S3::Model::GetBucketVersioningOutcome GetBucketVersioning(
            const Aws::S3::Model::GetBucketVersioningRequest& req) override {
        if (!return_error_for_error_s3_client) {
            return _correct_impl->GetBucketVersioning(req);
        }
        auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::RESOURCE_NOT_FOUND,
                                                            false);
        err.SetResponseCode(Aws::Http::HttpResponseCode::INTERNAL_SERVER_ERROR);
        return Aws::S3::Model::GetBucketVersioningOutcome(std::move(err));
    }

private:
    std::unique_ptr<S3ClientInterface> _correct_impl;
};

class MockS3Client {
public:
    MockS3Client(std::unique_ptr<S3ClientInterface> impl = std::make_unique<S3Client>())
            : _impl(std::move(impl)) {}
    auto ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request& req) {
        return _impl->ListObjectsV2(req);
    }

    auto DeleteObjects(const Aws::S3::Model::DeleteObjectsRequest& req) {
        return _impl->DeleteObjects(req);
    }

    auto DeleteObject(const Aws::S3::Model::DeleteObjectRequest& req) {
        return _impl->DeleteObject(req);
    }

    auto PutObject(const Aws::S3::Model::PutObjectRequest& req) { return _impl->PutObject(req); }

    auto HeadObject(const Aws::S3::Model::HeadObjectRequest& req) { return _impl->HeadObject(req); }

    auto GetBucketLifecycleConfiguration(
            const Aws::S3::Model::GetBucketLifecycleConfigurationRequest& req) {
        return _impl->GetBucketLifecycleConfiguration(req);
    }

    auto GetBucketVersioning(const Aws::S3::Model::GetBucketVersioningRequest& req) {
        return _impl->GetBucketVersioning(req);
    }

private:
    std::unique_ptr<S3ClientInterface> _impl;
};

std::unique_ptr<MockS3Client> _mock_client;

struct MockCallable {
    std::string point_name;
    std::function<void(void*)> func;
};

static auto callbacks = std::array {
        MockCallable {"s3_client::list_objects_v2",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::ListObjectsV2Outcome*,
                                                  Aws::S3::Model::ListObjectsV2Request*>*)p;
                          *pair.first = (*_mock_client).ListObjectsV2(*pair.second);
                      }},
        MockCallable {"s3_client::delete_objects",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::DeleteObjectsOutcome*,
                                                  Aws::S3::Model::DeleteObjectsRequest*>*)p;
                          *pair.first = (*_mock_client).DeleteObjects(*pair.second);
                      }},
        MockCallable {"s3_client::delete_object",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::DeleteObjectOutcome*,
                                                  Aws::S3::Model::DeleteObjectRequest*>*)p;
                          *pair.first = (*_mock_client).DeleteObject(*pair.second);
                      }},
        MockCallable {"s3_client::put_object",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::PutObjectOutcome*,
                                                  Aws::S3::Model::PutObjectRequest*>*)p;
                          *pair.first = (*_mock_client).PutObject(*pair.second);
                      }},
        MockCallable {"s3_client::head_object",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::HeadObjectOutcome*,
                                                  Aws::S3::Model::HeadObjectRequest*>*)p;
                          *pair.first = (*_mock_client).HeadObject(*pair.second);
                      }},
        MockCallable {
                "s3_client::get_bucket_lifecycle_configuration",
                [](void* p) {
                    auto pair =
                            *(std::pair<Aws::S3::Model::GetBucketLifecycleConfigurationOutcome*,
                                        Aws::S3::Model::GetBucketLifecycleConfigurationRequest*>*)p;
                    *pair.first = (*_mock_client).GetBucketLifecycleConfiguration(*pair.second);
                }},
        MockCallable {"s3_client::get_bucket_versioning", [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::GetBucketVersioningOutcome*,
                                                  Aws::S3::Model::GetBucketVersioningRequest*>*)p;
                          *pair.first = (*_mock_client).GetBucketVersioning(*pair.second);
                      }}};

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
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace doris::cloud {

std::string get_key(const std::string& relative_path) {
    return fmt::format("/{}", relative_path);
}

void create_file_under_prefix(std::string_view prefix, size_t file_nums) {
    for (size_t i = 0; i < file_nums; i++) {
        _mock_fs->put_object(get_key(fmt::format("{}{}", prefix, i)), "");
    }
}

TEST(S3AccessorTest, init) {
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    ASSERT_EQ(0, accessor->init());
}

TEST(S3AccessorTest, check_bucket_versioning) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        set_bucket_versioning_status_error = false;
    });
    { ASSERT_EQ(0, accessor->check_bucket_versioning()); }
    {
        set_bucket_versioning_status_error = true;
        ASSERT_EQ(-1, accessor->check_bucket_versioning());
    }
}

TEST(S3AccessorTest, check_bucket_versioning_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    return_error_for_error_s3_client = true;
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
    });
    ASSERT_EQ(-1, accessor->check_bucket_versioning());
}

TEST(S3AccessorTest, get_bucket_lifecycle) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        set_bucket_lifecycle = false;
    });
    {
        int64_t expiration_time = 0;
        ASSERT_EQ(-1, accessor->get_bucket_lifecycle(&expiration_time));
    }
    {
        set_bucket_lifecycle = true;
        int64_t expiration_time = 0;
        ASSERT_EQ(0, accessor->get_bucket_lifecycle(&expiration_time));
    }
}

TEST(S3AccessorTest, get_bucket_lifecycle_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    return_error_for_error_s3_client = true;
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
    });
    int64_t expiration_time = 0;
    ASSERT_EQ(-1, accessor->get_bucket_lifecycle(&expiration_time));
}

TEST(S3AccessorTest, list) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    create_file_under_prefix("test_list", 300);
    std::vector<ObjectMeta> files;
    ASSERT_EQ(0, accessor->list("test_list", &files));
    ASSERT_EQ(300, files.size());
}

TEST(S3AccessorTest, list_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    return_error_for_error_s3_client = true;
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
    });
    create_file_under_prefix("test_list", 300);
    std::vector<ObjectMeta> files;
    ASSERT_EQ(-1, accessor->list("test_list", &files));
}

TEST(S3AccessorTest, put) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_put";
    for (size_t i = 0; i < 300; i++) {
        ASSERT_EQ(0, accessor->put_object(fmt::format("{}{}", prefix, i), ""));
    }
    std::vector<ObjectMeta> files;
    ASSERT_EQ(0, accessor->list("test_put", &files));
    ASSERT_EQ(300, files.size());
}

TEST(S3AccessorTest, put_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
    });
    std::string prefix = "test_put_error";
    for (size_t i = 0; i < 300; i++) {
        if (i % 2) {
            return_error_for_error_s3_client = true;
            ASSERT_EQ(-1, accessor->put_object(fmt::format("{}{}", prefix, i), ""));
            return_error_for_error_s3_client = false;
            break;
        }
        ASSERT_EQ(0, accessor->put_object(fmt::format("{}{}", prefix, i), ""));
    }
    std::vector<ObjectMeta> files;
    ASSERT_EQ(0, accessor->list("test_put_error", &files));
}

TEST(S3AccessorTest, exist) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_exist";
    ASSERT_EQ(1, accessor->exist(prefix));
    ASSERT_EQ(0, accessor->put_object(prefix, ""));
    ASSERT_EQ(0, accessor->exist(prefix));
}

TEST(S3AccessorTest, exist_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
    });
    std::string prefix = "test_exist_error";
    ASSERT_EQ(1, accessor->exist(prefix));
    ASSERT_EQ(0, accessor->put_object(prefix, ""));
    return_error_for_error_s3_client = true;
    ASSERT_EQ(-1, accessor->exist(prefix));
}

TEST(S3AccessorTest, delete_object) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_object";
    create_file_under_prefix(prefix, 200);
    for (size_t i = 0; i < 200; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(0, accessor->delete_object(path));
        ASSERT_EQ(1, accessor->exist(path));
    }
}

TEST(S3AccessorTest, gcs_delete_objects) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<GcsAccessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_object";
    std::vector<std::string> paths;
    size_t num = 300;
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        _mock_fs->put_object(path, "");
        paths.emplace_back(std::move(path));
    }
    ASSERT_EQ(0, accessor->delete_objects(paths));
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(1, accessor->exist(path));
    }
}

TEST(S3AccessorTest, gcs_delete_objects_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<GcsAccessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
    });
    std::string prefix = "test_delete_objects";
    std::vector<std::string> paths_first_half;
    std::vector<std::string> paths_second_half;
    size_t num = 300;
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        _mock_fs->put_object(path, "");
        if (i < 150) {
            paths_first_half.emplace_back(std::move(path));
        } else {
            paths_second_half.emplace_back(std::move(path));
        }
    }
    std::vector<std::string> empty;
    ASSERT_EQ(0, accessor->delete_objects(empty));
    return_error_for_error_s3_client = true;
    ASSERT_EQ(-1, accessor->delete_objects(paths_first_half));
}

TEST(S3AccessorTest, delete_objects) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_objects";
    std::vector<std::string> paths;
    size_t num = 300;
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        _mock_fs->put_object(path, "");
        paths.emplace_back(std::move(path));
    }
    ASSERT_EQ(0, accessor->delete_objects(paths));
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(1, accessor->exist(path));
    }
}

TEST(S3AccessorTest, delete_objects_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
        delete_objects_return_part_error = false;
    });
    std::string prefix = "test_delete_objects";
    std::vector<std::string> paths_first_half;
    std::vector<std::string> paths_second_half;
    size_t num = 300;
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        _mock_fs->put_object(path, "");
        if (i < 150) {
            paths_first_half.emplace_back(std::move(path));
        } else {
            paths_second_half.emplace_back(std::move(path));
        }
    }
    std::vector<std::string> empty;
    ASSERT_EQ(0, accessor->delete_objects(empty));
    return_error_for_error_s3_client = true;
    delete_objects_return_part_error = true;
    ASSERT_EQ(-1, accessor->delete_objects(paths_first_half));
    delete_objects_return_part_error = false;
    ASSERT_EQ(-2, accessor->delete_objects(paths_second_half));
}

TEST(S3AccessorTest, delete_expired_objects) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    {
        std::string prefix = "atest_delete_expired_objects";
        size_t num = 2000;
        create_file_under_prefix(prefix, num);
        list_object_v2_with_expire_time = true;
        expire_time = 50;
        ASSERT_EQ(0, accessor->delete_expired_objects(prefix, 100));
        for (size_t i = 0; i < num; i++) {
            auto path = fmt::format("{}{}", prefix, i);
            ASSERT_EQ(1, accessor->exist(path));
        }
    }
    {
        std::string prefix = "btest_delete_expired_objects";
        size_t num = 2000;
        create_file_under_prefix(prefix, num);
        list_object_v2_with_expire_time = true;
        expire_time = 150;
        ASSERT_EQ(0, accessor->delete_expired_objects(prefix, 100));
        for (size_t i = 0; i < num; i++) {
            auto path = fmt::format("{}{}", prefix, i);
            ASSERT_EQ(1, accessor->exist(path));
        }
    }
    {
        std::string prefix = "ctest_delete_expired_objects";
        size_t num = 2000;
        create_file_under_prefix(prefix, num);
        list_object_v2_with_expire_time = true;
        expire_time = 150;
        return_error_for_error_s3_client = true;
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [&](int*) { return_error_for_error_s3_client = false; });
        ASSERT_EQ(0, accessor->delete_expired_objects(prefix, 100));
    }
}

TEST(S3AccessorTest, delete_object_by_prefix) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_objects_by_prefix";
    size_t num = 2000;
    create_file_under_prefix(prefix, num);
    ASSERT_EQ(0, accessor->delete_objects_by_prefix(prefix));
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(1, accessor->exist(path));
    }
}

TEST(S3AccessorTest, delete_object_by_prefix_error) {
    _mock_fs = std::make_unique<cloud::MockS3Accessor>(cloud::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>(std::make_unique<ErrorS3Client>());
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
        return_error_for_error_s3_client = false;
        delete_objects_return_part_error = false;
    });
    std::string prefix = "test_delete_objects_by_prefix";
    size_t num = 2000;
    create_file_under_prefix(prefix, num);
    delete_objects_return_part_error = true;
    return_error_for_error_s3_client = true;
    ASSERT_EQ(-1, accessor->delete_objects_by_prefix(prefix));
    return_error_for_error_s3_client = false;
    ASSERT_EQ(-2, accessor->delete_objects_by_prefix(prefix));
    delete_objects_return_part_error = false;
    ASSERT_EQ(-3, accessor->delete_objects_by_prefix(prefix));
}

} // namespace doris::cloud
