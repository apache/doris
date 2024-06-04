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

#pragma once

#include <glog/logging.h>

#include <mutex>
#include <set>

#include "common/sync_point.h"
#include "recycler/s3_accessor.h"

namespace doris::cloud {

class MockS3Accessor final : public S3Accessor {
public:
    explicit MockS3Accessor(const S3Conf& conf) : S3Accessor(conf) {}
    ~MockS3Accessor() override = default;

    const std::string& path() const override { return path_; }

    // returns 0 for success otherwise error
    int init() override { return 0; }

    // returns 0 for success otherwise error
    int delete_objects_by_prefix(const std::string& relative_path) override {
        TEST_SYNC_POINT_CALLBACK("MockS3Accessor::delete_objects_by_prefix", nullptr);
        LOG(INFO) << "delete object of prefix=" << relative_path;
        std::lock_guard lock(mtx_);
        if (relative_path.empty()) {
            objects_.clear();
            return 0;
        }
        auto begin = objects_.lower_bound(relative_path);
        if (begin == objects_.end()) {
            return 0;
        }
        auto path1 = relative_path;
        path1.back() += 1;
        auto end = objects_.lower_bound(path1);
        objects_.erase(begin, end);
        return 0;
    }

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override {
        TEST_SYNC_POINT_CALLBACK("MockS3Accessor::delete_objects", nullptr);
        {
            [[maybe_unused]] int ret = -1;
            TEST_SYNC_POINT_RETURN_WITH_VALUE("MockS3Accessor::delete_objects_ret", &ret);
        }
        for (auto& path : relative_paths) {
            LOG(INFO) << "delete object path=" << path;
        }
        std::lock_guard lock(mtx_);
        for (auto& path : relative_paths) {
            objects_.erase(path);
        }
        return 0;
    }

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path) override {
        LOG(INFO) << "delete object path=" << relative_path;
        std::lock_guard lock(mtx_);
        objects_.erase(relative_path);
        return 0;
    }

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override {
        std::lock_guard lock(mtx_);
        objects_.insert(relative_path);
        return 0;
    }

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<ObjectMeta>* paths) override {
        std::lock_guard lock(mtx_);
        if (relative_path.empty()) {
            for (const auto& obj : objects_) {
                paths->push_back({obj});
            }
            return 0;
        }
        auto begin = objects_.lower_bound(relative_path);
        if (begin == objects_.end()) {
            return 0;
        }
        auto path1 = relative_path;
        path1.back() += 1;
        auto end = objects_.lower_bound(path1);
        for (auto it = begin; it != end; ++it) {
            paths->push_back({*it});
        }
        return 0;
    }

    int exist(const std::string& relative_path) override {
        std::lock_guard lock(mtx_);
        return !objects_.count(relative_path);
    }

    // delete objects which last modified time is less than the input expired time and under the input relative path
    // returns 0 for success otherwise error
    int delete_expired_objects(const std::string& relative_path, int64_t expired_time) override {
        return 0;
    }

    int get_bucket_lifecycle(int64_t* expiration_days) override {
        *expiration_days = 7;
        return 0;
    }

    int check_bucket_versioning() override { return 0; }

private:
    std::string path_;

    std::mutex mtx_;
    std::set<std::string> objects_; // store objects' relative path
};

} // namespace doris::cloud
