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

#include <iterator>
#include <map>
#include <mutex>
#include <ranges>
#include <set>
#include <vector>

#include "common/logging.h"
#include "common/string_util.h"
#include "cpp/sync_point.h"
#include "mock_accessor.h"
#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {
class MockListIterator final : public ListIterator {
public:
    MockListIterator(std::vector<FileMeta> entries) : entries_(std::move(entries)) {}
    ~MockListIterator() override = default;

    bool is_valid() override { return true; }

    bool has_next() override { return !entries_.empty(); }

    std::optional<FileMeta> next() override {
        std::optional<FileMeta> ret;
        if (has_next()) {
            ret = std::move(entries_.back());
            entries_.pop_back();
        }

        return ret;
    }

private:
    std::vector<FileMeta> entries_;
};

class MockAccessor final : public StorageVaultAccessor {
public:
    explicit MockAccessor();

    ~MockAccessor() override;

    int delete_prefix(const std::string& path_prefix, int64_t expiration_time = 0) override;

    int delete_directory(const std::string& dir_path) override;

    int delete_all(int64_t expiration_time = 0) override;

    int delete_files(const std::vector<std::string>& paths) override;

    int delete_file(const std::string& path) override;

    int list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) override;

    int list_all(std::unique_ptr<ListIterator>* res) override;

    int put_file(const std::string& path, const std::string& content) override;

    int exists(const std::string& path) override;

    int abort_multipart_upload(const std::string& path, const std::string& upload_id) override;

    // Put an object with the given modification time (seconds); put_file() uses 0.
    int put_file_with_mtime(const std::string& path, int64_t mtime_s);

private:
    // expiration_time > 0 keeps the objects modified after it, as S3Accessor does.
    int delete_prefix_impl(const std::string& path_prefix, int64_t expiration_time = 0);

    auto get_prefix_range(const std::string& path_prefix);

    // Requires mtx_.
    std::vector<FileMeta> to_file_metas(auto&& paths) const {
        std::vector<FileMeta> metas;
        for (const auto& path : paths) {
            auto it = mtimes_.find(path);
            metas.push_back({.path = path, .mtime_s = it == mtimes_.end() ? 0 : it->second});
        }
        return metas;
    }

    std::mutex mtx_;
    std::set<std::string> objects_;
    // Modification times of the objects put by put_file_with_mtime(); others are 0.
    std::map<std::string, int64_t> mtimes_;
};

inline MockAccessor::MockAccessor() : StorageVaultAccessor(AccessorType::MOCK) {
    uri_ = "mock";
}

inline MockAccessor::~MockAccessor() = default;

inline auto MockAccessor::get_prefix_range(const std::string& path_prefix) {
    auto begin = objects_.lower_bound(path_prefix);
    if (begin == objects_.end()) {
        return std::make_pair(begin, begin);
    }

    auto path1 = path_prefix;
    path1.back() += 1;
    auto end = objects_.lower_bound(path1);
    return std::make_pair(begin, end);
}

inline int MockAccessor::delete_prefix_impl(const std::string& path_prefix,
                                            int64_t expiration_time) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("MockAccessor::delete_prefix", (int)0, &path_prefix);
    LOG(INFO) << "delete object of prefix=" << path_prefix;
    std::lock_guard lock(mtx_);

    auto [begin, end] = get_prefix_range(path_prefix);
    while (begin != end) {
        auto mtime = mtimes_.find(*begin);
        if (expiration_time > 0 && mtime != mtimes_.end() && mtime->second > expiration_time) {
            ++begin;
            continue;
        }
        if (mtime != mtimes_.end()) {
            mtimes_.erase(mtime);
        }
        begin = objects_.erase(begin);
    }
    return 0;
}

inline int MockAccessor::delete_prefix(const std::string& path_prefix, int64_t expiration_time) {
    auto norm_path_prefix = path_prefix;
    strip_leading(norm_path_prefix, "/");
    if (norm_path_prefix.empty()) {
        LOG_WARNING("invalid dir_path {}", path_prefix);
        return -1;
    }

    return delete_prefix_impl(norm_path_prefix, expiration_time);
}

inline int MockAccessor::delete_directory(const std::string& dir_path) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    return delete_prefix_impl(!norm_dir_path.ends_with('/') ? norm_dir_path + '/' : norm_dir_path);
}

inline int MockAccessor::delete_all(int64_t expiration_time) {
    std::lock_guard lock(mtx_);
    objects_.clear();
    mtimes_.clear();
    return 0;
}

inline int MockAccessor::delete_files(const std::vector<std::string>& paths) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("MockAccessor::delete_files", (int)0, &paths);

    for (auto&& path : paths) {
        delete_file(path);
    }
    return 0;
}

inline int MockAccessor::delete_file(const std::string& path) {
    LOG(INFO) << "delete object path=" << path;
    std::lock_guard lock(mtx_);
    objects_.erase(path);
    mtimes_.erase(path);
    return 0;
}

inline int MockAccessor::put_file(const std::string& path, const std::string& content) {
    std::lock_guard lock(mtx_);
    objects_.insert(path);
    mtimes_.erase(path);
    return 0;
}

inline int MockAccessor::put_file_with_mtime(const std::string& path, int64_t mtime_s) {
    std::lock_guard lock(mtx_);
    objects_.insert(path);
    mtimes_[path] = mtime_s;
    return 0;
}

inline int MockAccessor::list_all(std::unique_ptr<ListIterator>* res) {
    std::vector<FileMeta> entries;

    {
        std::lock_guard lock(mtx_);
        entries = to_file_metas(objects_ | std::ranges::views::reverse);
    }

    *res = std::make_unique<MockListIterator>(std::move(entries));

    return 0;
}

inline int MockAccessor::list_directory(const std::string& dir_path,
                                        std::unique_ptr<ListIterator>* res) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    std::vector<FileMeta> entries;

    {
        std::lock_guard lock(mtx_);
        auto [begin, end] = get_prefix_range(norm_dir_path);
        entries = to_file_metas(std::ranges::subrange(begin, end) | std::ranges::views::reverse);
    }

    *res = std::make_unique<MockListIterator>(std::move(entries));

    return 0;
}

inline int MockAccessor::exists(const std::string& path) {
    std::lock_guard lock(mtx_);
    return !objects_.contains(path);
}

inline int MockAccessor::abort_multipart_upload(const std::string& path,
                                                const std::string& upload_id) {
    return 0;
}

} // namespace doris::cloud
