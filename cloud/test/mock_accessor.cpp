
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

#include "mock_accessor.h"

#include <glog/logging.h>

#include <iterator>
#include <ranges>
#include <vector>

#include "common/logging.h"
#include "common/string_util.h"
#include "cpp/sync_point.h"
#include "recycler/storage_vault_accessor.h"

namespace doris::cloud {

class MockListIterator final : public ListIterator {
public:
    MockListIterator(std::vector<std::string> entries) : entries_(std::move(entries)) {}
    ~MockListIterator() override = default;

    bool is_valid() override { return true; }

    bool has_next() override { return !entries_.empty(); }

    std::optional<FileMeta> next() override {
        std::optional<FileMeta> ret;
        if (has_next()) {
            ret = FileMeta {.path = std::move(entries_.back())};
            entries_.pop_back();
        }

        return ret;
    }

private:
    std::vector<std::string> entries_;
};

MockAccessor::MockAccessor() : StorageVaultAccessor(AccessorType::MOCK) {
    uri_ = "mock";
}

MockAccessor::~MockAccessor() = default;

auto MockAccessor::get_prefix_range(const std::string& path_prefix) {
    auto begin = objects_.lower_bound(path_prefix);
    if (begin == objects_.end()) {
        return std::make_pair(begin, begin);
    }

    auto path1 = path_prefix;
    path1.back() += 1;
    auto end = objects_.lower_bound(path1);
    return std::make_pair(begin, end);
}

int MockAccessor::delete_prefix_impl(const std::string& path_prefix) {
    TEST_SYNC_POINT("MockAccessor::delete_prefix");
    LOG(INFO) << "delete object of prefix=" << path_prefix;
    std::lock_guard lock(mtx_);

    auto [begin, end] = get_prefix_range(path_prefix);
    if (begin == end) {
        return 0;
    }

    objects_.erase(begin, end);
    return 0;
}

int MockAccessor::delete_prefix(const std::string& path_prefix, int64_t expiration_time) {
    auto norm_path_prefix = path_prefix;
    strip_leading(norm_path_prefix, "/");
    if (norm_path_prefix.empty()) {
        LOG_WARNING("invalid dir_path {}", path_prefix);
        return -1;
    }

    return delete_prefix_impl(norm_path_prefix);
}

int MockAccessor::delete_directory(const std::string& dir_path) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    return delete_prefix_impl(!norm_dir_path.ends_with('/') ? norm_dir_path + '/' : norm_dir_path);
}

int MockAccessor::delete_all(int64_t expiration_time) {
    std::lock_guard lock(mtx_);
    objects_.clear();
    return 0;
}

int MockAccessor::delete_files(const std::vector<std::string>& paths) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("MockAccessor::delete_files", (int)0);

    for (auto&& path : paths) {
        delete_file(path);
    }
    return 0;
}

int MockAccessor::delete_file(const std::string& path) {
    LOG(INFO) << "delete object path=" << path;
    std::lock_guard lock(mtx_);
    objects_.erase(path);
    return 0;
}

int MockAccessor::put_file(const std::string& path, const std::string& content) {
    std::lock_guard lock(mtx_);
    objects_.insert(path);
    return 0;
}

int MockAccessor::list_all(std::unique_ptr<ListIterator>* res) {
    std::vector<std::string> entries;

    {
        std::lock_guard lock(mtx_);
        entries.reserve(objects_.size());
        entries.assign(objects_.rbegin(), objects_.rend());
    }

    *res = std::make_unique<MockListIterator>(std::move(entries));

    return 0;
}

int MockAccessor::list_directory(const std::string& dir_path, std::unique_ptr<ListIterator>* res) {
    auto norm_dir_path = dir_path;
    strip_leading(norm_dir_path, "/");
    if (norm_dir_path.empty()) {
        LOG_WARNING("invalid dir_path {}", dir_path);
        return -1;
    }

    std::vector<std::string> entries;

    {
        std::lock_guard lock(mtx_);
        auto [begin, end] = get_prefix_range(norm_dir_path);
        if (begin != end) {
            entries.reserve(std::distance(begin, end));
            std::ranges::copy(std::ranges::subrange(begin, end) | std::ranges::views::reverse,
                              std::back_inserter(entries));
        }
    }

    *res = std::make_unique<MockListIterator>(std::move(entries));

    return 0;
}

int MockAccessor::exists(const std::string& path) {
    std::lock_guard lock(mtx_);
    return !objects_.contains(path);
}

} // namespace doris::cloud
