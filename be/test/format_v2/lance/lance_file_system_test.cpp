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

#include "format_v2/lance/lance_file_system.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <string>
#include <vector>

#include "testutil/mock/obj_storage_client_test_stub.h"
#include "util/s3_util.h"

namespace doris::format::lance {
namespace {

class HostStorageClient final : public io::ObjStorageClientTestStub {
public:
    std::map<std::string, std::string> objects;
    int heads = 0;
    int gets = 0;
    std::string last_key;
    ObjStorageHeadResult head_object(const ObjStoragePath& path) override {
        ++heads;
        last_key = path.key;
        auto it = objects.find(path.key);
        if (it == objects.end())
            return {.resp = {.status = {TStatusCode::NOT_FOUND, "absent"}}, .file_size = 0};
        return {.resp = ObjStorageResponse::OK(),
                .file_size = static_cast<long long>(it->second.size())};
    }
    ObjStorageResponse get_object(const ObjStoragePath& path, void* buffer, size_t offset,
                                  size_t size, size_t* count) override {
        ++gets;
        last_key = path.key;
        auto& value = objects.at(path.key);
        *count = std::min(size, value.size() - offset);
        std::memcpy(buffer, value.data() + offset, *count);
        return ObjStorageResponse::OK();
    }
    ObjStorageResponse list_objects(const ObjStoragePath& path,
                                    std::vector<ObjectMeta>* out) override {
        for (const auto& [key, value] : objects) {
            if (key.starts_with(path.prefix))
                out->push_back({.key = key, .size = static_cast<int64_t>(value.size())});
        }
        return ObjStorageResponse::OK();
    }
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath&) override { return {}; }
    ObjStorageResponse put_object(const ObjStoragePath&, std::string_view) override { return {}; }
    ObjStorageUploadResult upload_part(const ObjStoragePath&, const std::string&, std::string_view,
                                       int) override {
        return {};
    }
    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath&, const std::string&,
            const std::vector<ObjStorageCompletedPart>&) override {
        return {};
    }
    ObjStorageResponse delete_objects(const ObjStoragePath&, std::vector<std::string>) override {
        return {};
    }
    ObjStorageResponse delete_object(const ObjStoragePath&) override { return {}; }
    std::string generate_presigned_url(const ObjStoragePath&, int64_t) override { return {}; }

protected:
    ObjStorageListPageResult list_objects_page(const ObjStoragePath&, std::string_view) override {
        return {};
    }
};

TEST(LanceFileSystemTest, RemoteRangesPreserveObjectKeysAndReuseOnlyImmutableSizes) {
    auto client = std::make_shared<HostStorageClient>();
    client->objects["dataset/data/a +%?#.lance"] = "remote bytes";
    client->objects["dataset/_latest.manifest"] = "manifest";
    S3ClientConf received;
    auto& factory = S3ClientFactory::instance();
    factory.set_client_creator_for_test([&](const S3ClientConf& conf) {
        received = conf;
        return client;
    });
    struct Reset {
        ~Reset() { S3ClientFactory::instance().clear_client_creator_for_test(); }
    } reset;
    const char* options[] {"aws_endpoint", "https://generic.example", "aws_endpoint_url_s3",
                           "https://specific.example", nullptr};
    auto result = LanceFileSystem::create("s3://example-bucket/dataset", options);
    ASSERT_TRUE(result.has_value());
    auto storage = std::move(result).value();
    auto callbacks = storage->options();
    const char* error = nullptr;
    for (int i = 0; i < 2; ++i) {
        void* file = nullptr;
        LanceHostFileMeta meta {};
        ASSERT_EQ(callbacks.open(callbacks.context,
                                 "s3://example-bucket/dataset/data/a%20%2B%25%3F%23.lance", &file,
                                 &meta, &error),
                  0);
        EXPECT_EQ(meta.size, 12);
        uint8_t bytes[6] {};
        size_t count = 0;
        EXPECT_EQ(callbacks.read_at(file, 0, bytes, sizeof(bytes), &count, &error), 0);
        EXPECT_EQ(std::string(reinterpret_cast<char*>(bytes), count), "remote");
        callbacks.close(file);
    }
    EXPECT_EQ(client->heads, 1);
    EXPECT_EQ(client->gets, 2);
    EXPECT_EQ(client->last_key, "dataset/data/a +%?#.lance");
    EXPECT_EQ(received.endpoint, "https://specific.example");
    EXPECT_FALSE(received.use_virtual_addressing);
    for (int i = 0; i < 2; ++i) {
        void* file = nullptr;
        LanceHostFileMeta meta {};
        ASSERT_EQ(callbacks.open(callbacks.context, "s3://example-bucket/dataset/_latest.manifest",
                                 &file, &meta, &error),
                  0);
        callbacks.close(file);
    }
    EXPECT_EQ(client->heads, 3);
    std::vector<std::string> entries;
    ASSERT_EQ(callbacks.list(
                      callbacks.context, "s3://example-bucket/dataset",
                      [](void* context, const char* uri, const LanceHostFileMeta*) -> int32_t {
                          static_cast<std::vector<std::string>*>(context)->emplace_back(uri);
                          return 0;
                      },
                      &entries, &error),
              0);
    EXPECT_EQ(entries.size(), 2);
    EXPECT_NE(std::find(entries.begin(), entries.end(),
                        "s3://example-bucket/dataset/data/a%20%2B%25%3F%23.lance"),
              entries.end());
}

TEST(LanceFileSystemTest, LocalReaderDecodesUrisAndRetainsOpenHandles) {
    const auto root = std::filesystem::temp_directory_path() /
                      ("lance-host-" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(root / "data");
    const auto path = root / "data/a +%?#.lance";
    {
        std::ofstream stream(path);
        stream << "host bytes";
    }
    const auto uri = "file://" + root.string() + "/data/a%20%2B%25%3F%23.lance";
    auto result = LanceFileSystem::create(root.string(), nullptr);
    ASSERT_TRUE(result.has_value());
    auto storage = std::move(result).value();
    const auto callbacks = storage->options();
    void* file = nullptr;
    LanceHostFileMeta meta {};
    const char* error = nullptr;
    ASSERT_EQ(callbacks.open(callbacks.context, uri.c_str(), &file, &meta, &error), 0);
    ASSERT_NE(file, nullptr);
    ASSERT_EQ(meta.size, 10);
    // The file owns the adapter even after the dataset-side owner disappears.
    storage.reset();
    uint8_t bytes[5] {};
    size_t count = 0;
    EXPECT_EQ(callbacks.read_at(file, 5, bytes, sizeof(bytes), &count, &error), 0);
    EXPECT_EQ(count, 5);
    EXPECT_EQ(std::string(reinterpret_cast<char*>(bytes), count), "bytes");
    callbacks.close(file);
    std::filesystem::remove_all(root);
}

TEST(LanceFileSystemTest, ListingAndMissingObjectsUseHostCallbacks) {
    const auto root = std::filesystem::path(__FILE__).parent_path().parent_path() /
                      "table/lance/data/all_types.lance";
    auto result = LanceFileSystem::create(root.string(), nullptr);
    ASSERT_TRUE(result.has_value());
    auto storage = std::move(result).value();
    const auto callbacks = storage->options();
    const char* error = nullptr;
    std::vector<std::string> files;
    EXPECT_EQ(callbacks.list(
                      callbacks.context, ("file://" + root.string()).c_str(),
                      [](void* context, const char* uri, const LanceHostFileMeta*) -> int32_t {
                          static_cast<std::vector<std::string>*>(context)->emplace_back(uri);
                          return 0;
                      },
                      &files, &error),
              0);
    EXPECT_FALSE(files.empty());
    void* file = nullptr;
    LanceHostFileMeta meta {};
    EXPECT_EQ(callbacks.open(callbacks.context, ("file://" + root.string() + "/absent").c_str(),
                             &file, &meta, &error),
              1);
    EXPECT_EQ(file, nullptr);
    EXPECT_EQ(callbacks.open(callbacks.context, "gs://example-bucket/dataset/file", &file, &meta,
                             &error),
              2);
    EXPECT_EQ(file, nullptr);
    EXPECT_EQ(callbacks.open(callbacks.context, "file:///invalid%ZZ", &file, &meta, &error), 2);
    EXPECT_EQ(callbacks.open(callbacks.context, "file://remote/path", &file, &meta, &error), 2);
}

TEST(LanceFileSystemTest, CacheNamespaceIncludesStorageOriginAndOptions) {
    const char* options[] {"aws_endpoint", "https://storage.example", nullptr};
    auto first = LanceFileSystem::create("s3://example-bucket/dataset", nullptr);
    auto second = LanceFileSystem::create("s3://example-bucket/dataset", options);
    auto repeat = LanceFileSystem::create("s3://example-bucket/dataset", options);
    ASSERT_TRUE(first.has_value() && second.has_value() && repeat.has_value());
    EXPECT_NE(std::string(first.value()->options().cache_namespace),
              second.value()->options().cache_namespace);
    EXPECT_EQ(std::string(second.value()->options().cache_namespace),
              repeat.value()->options().cache_namespace);
    const char* malformed[] {"aws_endpoint", nullptr};
    EXPECT_FALSE(LanceFileSystem::create("s3://example-bucket/dataset", malformed).has_value());
}

} // namespace
} // namespace doris::format::lance
