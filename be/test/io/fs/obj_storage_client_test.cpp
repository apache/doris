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

#include "cpp/obj-client/obj_storage_client.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

namespace doris {
namespace {

class FakeObjStorageClient final : public ObjStorageClient {
public:
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath&) override {
        ++calls;
        return {};
    }

    ObjStorageResponse put_object(const ObjStoragePath&, std::string_view) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageUploadResult upload_part(const ObjStoragePath&, const std::string&, std::string_view,
                                       int) override {
        ++calls;
        return {};
    }

    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath&, const std::string&,
            const std::vector<ObjStorageCompletedPart>&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageHeadResult head_object(const ObjStoragePath&) override {
        ++calls;
        return {};
    }

    ObjStorageResponse get_object(const ObjStoragePath&, void*, size_t, size_t,
                                  size_t* size_return) override {
        ++calls;
        *size_return = 4;
        return ObjStorageResponse::OK();
    }

    ObjStorageListPageResult list_objects_page(const ObjStoragePath&,
                                               std::string_view continuation_token) override {
        ++calls;
        ++list_page_calls;
        if (list_pages.empty()) {
            return {};
        }
        const size_t index =
                continuation_token.empty()
                        ? 0
                        : static_cast<size_t>(std::stoull(std::string(continuation_token)));
        ObjStorageListPageResult page {.resp = ObjStorageResponse::OK()};
        page.objects = list_pages[index];
        page.has_more = index + 1 < list_pages.size();
        if (page.has_more) {
            page.continuation_token = std::to_string(index + 1);
        }
        return page;
    }

    ObjStorageResponse delete_objects(const ObjStoragePath&, std::vector<std::string>) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse delete_object(const ObjStoragePath&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageCapabilities capabilities() const override { return {.max_delete_batch = 2}; }

    std::string generate_presigned_url(const ObjStoragePath&, int64_t) override {
        ++presigned_url_calls;
        return "url";
    }

    ObjStorageResponse get_lifecycle(const std::string&, int64_t*) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse check_versioning(const std::string&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    ObjStorageResponse abort_multipart_upload(const ObjStoragePath&, const std::string&) override {
        ++calls;
        return ObjStorageResponse::OK();
    }

    int calls = 0;
    int presigned_url_calls = 0;
    int list_page_calls = 0;
    std::vector<std::vector<ObjectMeta>> list_pages;
};

TEST(ObjStorageClientTest, SupportsLazyAndEagerListing) {
    auto client = std::make_shared<FakeObjStorageClient>();
    client->list_pages = {
            {{.key = "first"}, {.key = "second"}},
            {{.key = "third"}},
    };
    ObjStoragePath opts {.bucket = "bucket", .prefix = "prefix"};

    auto iter = list_objects(client, opts);
    EXPECT_EQ(client->list_page_calls, 0);
    auto first = iter->next();
    ASSERT_TRUE(first.object.has_value());
    EXPECT_EQ(first.object->key, "first");
    EXPECT_EQ(client->list_page_calls, 1);
    auto second = iter->next();
    ASSERT_TRUE(second.object.has_value());
    EXPECT_EQ(second.object->key, "second");
    EXPECT_EQ(client->list_page_calls, 1);
    auto third = iter->next();
    ASSERT_TRUE(third.object.has_value());
    EXPECT_EQ(third.object->key, "third");
    EXPECT_EQ(client->list_page_calls, 2);

    std::vector<ObjectMeta> objects;
    EXPECT_TRUE(client->list_objects(opts, &objects).ok());
    ASSERT_EQ(objects.size(), 3);
    EXPECT_EQ(objects[0].key, "first");
    EXPECT_EQ(objects[1].key, "second");
    EXPECT_EQ(objects[2].key, "third");
    EXPECT_EQ(client->list_page_calls, 4);
}

} // namespace
} // namespace doris
