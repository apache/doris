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

#include "io/fs/azure_obj_storage_client.h"

#include <gtest/gtest.h>

#include "io/fs/file_system.h"
#include "io/fs/obj_storage_client.h"
#include "util/s3_util.h"

#ifdef USE_AZURE
#include <aws/core/utils/HashingUtils.h>

#include <azure/core/http/transport.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>

#include "core/block/block.h"
#include "core/data_type/data_type_file.h"
#include "core/data_type/data_type_string.h"
#include "core/field.h"
#include "core/value/file_value.h"
#include "exec/common/data_gen_functions/vlist_file_tvf.h"
#include "exprs/function/function_to_file.h"
#include "io/fs/azure_obj_storage_client.h"
#include "io/fs/s3_file_system.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_query_context.h"
#include "testutil/mock/mock_runtime_state.h"
#endif

namespace doris {

#ifdef USE_AZURE

using namespace Azure::Storage::Blobs;

class AzureListTransport : public Azure::Core::Http::HttpTransport {
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request& request,
                                                         const Azure::Core::Context&) override {
        const auto params = request.GetUrl().GetQueryParameters();
        EXPECT_EQ(Azure::Core::Url::Decode(params.at("prefix")), "tree/");
        const bool hierarchical = params.contains("delimiter");
        if (hierarchical) {
            EXPECT_EQ(Azure::Core::Url::Decode(params.at("delimiter")), "/");
        }
        const bool next = params.contains("marker");
        if (next) {
            EXPECT_EQ(Azure::Core::Url::Decode(params.at("marker")), "next-page");
        }
        auto blob = [](const std::string& name) {
            return "<Blob><Name>" + name +
                   "</Name><Properties><Last-Modified>Mon, 12 Oct 2026 00:00:00 "
                   "GMT</Last-Modified>"
                   "<Etag>&quot;opaque-2&quot;</Etag><Content-Length>7</Content-Length>"
                   "<BlobType>BlockBlob</BlobType></Properties></Blob>";
        };
        _body = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
                "<EnumerationResults ServiceEndpoint=\"https://account.blob.core.windows.net/\""
                " ContainerName=\"container\"><Prefix>tree/</Prefix><Blobs>";
        _body += blob(next ? "tree/b.txt" : "tree/a.txt");
        if (!next) {
            _body += hierarchical ? "<BlobPrefix><Name>tree/sub/</Name></BlobPrefix>"
                                  : blob("tree/sub/deep.txt");
        }
        _body += std::string("</Blobs><NextMarker>") + (next ? "" : "next-page") +
                 "</NextMarker></EnumerationResults>";
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
                1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
        response->SetHeader("content-type", "application/xml");
        response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(
                reinterpret_cast<const uint8_t*>(_body.data()), _body.size()));
        ++requests;
        return response;
    }
    std::string _body;
    size_t requests = 0;
};

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest assertions expand to branches.
TEST(AzureObjStorageClientListTest, NativeHierarchyListingKeepsOnlyDirectBlobsAndPages) {
    auto transport = std::make_shared<AzureListTransport>();
    BlobClientOptions options;
    options.Transport.Transport = transport;
    auto container = std::make_shared<BlobContainerClient>(
            "https://account.blob.core.windows.net/container", options);
    auto client = std::make_shared<io::AzureObjStorageClient>(container);
    for (const std::string delimiter : {"/", ""}) {
        std::vector<io::ObjectMeta> objects;
        std::string token;
        do {
            auto page =
                    client->list_objects_page({.prefix = "tree/", .delimiter = delimiter}, token);
            ASSERT_TRUE(page.resp.ok()) << page.resp.status.msg;
            objects.insert(objects.end(), page.objects.begin(), page.objects.end());
            token = page.has_more ? page.continuation_token : std::string();
        } while (!token.empty());
        ASSERT_EQ(objects.size(), delimiter.empty() ? 3 : 2);
        EXPECT_EQ(objects.front().key, "tree/a.txt");
        EXPECT_EQ(objects.back().key, "tree/b.txt");
        if (delimiter.empty()) {
            EXPECT_EQ(objects[1].key, "tree/sub/deep.txt");
        }
        for (const auto& object : objects) {
            ASSERT_TRUE(object.etag.has_value());
            EXPECT_EQ(*object.etag, "\"opaque-2\"");
            EXPECT_EQ(file_checksum_from_etag(object.etag), "ETAG:opaque-2");
        }
    }
    EXPECT_EQ(transport->requests, 4);
}

class AzureHeadTransport : public Azure::Core::Http::HttpTransport {
public:
    std::unique_ptr<Azure::Core::Http::RawResponse> Send(Azure::Core::Http::Request& request,
                                                         const Azure::Core::Context&) override {
        EXPECT_EQ(request.GetMethod(), Azure::Core::Http::HttpMethod::Head);
        EXPECT_TRUE(request.GetUrl().GetPath().ends_with("container/payload.bin"));
        auto response = std::make_unique<Azure::Core::Http::RawResponse>(
                1, 1, Azure::Core::Http::HttpStatusCode::Ok, "OK");
        response->SetHeader("content-length", "7");
        response->SetHeader("content-type", "text/csv");
        response->SetHeader("etag", "\"opaque-2\"");
        response->SetHeader("last-modified", "Mon, 12 Oct 2026 00:00:00 GMT");
        response->SetHeader("x-ms-creation-time", "Mon, 12 Oct 2026 00:00:00 GMT");
        response->SetHeader("x-ms-blob-type", "BlockBlob");
        response->SetHeader("x-ms-server-encrypted", "true");
        response->SetBodyStream(std::make_unique<Azure::Core::IO::MemoryBodyStream>(nullptr, 0));
        ++requests;
        return response;
    }
    size_t requests = 0;
};

class AzureFileMetadataTest : public testing::Test {
protected:
    static void SetUpTestSuite() { S3ClientFactory::instance(); }

    std::shared_ptr<io::S3FileSystem> filesystem(
            const std::shared_ptr<Azure::Core::Http::HttpTransport>& transport) {
        BlobClientOptions options;
        options.Transport.Transport = transport;
        auto container = std::make_shared<BlobContainerClient>(
                "https://account.blob.core.windows.net/container", options);
        auto fs = std::shared_ptr<io::S3FileSystem>(new io::S3FileSystem(
                S3Conf {.bucket = "container", .prefix = "", .client_conf = {}},
                "azure-file-test"));
        fs->_client->_client = std::make_shared<io::AzureObjStorageClient>(container);
        return fs;
    }
};

class ToFileWithAzureFilesystem : public FunctionToFile {
public:
    explicit ToFileWithAzureFilesystem(std::shared_ptr<io::FileSystem> filesystem)
            : _filesystem(std::move(filesystem)) {}

    Status create_filesystem(const TS3Resource&, const std::string&,
                             std::shared_ptr<io::FileSystem>* result) const override {
        *result = _filesystem;
        return Status::OK();
    }

private:
    std::shared_ptr<io::FileSystem> _filesystem;
};

TEST_F(AzureFileMetadataTest, HeadMetadataOverridesFilenameInferenceAndProvidesChecksum) {
    auto transport = std::make_shared<AzureHeadTransport>();
    auto fs = filesystem(transport);
    const auto head = fs->client_holder()->get()->head_object({.key = "payload.bin"});
    ASSERT_TRUE(head.resp.ok()) << head.resp.status.msg;
    EXPECT_EQ(head.file_size, 7);
    EXPECT_EQ(head.content_type, "text/csv");
    EXPECT_EQ(head.etag, "\"opaque-2\"");

    auto text = std::make_shared<DataTypeString>();
    auto file = std::make_shared<DataTypeFile>();
    auto resource = text->create_column_const(1, Field::create_field<TYPE_STRING>("resource"));
    auto uri = text->create_column();
    uri->insert(Field::create_field<TYPE_STRING>("s3://container/payload.bin"));
    auto query = MockQueryContext::create();
    query->set_s3_resources({{"resource", TS3Resource {}}});
    MockRuntimeState state;
    state._query_ctx = query.get();
    auto context = FunctionContext::create_context(&state, file, {text, text});
    context->set_constant_cols({std::make_shared<ColumnPtrWrapper>(resource), nullptr});
    ToFileWithAzureFilesystem to_file(fs);
    ASSERT_TRUE(to_file.open(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
    Block block {
            {resource, text, "resource"}, {std::move(uri), text, "uri"}, {nullptr, file, "result"}};
    ASSERT_TRUE(to_file.execute(context.get(), block, {0, 1}, 2, 1).ok());
    const auto value = (*block.get_by_position(2).column)[0].get<TYPE_FILE>();
    EXPECT_EQ(value[2].get<TYPE_BIGINT>(), 7);
    EXPECT_EQ(value[3].get<TYPE_STRING>(), "text/csv");
    ASSERT_FALSE(value[4].is_null());
    EXPECT_EQ(value[4].get<TYPE_STRING>(), "ETAG:opaque-2");
    EXPECT_EQ(transport->requests, 2);
    ASSERT_TRUE(to_file.close(context.get(), FunctionContext::FRAGMENT_LOCAL).ok());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): GTest assertions expand to branches.
TEST_F(AzureFileMetadataTest, ListingEtagsReachFileChecksumsWithoutHeadRequests) {
    for (const bool recursive : {false, true}) {
        auto transport = std::make_shared<AzureListTransport>();
        ObjectPool pool;
        MockRowDescriptor descriptor(
                {std::make_shared<DataTypeString>(), std::make_shared<DataTypeFile>()}, &pool);
        descriptor.tuple_desc_map[0]->slots()[0]->_col_name = "path";
        descriptor.tuple_desc_map[0]->slots()[1]->_col_name = "file";
        VListFileTVF listing(0, descriptor.tuple_desc_map[0]);
        listing._filesystem = filesystem(transport);
        TTVFListFileScanRange params;
        params.uri = "s3://container/tree/";
        params.recursive = recursive;
        TScanRangeParams range;
        range.scan_range.data_gen_scan_range.__set_list_file_params(params);
        ASSERT_TRUE(listing.set_scan_ranges({range}).ok());
        MockRuntimeState state;
        Block block;
        bool eos = false;
        size_t rows = 0;
        while (!eos) {
            ASSERT_TRUE(listing.get_next(&state, &block, &eos).ok());
            for (size_t i = 0; i < block.rows(); ++i) {
                const auto value = (*block.get_by_position(1).column)[i].get<TYPE_FILE>();
                ASSERT_FALSE(value[4].is_null());
                EXPECT_EQ(value[4].get<TYPE_STRING>(), "ETAG:opaque-2");
            }
            rows += block.rows();
            block.clear_column_data();
        }
        EXPECT_EQ(rows, recursive ? 3 : 2);
        EXPECT_EQ(transport->requests, 2);
    }
}

TEST(AzureObjStorageClientTlsHelperTest, detects_tls_ca_error) {
    EXPECT_TRUE(io::is_azure_tls_ca_error_message(
            "Problem with the SSL CA cert (path? access rights?)"));
    EXPECT_TRUE(io::is_azure_tls_ca_error_message(
            "curl error: peer failed verification for cert chain"));
    EXPECT_TRUE(io::is_azure_tls_ca_error_message("unable to get local issuer certificate"));
    EXPECT_FALSE(io::is_azure_tls_ca_error_message("AuthenticationFailed"));
}

TEST(AzureObjStorageClientTlsHelperTest, appends_debug_suffix_only_for_tls_ca_error) {
    std::string_view debug_ctx = "tls_debug(selected_ca_file='/etc/ssl/certs/ca-bundle.crt')";

    EXPECT_EQ(io::build_azure_tls_debug_suffix(
                      "Problem with the SSL CA cert (path? access rights?)", debug_ctx),
              ", tls_debug(selected_ca_file='/etc/ssl/certs/ca-bundle.crt')");
    EXPECT_EQ(io::build_azure_tls_debug_suffix("AuthenticationFailed", debug_ctx), "");
    EXPECT_EQ(io::build_azure_tls_debug_suffix(
                      "Problem with the SSL CA cert (path? access rights?)", ""),
              "");
}

class AzureObjStorageClientTest : public testing::Test {
protected:
    static std::shared_ptr<io::ObjStorageClient> obj_storage_client;

    static void SetUpTestSuite() {
        if (!std::getenv("AZURE_ACCOUNT_NAME") || !std::getenv("AZURE_ACCOUNT_KEY") ||
            !std::getenv("AZURE_CONTAINER_NAME")) {
            return;
        }

        std::string accountName = std::getenv("AZURE_ACCOUNT_NAME");
        std::string accountKey = std::getenv("AZURE_ACCOUNT_KEY");
        std::string containerName = std::getenv("AZURE_CONTAINER_NAME");

        // Initialize Azure SDK
        [[maybe_unused]] auto& s3ClientFactory = S3ClientFactory::instance();

        AzureObjStorageClientTest::obj_storage_client = S3ClientFactory::instance().create(
                {.endpoint = fmt::format("https://{}.blob.core.windows.net", accountName),
                 .region = "dummy-region",
                 .ak = accountName,
                 .sk = accountKey,
                 .token = "",
                 .bucket = containerName,
                 .provider = io::ObjStorageType::AZURE,
                 .role_arn = "",
                 .external_id = ""});
    }

    void SetUp() override {
        if (AzureObjStorageClientTest::obj_storage_client == nullptr) {
            GTEST_SKIP() << "Skipping Azure test, because AZURE environment not set";
        }
    }
};

std::shared_ptr<io::ObjStorageClient> AzureObjStorageClientTest::obj_storage_client = nullptr;

TEST_F(AzureObjStorageClientTest, put_list_delete_object) {
    LOG(INFO) << "AzureObjStorageClientTest::put_list_delete_object";

    auto response = AzureObjStorageClientTest::obj_storage_client->put_object(
            {.key = "AzureObjStorageClientTest/put_list_delete_object"}, std::string("aaaa"));
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    std::vector<io::FileInfo> files;
    // clang-format off
    response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 1);
    files.clear();

    response = AzureObjStorageClientTest::obj_storage_client->delete_object(
            {.key = "AzureObjStorageClientTest/put_list_delete_object"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/put_list_delete_object",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}

TEST_F(AzureObjStorageClientTest, delete_objects_recursively) {
    LOG(INFO) << "AzureObjStorageClientTest::delete_objects_recursively";

    for (int i = 0; i < 22; i++) {
        std::string key =
                "AzureObjStorageClientTest/delete_objects_recursively" + std::to_string(i);

        auto response = AzureObjStorageClientTest::obj_storage_client->put_object(
                {.key = key}, std::string("aaaa"));
        EXPECT_EQ(response.status.code, ErrorCode::OK);
        LOG(INFO) << "put " << key << " OK";
    }

    std::vector<io::FileInfo> files;
    // clang-format off
    auto response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 22);
    files.clear();

    response = AzureObjStorageClientTest::obj_storage_client->delete_objects_recursively(
            {.prefix = "AzureObjStorageClientTest/delete_objects_recursively"});
    EXPECT_EQ(response.status.code, ErrorCode::OK);

    // clang-format off
    response = AzureObjStorageClientTest::obj_storage_client->list_objects({.bucket = "dummy",
            .prefix = "AzureObjStorageClientTest/delete_objects_recursively",}, &files);
    // clang-format on
    EXPECT_EQ(response.status.code, ErrorCode::OK);
    EXPECT_EQ(files.size(), 0);
}
#else

class AzureObjStorageClientTest : public testing::Test {
protected:
    void SetUp() override { GTEST_SKIP() << "Skipping Azure test, because USE_AZURE not defined"; }
};

TEST_F(AzureObjStorageClientTest, dummy_test) {
    LOG(INFO) << "AzureObjStorageClientTest::dummy_test";
}

#endif // #ifdef USE_AZURE

} // namespace doris
