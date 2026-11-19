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

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <variant>

#include "common/config.h"
#include "cpp/obj-storage/parser/obj_storage_config.h"
#include "io/file_factory.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/fs/obj_storage_file_writer.h"
#include "io/object_storage/azure_blob_location.h"

namespace doris::io {
namespace {

// SharedKey builds an Azure SDK client without any network request, so these tests exercise
// the typed entries end to end up to the first storage request.
TAzureClientConfig typed_access_shared_key_config() {
    TAzureSharedKey shared_key;
    shared_key.__set_account_key("YWNjb3VudC1rZXk=");
    TAzureCredential credential;
    credential.__set_shared_key(shared_key);
    TAzureClientConfig config;
    config.__set_endpoint("https://acct.blob.core.windows.net");
    config.__set_account_name("acct");
    config.__set_credential(credential);
    return config;
}

Result<FileSystemSPtr> typed_access_create_fs(const TAzureClientConfig& config,
                                              const std::string& path) {
    std::map<std::string, std::string> properties;
    FSPropertiesRef fs_properties(TFileType::FILE_S3);
    fs_properties.properties = &properties;
    fs_properties.azure_client_config = &config;
    return FileFactory::create_fs(fs_properties, {.path = path, .fs_name {}});
}

} // namespace

TEST(AzureTypedAccessTest, ScanParametersCarryTypedAccessIntoFileSystemProperties) {
    TFileScanRangeParams params;
    EXPECT_FALSE(azure_client_config_of(params).has_value());

    auto config = typed_access_shared_key_config();
    params.__set_azure_client_config(config);
    FileSystemProperties properties;
    properties.system_type = TFileType::FILE_S3;
    properties.azure_client_config = azure_client_config_of(params);
    ASSERT_TRUE(properties.azure_client_config.has_value());
    EXPECT_EQ(properties.azure_client_config->account_name, "acct");

    // The reference view used by create_fs points at the same typed access.
    FSPropertiesRef ref(properties);
    EXPECT_EQ(ref.azure_client_config, &*properties.azure_client_config);
    properties.azure_client_config.reset();
    EXPECT_EQ(FSPropertiesRef(properties).azure_client_config, nullptr);
}

TEST(AzureTypedAccessTest, CreateFsTakesContainerAndKeysFromNativeLocations) {
    auto config = typed_access_shared_key_config();
    auto fs = typed_access_create_fs(config, "abfss://c1@acct.dfs.core.windows.net/db/t/f.parquet");
#ifdef USE_AZURE
    ASSERT_TRUE(fs.has_value()) << fs.error();
    auto* fs_impl = dynamic_cast<ObjStorageFileSystem*>(fs.value().get());
    ASSERT_NE(fs_impl, nullptr);
    EXPECT_EQ(fs_impl->bucket(), "c1");
    EXPECT_TRUE(std::holds_alternative<AzureObjStorageConfig>(fs_impl->client_holder()->config()));

    // A known size lets us open the reader without sending a storage request.
    FileReaderOptions options;
    options.file_size = 1;
    for (const auto* path :
         {"abfss://c1@acct.dfs.core.windows.net/db/t/f.parquet",
          "wasbs://c1@acct.blob.core.windows.net/db/t/f.parquet",
          "https://acct.blob.core.windows.net/c1/db/t/f.parquet", "s3://c1/db/t/f.parquet"}) {
        FileReaderSPtr reader;
        auto st = fs_impl->open_file(path, &reader, &options);
        ASSERT_TRUE(st.ok()) << path << ": " << st;
        EXPECT_EQ(reader->path().native(), "s3://c1/db/t/f.parquet") << path;
    }
    FileReaderSPtr reader;
    EXPECT_FALSE(fs_impl->open_file("abfss://other@acct.dfs.core.windows.net/k", &reader, &options)
                         .ok());
    EXPECT_FALSE(
            fs_impl->open_file("abfss://c1@elsewhere.dfs.core.windows.net/k", &reader, &options)
                    .ok());
#else
    EXPECT_FALSE(fs.has_value());
#endif
}

TEST(AzureTypedAccessTest, ScanReaderUsesTypedCredentialsBeforeLegacyProperties) {
    FileSystemProperties props;
    props.system_type = TFileType::FILE_S3;
    props.properties = {{"provider", "azure"},
                        {"AWS_ENDPOINT", "https://old.blob.core.windows.net"},
                        {"AWS_ACCESS_KEY", "old"},
                        {"AWS_SECRET_KEY", "YWNjb3VudC1rZXk="}};
    props.azure_client_config = typed_access_shared_key_config();
    FileDescription file;
    file.path = "abfss://blue@acct.dfs.core.windows.net/data/f.parquet";
    file.file_size = 1;
    FileReaderOptions options;
#ifdef USE_AZURE
    auto reader = FileFactory::create_file_reader(props, file, options);
    ASSERT_TRUE(reader.has_value()) << reader.error();
    EXPECT_EQ((*reader)->path().native(), "s3://blue/data/f.parquet");
#endif

    TAzureSas sas;
    sas.__set_token("sig=example");
    sas.__set_expires_at_ms(1);
    TAzureCredential expired;
    expired.__set_sas(sas);
    props.azure_client_config->__set_credential(expired);
    auto denied = FileFactory::create_file_reader(props, file, options);
    ASSERT_FALSE(denied.has_value());
    EXPECT_TRUE(denied.error().is<ErrorCode::INVALID_ARGUMENT>()) << denied.error();
}

TEST(AzureTypedAccessTest, CreateFsRejectsAnotherAccountsLocation) {
    auto config = typed_access_shared_key_config();
    auto fs = typed_access_create_fs(config, "abfss://c1@other.dfs.core.windows.net/db/f.parquet");
    // Rejected while resolving the config, before any client is built.
    ASSERT_FALSE(fs.has_value());
    EXPECT_TRUE(fs.error().is<ErrorCode::INVALID_ARGUMENT>()) << fs.error();
}

TEST(AzureTypedAccessTest, EachFileBindsItsOwnContainer) {
    TAzureSas sas;
    sas.__set_token("sv=2025-01-05&sig=example");
    TAzureCredential credential;
    credential.__set_sas(sas);
    auto config = typed_access_shared_key_config();
    config.__set_credential(credential);
    for (const auto* container : {"blue", "green"}) {
        auto fs = typed_access_create_fs(
                config, std::string("abfss://") + container + "@acct.dfs.core.windows.net/file");
#ifdef USE_AZURE
        ASSERT_TRUE(fs.has_value()) << fs.error();
        EXPECT_EQ(dynamic_cast<ObjStorageFileSystem&>(**fs).bucket(), container);
#else
        EXPECT_FALSE(fs.has_value());
#endif
        EXPECT_FALSE(config.__isset.container);
    }
    config.__set_container("blue");
    EXPECT_FALSE(typed_access_create_fs(config, "s3://green/file").has_value());
}

TEST(AzureTypedAccessTest, ResolvesCustomServicePathAndRelativeKeys) {
    AzureBlobEndpoint endpoint {.url = "http://localhost:10000/devstoreaccount1/",
                                .account = "devstoreaccount1"};
    auto uri = endpoint.resolve("http://localhost:10000/devstoreaccount1/blue/dir/key", "");
    ASSERT_TRUE(uri.has_value()) << uri.error();
    EXPECT_EQ(uri->container, "blue");
    EXPECT_EQ(uri->key, "dir/key");
    EXPECT_FALSE(
            endpoint.resolve("http://localhost:10000/devstoreaccount10/blue/key", "").has_value());
    EXPECT_FALSE(endpoint.resolve("relative", "").has_value());
    auto relative = endpoint.resolve("relative", "blue");
    ASSERT_TRUE(relative.has_value());
    EXPECT_EQ(relative->key, "relative");
}

TEST(AzureTypedAccessTest, BindsCustomLocationsToTheConfiguredService) {
    AzureBlobEndpoint service {.url = "http://storage.test:10000/accounts/dev", .account = "dev"};
    for (const auto* path : {"http://STORAGE.test:10000/accounts/dev/blue/dir/key",
                             "wasb://blue@storage.test:10000/dir/key", "s3://blue/dir/key"}) {
        auto location = service.resolve(path, "blue");
        ASSERT_TRUE(location.has_value()) << location.error();
        EXPECT_EQ(location->key, "dir/key");
    }
    for (const auto* path :
         {"http://backup.test:10000/accounts/dev/blue/dir/key",
          "http://storage.test:10001/accounts/dev/blue/dir/key",
          "http://storage.test:10000/accounts/development/blue/dir/key",
          "abfs://blue@backup.test:10000/dir/key", "wasb://blue@storage.test:10001/dir/key"}) {
        auto location = service.resolve(path, "");
        ASSERT_FALSE(location.has_value()) << path;
        EXPECT_TRUE(location.error().is<ErrorCode::INVALID_ARGUMENT>());
    }
    AzureBlobEndpoint custom {.url = "https://storage.test", .account = "acct"};
    EXPECT_FALSE(custom.resolve("https://backup.test/blue/key", "blue").has_value());
    EXPECT_FALSE(custom.resolve("http://storage.test:80/blue/key", "blue").has_value());
}

TEST(AzureTypedAccessTest, BlobAndDfsAliasesRequireTheSameAccountCloudAndPort) {
    AzureBlobEndpoint service {.url = "https://acct.blob.core.usgovcloudapi.net:443",
                               .account = "acct"};
    EXPECT_TRUE(service.resolve("abfss://blue@ACCT.dfs.core.usgovcloudapi.net/key", "blue")
                        .has_value());
    for (const auto* path : {"abfss://blue@acct.dfs.core.windows.net/key",
                             "https://acct.dfs.core.usgovcloudapi.net:8443/blue/key",
                             "https://storage.test/blue/key"}) {
        EXPECT_FALSE(service.resolve(path, "blue").has_value()) << path;
    }
    AzureBlobEndpoint onelake {.url = "https://onelake.blob.fabric.microsoft.com", .account = ""};
    EXPECT_TRUE(onelake.resolve("abfss://blue@onelake.dfs.fabric.microsoft.com/key", "blue")
                        .has_value());
}

TEST(AzureTypedAccessTest, HttpBlobNamesAreUnescapedOnceAfterResolvingTheContainer) {
    AzureBlobEndpoint service {.url = "http://localhost:10000/dev", .account = "dev"};
    auto file =
            service.resolve("http://localhost:10000/dev/blue/a+%20b%2520%23%3F.parquet", "blue");
    ASSERT_TRUE(file.has_value()) << file.error();
    EXPECT_EQ(file->key, "a+ b%20#?.parquet");
    auto logical = service.resolve("s3://blue/a%20b.parquet", "blue");
    ASSERT_TRUE(logical.has_value()) << logical.error();
    EXPECT_EQ(logical->key, "a%20b.parquet");
    EXPECT_FALSE(service.resolve("http://localhost:10000/dev/blue/bad%2", "blue").has_value());
}

TEST(AzureTypedAccessTest, CreateFileWriterUsesTheNativeLocation) {
    auto config = typed_access_shared_key_config();
    std::map<std::string, std::string> properties;
    FileWriterOptions options;
    // The writer consults the file cache when it is enabled; another test may have left it on.
    bool saved_enable_file_cache = config::enable_file_cache;
    config::enable_file_cache = false;
    auto writer = FileFactory::create_file_writer(
            TFileType::FILE_S3, nullptr, {}, properties,
            "abfss://c1@acct.dfs.core.windows.net/db/t/f.parquet", options, &config);
    config::enable_file_cache = saved_enable_file_cache;
#ifdef USE_AZURE
    ASSERT_TRUE(writer.has_value()) << writer.error();
    auto* s3_writer = dynamic_cast<ObjStorageFileWriter*>(writer.value().get());
    ASSERT_NE(s3_writer, nullptr);
    EXPECT_EQ(s3_writer->bucket(), "c1");
    EXPECT_EQ(s3_writer->key(), "db/t/f.parquet");
#else
    EXPECT_FALSE(writer.has_value());
#endif
}

} // namespace doris::io
