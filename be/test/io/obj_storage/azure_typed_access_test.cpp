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
#include <sstream>
#include <string>
#include <variant>

#include "common/config.h"
#include "io/file_factory.h"
#include "io/fs/file_system.h"
#include "io/fs/obj_storage_file_system.h"
#include "io/fs/obj_storage_file_writer.h"
#include "io/obj_storage/obj_storage_config.h"

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

TEST(AzureTypedAccessTest, ObjectStorageFileSystemTypeIsNamedNeutrally) {
    std::ostringstream out;
    out << FileSystemType::OBJECT_STORAGE;
    EXPECT_EQ(out.str(), "OBJECT_STORAGE");
}

TEST(AzureTypedAccessTest, CreateFsTakesContainerAndKeysFromNativeLocations) {
    auto config = typed_access_shared_key_config();
    auto fs = typed_access_create_fs(config, "abfss://c1@acct.dfs.core.windows.net/db/t/f.parquet");
#ifdef USE_AZURE
    ASSERT_TRUE(fs.has_value()) << fs.error();
    auto* s3_fs = dynamic_cast<ObjStorageFileSystem*>(fs.value().get());
    ASSERT_NE(s3_fs, nullptr);
    EXPECT_EQ(s3_fs->bucket(), "c1");
    const auto& backend = s3_fs->client_holder()->state()->config.backend;
    ASSERT_TRUE(std::holds_alternative<AzureClientConfig>(backend));
    EXPECT_EQ(std::get<AzureClientConfig>(backend).container, "c1");

    // Every location form of the same file resolves to the same key.
    for (const auto* path :
         {"abfss://c1@acct.dfs.core.windows.net/db/t/f.parquet",
          "wasbs://c1@acct.blob.core.windows.net/db/t/f.parquet",
          "https://acct.blob.core.windows.net/c1/db/t/f.parquet", "s3://c1/db/t/f.parquet"}) {
        auto key = s3_fs->_get_key(Path(path));
        ASSERT_TRUE(key.has_value()) << path << ": " << key.error();
        EXPECT_EQ(key.value(), "db/t/f.parquet") << path;
    }
#else
    EXPECT_FALSE(fs.has_value());
#endif
}

TEST(AzureTypedAccessTest, CreateFsRejectsAnotherAccountsLocation) {
    auto config = typed_access_shared_key_config();
    auto fs = typed_access_create_fs(config, "abfss://c1@other.dfs.core.windows.net/db/f.parquet");
    // Rejected while resolving the config, before any client is built.
    ASSERT_FALSE(fs.has_value());
    EXPECT_TRUE(fs.error().is<ErrorCode::INVALID_ARGUMENT>()) << fs.error();
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
