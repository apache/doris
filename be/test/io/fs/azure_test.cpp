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

#include <fmt/core.h>
#include <gtest/gtest.h>

#ifdef USE_AZURE
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#endif
#include <cstdio>
#include <iostream>
#include <stdexcept>
#include <utility>

#include "common/config.h"

namespace doris {

std::string GetConnectionString() {
    const static std::string ConnectionString = "";

    if (!ConnectionString.empty()) {
        return ConnectionString;
    }
    const static std::string envConnectionString = std::getenv("AZURE_STORAGE_CONNECTION_STRING");
    if (!envConnectionString.empty()) {
        return envConnectionString;
    }
    throw std::runtime_error("Cannot find connection string.");
}

TEST(AzureTest, Write) {
    GTEST_SKIP() << "Skipping Azure test, because this test it to test the compile and linkage";
#ifdef USE_AZURE
    using namespace Azure::Storage::Blobs;

    std::string accountName = config::test_s3_ak;
    std::string accountKey = config::test_s3_sk;

    auto cred =
            std::make_shared<Azure::Storage::StorageSharedKeyCredential>(accountName, accountKey);

    const std::string containerName = config::test_s3_bucket;
    const std::string blobName = "sample-blob";
    const std::string blobContent = "Fuck Azure!";
    const std::string uri =
            fmt::format("https://{}.blob.core.windows.net/{}", accountName, containerName);

    // auto containerClient =
    //         BlobContainerClient::CreateFromConnectionString(GetConnectionString(), containerName);

    auto containerClient = BlobContainerClient(uri, cred);
    containerClient.CreateIfNotExists();

    std::vector<int> blockIds1;

    auto blockBlobContainer = containerClient.GetBlockBlobClient(blobName);

    // Azure::Storage::StorageException exception;

    BlockBlobClient blobClient = containerClient.GetBlockBlobClient(blobName);
    std::vector<std::string> blockIds;

    std::vector<uint8_t> buffer(blobContent.begin(), blobContent.end());
    auto aresp = blobClient.UploadFrom(buffer.data(), buffer.size());

    Azure::Storage::Metadata blobMetadata = {{"key1", "value1"}, {"key2", "value2"}};
    blobClient.SetMetadata(blobMetadata);

    auto properties = blobClient.GetProperties().Value;
    for (auto metadata : properties.Metadata) {
        std::cout << metadata.first << ":" << metadata.second << std::endl;
    }
    // We know blob size is small, so it's safe to cast here.
    buffer.resize(static_cast<size_t>(properties.BlobSize));

    blobClient.DownloadTo(buffer.data(), buffer.size());

    std::cout << std::string(buffer.begin(), buffer.end()) << std::endl;
#endif
}

} // namespace doris
