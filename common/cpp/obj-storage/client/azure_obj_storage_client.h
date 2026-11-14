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

#include <azure/storage/blobs/blob_service_client.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <chrono>
#include <optional>

#include "cpp/obj-storage/client/obj_storage_client.h"

namespace doris {
struct AzureObjStorageConfig;
void initialize_azure_sdk(int log_level);

bool is_azure_tls_ca_error_message(std::string_view message);
std::string build_azure_tls_debug_suffix(std::string_view error_message,
                                         std::string_view tls_debug_context);
std::string azure_multipart_block_id(std::string_view upload_id, int part_num);
std::string build_azure_batch_delete_failure_message(const ObjStoragePath& opts,
                                                     std::string_view key);

class AzureObjStorageClient final : public ObjStorageClient {
public:
    static ObjStorageClientBuildResult create(const AzureObjStorageConfig& config);

    AzureObjStorageClient(Azure::Storage::Blobs::BlobServiceClient service,
                          std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> key,
                          ObjStorageEndpointInfo endpoint,
                          std::optional<std::chrono::milliseconds> request_timeout = {})
            : _service(std::move(service)),
              _credential(std::move(key)),
              _config(std::move(endpoint)),
              _request_timeout(request_timeout) {}
    ~AzureObjStorageClient() override = default;
    ObjStorageUploadResult create_multipart_upload(const ObjStoragePath& opts) override;
    ObjStorageResponse put_object(const ObjStoragePath& opts, std::string_view stream) override;
    ObjStorageUploadResult upload_part(const ObjStoragePath& opts, const std::string& upload_id,
                                       std::string_view, int partNum) override;
    ObjStorageResponse complete_multipart_upload(
            const ObjStoragePath& opts, const std::string& upload_id,
            const std::vector<ObjStorageCompletedPart>& completed_parts) override;
    ObjStorageHeadResult head_object(const ObjStoragePath& opts) override;
    ObjStorageResponse get_object(const ObjStoragePath& opts, void* buffer, size_t offset,
                                  size_t bytes_read, size_t* size_return) override;
    ObjStorageResponse delete_object(const ObjStoragePath& opts) override;
    std::string generate_presigned_url(const ObjStoragePath& opts,
                                       int64_t expiration_secs) override;
    ObjStorageResponse get_lifecycle(const std::string& bucket, int64_t* expiration_days) override;

    ObjStorageResponse check_versioning(const std::string& bucket) override;

    ObjStorageResponse abort_multipart_upload(const ObjStoragePath& opts,
                                              const std::string& upload_id) override;
    ObjStorageCapabilities capabilities() const override {
        return {.max_delete_batch = 256, .max_list_page = 5000};
    }

protected:
    ObjStorageResponse delete_objects_batch(const ObjStoragePath& opts,
                                            std::span<std::string> objects) override;
    ObjStorageListPageResult list_objects_page(const ObjStoragePath& path,
                                               std::string_view continuation_token) override;

private:
    Azure::Core::Context request_context() const;
    Azure::Storage::Blobs::BlobServiceClient _service;
    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> _credential;
    ObjStorageEndpointInfo _config;
    std::optional<std::chrono::milliseconds> _request_timeout;
};

} // namespace doris

namespace doris::io {
using ::doris::AzureObjStorageClient;
using ::doris::azure_multipart_block_id;
using ::doris::build_azure_batch_delete_failure_message;
using ::doris::build_azure_tls_debug_suffix;
using ::doris::is_azure_tls_ca_error_message;
} // namespace doris::io
