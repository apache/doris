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

#include <cpp/sync_point.h>
#include <fmt/core.h>
#include <gen_cpp/Status_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <azure/core/http/http.hpp>
#include <azure/core/http/http_status_code.hpp>
#include <azure/core/io/body_stream.hpp>
#include <azure/storage/blobs.hpp>
#include <azure/storage/blobs/blob_batch.hpp>
#include <azure/storage/blobs/blob_client.hpp>
#ifdef USE_AZURE
#include <azure/core/diagnostics/logger.hpp>
#include <azure/storage/blobs/blob_container_client.hpp>
#endif
#include <azure/storage/blobs/blob_sas_builder.hpp>
#include <azure/storage/blobs/rest_client.hpp>
#include <azure/storage/common/account_sas_builder.hpp>
#include <azure/storage/common/storage_credential.hpp>
#include <azure/storage/common/storage_exception.hpp>
#include <chrono>
#include <exception>
#include <iterator>
#include <optional>
#include <ranges>

#include "cpp/obj-storage/client/obj_storage_client.h"
#include "cpp/obj-storage/client_bvar.h"
#include "cpp/obj_retry_strategy.h"
#include "cpp/util.h"

namespace doris {
void initialize_azure_logging(int log_level);
struct AzureObjStorageConfig;

using namespace Azure::Storage::Blobs;

static const Azure::DateTime SystemClockEpoch {1970, 1, 1};

bool is_azure_tls_ca_error_message(std::string_view message);
std::string build_azure_tls_debug_suffix(std::string_view error_message,
                                         std::string_view tls_debug_context);
std::string azure_multipart_block_id(std::string_view upload_id, int part_num);
std::string build_azure_batch_delete_failure_message(const ObjStoragePath& opts,
                                                     std::string_view key);

class AzureObjStorageClient final : public ObjStorageClient {
public:
    static ObjStorageClientBuildResult create(const AzureObjStorageConfig& config);

    AzureObjStorageClient(std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> client,
                          ObjStorageEndpointInfo config,
                          std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> credential,
                          std::optional<std::chrono::milliseconds> request_timeout = std::nullopt)
            : _config(std::move(config)),
              _client(std::move(client)),
              _credential(std::move(credential)),
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
    ObjStorageEndpointInfo _config;
    std::shared_ptr<Azure::Storage::Blobs::BlobServiceClient> _client;
    std::shared_ptr<Azure::Storage::StorageSharedKeyCredential> _credential;
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
