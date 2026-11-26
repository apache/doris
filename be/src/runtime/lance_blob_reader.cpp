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

#include "runtime/lance_blob_reader.h"

#include <lance/lance.h>

#include <limits>
#include <string>
#include <utility>

#include "core/column/column_varbinary.h"
#include "format_v2/lance/lance_reader_helper.h"
#include "format_v2/lance/lance_session_manager.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris {

Status LanceBlobReader::_get_or_open_dataset(RuntimeState* state,
                                             const TLanceBlobResource& resource,
                                             std::shared_ptr<LanceDataset>* dataset) {
    DatasetKey key {resource.dataset_uri, resource.version, resource.storage_options};
    {
        std::lock_guard lock(_dataset_cache_mutex);
        const auto cached = _datasets_by_open_spec.find(key);
        if (cached != _datasets_by_open_spec.end()) {
            *dataset = cached->second;
            return Status::OK();
        }
    }
    RETURN_IF_CANCELLED(state);
    std::vector<std::string> options;
    RETURN_IF_ERROR(format::lance::build_lance_storage_options(resource.storage_options, &options));
    std::vector<const char*> option_ptrs;
    for (const auto& option : options) {
        option_ptrs.push_back(option.c_str());
    }
    option_ptrs.push_back(nullptr);
    LanceDataset* raw = nullptr;
    RETURN_IF_ERROR(format::lance::LanceSessionManager::instance().open_dataset(
            resource.dataset_uri.c_str(), options.empty() ? nullptr : option_ptrs.data(),
            static_cast<uint64_t>(resource.version), &raw));
    std::shared_ptr<LanceDataset> opened(raw, lance_dataset_close);
    // Concurrent expressions may open the same source; retain one and safely release the other.
    std::lock_guard lock(_dataset_cache_mutex);
    *dataset = _datasets_by_open_spec.try_emplace(std::move(key), std::move(opened)).first->second;
    return Status::OK();
}

Status LanceBlobReader::read_blob(RuntimeState* state, const std::vector<BlobRequest>& requests,
                                  ColumnVarbinary* output) {
    if (state == nullptr || state->get_query_ctx() == nullptr) {
        return Status::InvalidArgument("External FILE content requires a query read context");
    }
    const auto* query_ctx = state->get_query_ctx();
    // source ID -> Lance row ID -> output row positions requesting that Blob.
    // Group by source to batch handle resolution, and read each distinct Blob once even when
    // multiple output rows reference it.
    std::map<uint32_t, std::map<uint64_t, std::vector<size_t>>> groups;
    for (const auto& request : requests) {
        if (request.output_row_index >= output->size()) {
            return Status::InternalError("FILE read output position is out of bounds");
        }
        groups[request.blob_source_id][request.blob_row_id].push_back(request.output_row_index);
    }
    for (const auto& [source_id, rows] : groups) {
        RETURN_IF_CANCELLED(state);
        const auto* resource = query_ctx->get_lance_blob_resource(source_id);
        if (resource == nullptr) {
            return Status::InvalidArgument("Lance BLOB resource {} is not declared by FE",
                                           source_id);
        }
        std::shared_ptr<LanceDataset> dataset;
        RETURN_IF_ERROR(_get_or_open_dataset(state, *resource, &dataset));
        std::vector<uint64_t> ids;
        ids.reserve(rows.size());
        for (const auto& entry : rows) {
            ids.push_back(entry.first);
        }
        std::vector<LanceBlobFile*> handles(ids.size(), nullptr);
        Defer close_handles([&] {
            for (auto* handle : handles) {
                if (handle != nullptr) {
                    lance_blob_file_close(handle);
                }
            }
        });
        if (lance_dataset_take_blobs(dataset.get(), ids.data(), ids.size(),
                                     resource->column_name.c_str(), handles.data()) != 0) {
            return format::lance::lance_error("resolve FILE payload handles");
        }
        // Validate each Blob's length before allocating and reading its payload.
        for (size_t i = 0; i < handles.size(); ++i) {
            RETURN_IF_CANCELLED(state);
            if (handles[i] == nullptr) {
                return Status::Corruption("Non-NULL FILE resolved to NULL in its pinned snapshot");
            }
            const auto blob_size = lance_blob_file_size(handles[i]);
            if (lance_last_error_code() != LANCE_OK) {
                return format::lance::lance_error("get FILE payload size");
            }
            if (blob_size > std::numeric_limits<uint32_t>::max()) {
                return Status::InvalidArgument(
                        "FILE payload size {} exceeds VARBINARY length capacity", blob_size);
            }
            const auto size = static_cast<size_t>(blob_size);
            char* payload = size == 0 ? nullptr : output->alloc(size);
            if (size != 0 &&
                lance_blob_file_read_range(handles[i], 0, reinterpret_cast<uint8_t*>(payload),
                                           size) != 0) {
                return format::lance::lance_error("read FILE payload");
            }
            const StringView value(payload, static_cast<uint32_t>(size));
            for (const auto position : rows.at(ids[i])) {
                output->get_data()[position] = value;
            }
        }
    }
    return Status::OK();
}

} // namespace doris
