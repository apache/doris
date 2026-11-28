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

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <vector>

#include "common/status.h"

struct LanceDataset;

namespace doris {
class ColumnVarbinary;
class RuntimeState;
class TLanceBlobResource;

// One instance per BE/query. Dataset ownership is independent of Scanner EOF and of expression
// clones; opened dataset handles are shared. Input/output positions remain call-local.
class LanceBlobReader {
public:
    struct BlobRequest {
        // ID in the current query's lance_blob_resources.
        uint32_t blob_source_id;
        // Native Lance row ID, independent of the input/output column positions.
        uint64_t blob_row_id;
        size_t output_row_index;
    };
    Status read_blob(RuntimeState* state, const std::vector<BlobRequest>& requests,
                     ColumnVarbinary* output);

private:
    using BlobRows = std::map<uint64_t, std::vector<size_t>>;

    struct DatasetKey {
        std::string uri;
        int64_t version;
        std::map<std::string, std::string> storage_options;

        bool operator<(const DatasetKey& other) const {
            return std::tie(uri, version, storage_options) <
                   std::tie(other.uri, other.version, other.storage_options);
        }
    };

    Status _get_or_open_dataset(RuntimeState* state, const TLanceBlobResource& resource,
                                std::shared_ptr<LanceDataset>* dataset);
    Status _read_source_blobs(RuntimeState* state, const TLanceBlobResource& resource,
                              const BlobRows& rows, ColumnVarbinary* output);
    std::mutex _dataset_cache_mutex;
    // URI, pinned version and access options define dataset identity, independently of the column.
    std::map<DatasetKey, std::shared_ptr<LanceDataset>> _datasets_by_open_spec;
};

} // namespace doris
