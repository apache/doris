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
#include <vector>

#include "common/status.h"

struct LanceDataset;

namespace doris {
class ColumnVarbinary;
class RuntimeState;

// One instance per BE/query. Dataset ownership is independent of Scanner EOF and of expression
// clones; opened dataset handles are shared. Input/output positions remain call-local.
class LanceBlobReader {
public:
    struct BlobRequest {
        // ID in the current query's LanceBlobSourceDictionary.
        uint32_t blob_source_id;
        // Native Lance row ID, independent of the input/output column positions.
        uint64_t blob_row_id;
        size_t output_row_index;
    };
    Status read_blob(RuntimeState* state, const std::vector<BlobRequest>& requests,
                     ColumnVarbinary* output);

private:
    Status _get_or_open_dataset(RuntimeState* state, int32_t scan_node_id,
                                std::shared_ptr<LanceDataset>* dataset);
    std::mutex _dataset_cache_mutex;
    // Open dataset handles shared by all BLOB columns from the same scan node.
    std::map<int32_t, std::shared_ptr<LanceDataset>> _datasets_by_scan_node_id;
};

} // namespace doris
