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

#include "runtime/lance_blob_source_dictionary.h"

#include <algorithm>
#include <cctype>

namespace doris {
namespace {
bool is_remote_dataset(const std::string& uri) {
    const auto end = uri.find("://");
    if (end == std::string::npos || end == 0) {
        return false;
    }
    auto scheme = uri.substr(0, end);
    std::transform(scheme.begin(), scheme.end(), scheme.begin(),
                   [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return scheme != "file" && scheme != "memory";
}
} // namespace

Status LanceBlobSourceDictionary::init(const std::map<int, TFileScanRangeParams>& scan_params) {
    for (const auto& [scan_node_id, params] : scan_params) {
        if (!params.__isset.lance_scan_params) {
            continue;
        }
        const auto& lance = params.lance_scan_params;
        if (!lance.__isset.blob_read_source_ids) {
            continue;
        }
        if (scan_node_id < 0 || !lance.__isset.scan_node_id || lance.scan_node_id != scan_node_id ||
            lance.blob_read_source_ids.empty()) {
            return Status::InvalidArgument("Incomplete FE Lance BLOB source specification");
        }
        if (!lance.__isset.dataset_uri || !lance.__isset.version || lance.version <= 0 ||
            lance.dataset_uri.find('\0') != std::string::npos ||
            !is_remote_dataset(lance.dataset_uri)) {
            return Status::NotSupported(
                    "Lance BLOB reads require remote storage and a fixed snapshot");
        }
        for (const auto& [column_name, source_id] : lance.blob_read_source_ids) {
            if (source_id <= 0 || column_name.empty() ||
                column_name.find('\0') != std::string::npos ||
                !_sources_by_id
                         .emplace(static_cast<uint32_t>(source_id),
                                  LanceBlobSource {scan_node_id, column_name})
                         .second) {
                return Status::InvalidArgument("Invalid or duplicate FE FILE read source ID {}",
                                               source_id);
            }
        }
    }
    return Status::OK();
}

const LanceBlobSource* LanceBlobSourceDictionary::find_source(uint32_t source_id) const {
    const auto it = _sources_by_id.find(source_id);
    return it == _sources_by_id.end() ? nullptr : &it->second;
}

} // namespace doris
