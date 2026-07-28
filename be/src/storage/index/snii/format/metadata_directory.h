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
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"

namespace doris::snii::format {

struct MetadataBlobRef {
    uint64_t offset = 0;
    uint64_t length = 0;
};

struct LogicalIndexMetadataRef {
    uint64_t index_id = 0;
    std::string index_suffix;
    MetadataBlobRef core_metadata;
    MetadataBlobRef sampled_term_index;
    MetadataBlobRef dict_block_directory;
};

class MetadataDirectory {
public:
    static Status decode(Slice bytes, MetadataDirectory* out);

    const LogicalIndexMetadataRef* find(uint64_t index_id, std::string_view suffix) const;
    size_t size() const { return entries_.size(); }
    const std::vector<LogicalIndexMetadataRef>& entries() const { return entries_; }

private:
    std::vector<LogicalIndexMetadataRef> entries_;
};

Status encode_metadata_directory(const std::vector<LogicalIndexMetadataRef>& entries,
                                 ByteSink* out);

} // namespace doris::snii::format
