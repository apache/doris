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

#include <cstdint>
#include <optional>

#include "common/status.h"
#include "storage/index/inverted/common_grams/common_grams_segment_metadata.h"
#include "storage/index/inverted/gram/gram_scheme.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/format/format_constants.h"
#include "storage/index/snii/format/stats_block.h"

namespace doris::snii::format {

struct RegionRef {
    uint64_t offset = 0;
    uint64_t length = 0;
};

struct SectionRefs {
    RegionRef dict_region;
    RegionRef posting_region;
    RegionRef norms;
    RegionRef null_bitmap;
    RegionRef bsbf;
};

enum class CommonGramsPostingPolicy : uint8_t {
    kNone = 0,
    kDocsOnlyV1 = 1,
    kHybridV1 = kDocsOnlyV1,
};

struct CoreMetadata {
    IndexConfig index_config = IndexConfig::kDocsOnly;
    StatsBlock stats;
    SectionRefs section_refs;
    std::optional<segment_v2::inverted_index::CommonGramsSegmentMetadata> common_grams_metadata;
    CommonGramsPostingPolicy common_grams_posting_policy = CommonGramsPostingPolicy::kNone;
    // gram 族索引的分段方案，供 P1 的 mode=auto 按段自适应选型使用。P0 写入侧恒为
    // nullopt：这里只预留字段与编解码，不改变任何既有段的编码字节。
    std::optional<segment_v2::gram::GramScheme> gram_scheme;
};

Status encode_core_metadata(const CoreMetadata& metadata, ByteSink* out);
Status decode_core_metadata(Slice framed_bytes, CoreMetadata* out);

} // namespace doris::snii::format
