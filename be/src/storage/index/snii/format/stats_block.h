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

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/format/format_constants.h"

namespace doris::snii::format {

// Statistics block within the per-index meta block. Carries only the counting stats
// needed for query planning and BM25; section location info is stored separately in SectionRefs (see design spec "Per-index meta block").
//
// On-disk layout (framed by SectionFramer with unified type+len+crc32c):
//   [u8 type=kStatsBlock][varint64 payload_len][payload][fixed32 crc32c]
//   payload = varint64{ doc_count, indexed_doc_count, term_count,
//                       sum_total_term_freq, null_count }
// For field semantics see design spec "Scoring statistics design".
struct StatsBlock {
    uint64_t doc_count = 0;           // total doc count at segment level (including unindexed/NULL)
    uint64_t indexed_doc_count = 0;   // number of docs actually indexed (denominator for avgdl)
    uint64_t term_count = 0;          // number of unique terms in this index
    uint64_t sum_total_term_freq = 0; // total token count across all indexed docs
    uint64_t null_count = 0;          // number of NULL / not-indexed docs
};

// Encodes into a kStatsBlock framed section (with built-in crc32c checksum) and appends to sink.
void encode_stats_block(const StatsBlock& sb, ByteSink* sink);

// Reads and verifies a kStatsBlock framed section from src, populates out.
// CRC mismatch / truncation → kCorruption; type is not kStatsBlock → kInvalidArgument.
Status decode_stats_block(ByteSource* src, StatsBlock* out);

} // namespace doris::snii::format
