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

#include <array>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"

namespace doris::segment_v2::kuromoji {

// One morpheme entry to serialize. `feature` is the raw UTF-8 feature columns
// (comma-joined IPADIC fields), or empty for none.
struct BuilderWord {
    int16_t left_id = 0;
    int16_t right_id = 0;
    int16_t word_cost = 0;
    std::string feature;
};

// System dictionary input: (surface, homograph entries). Any order; the builder
// sorts surfaces by raw bytes (required by Darts).
struct SystemDictInput {
    std::vector<std::pair<std::string, std::vector<BuilderWord>>> surfaces;
};

// Connection-cost matrix: cells row-major by backward_id,
// cells[backward_id * forward_size + forward_id].
struct MatrixInput {
    uint32_t forward_size = 0;
    uint32_t backward_size = 0;
    std::vector<int16_t> cells;
};

// Character definitions: each BMP code point -> category ordinal; per-category flags.
struct CharDefInput {
    std::array<uint8_t, 0x10000> catmap {};
    std::vector<CategoryDef> defs; // indexed by category ordinal
};

// Unknown-word entries keyed by category ordinal.
struct UnkDictInput {
    std::vector<std::vector<BuilderWord>> per_category;
};

// Offline serializer: writes the four mmap-friendly .bin files consumed by
// KuromojiDictionary. Build-time only (the offline converter + unit tests).
class KuromojiDictionaryBuilder {
public:
    static Status write_system(const std::string& path, const SystemDictInput& in);
    static Status write_matrix(const std::string& path, const MatrixInput& in);
    static Status write_chardef(const std::string& path, const CharDefInput& in);
    static Status write_unkdict(const std::string& path, const UnkDictInput& in);
};

} // namespace doris::segment_v2::kuromoji
