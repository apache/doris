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

#include <CLucene.h>
#include <parallel_hashmap/phmap.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <memory>
#include <unordered_map>
#include <vector>

#include "CLucene/_SharedHeader.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/core/CharacterUtil.h"
#include "olap/rowset/segment_v2/inverted_index/analyzer/ik/dic/Hit.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class DictSegment : public std::enable_shared_from_this<DictSegment> {
private:
    static constexpr size_t ARRAY_LENGTH_LIMIT = 3;
    int32_t key_char_;
    std::vector<std::unique_ptr<DictSegment>> children_array_;
    phmap::flat_hash_map<int32_t, std::unique_ptr<DictSegment>> children_map_;

    DictSegment* lookforSegment(int32_t key_char, bool create_if_missing);
    int node_state_ {0};
    size_t store_size_ {0};

public:
    explicit DictSegment(int32_t key_char);
    ~DictSegment() = default;

    DictSegment(const DictSegment&) = delete;
    DictSegment& operator=(const DictSegment&) = delete;
    DictSegment(DictSegment&&) noexcept = default;
    DictSegment& operator=(DictSegment&&) noexcept = default;

    bool hasNextNode() const;
    Hit match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
              size_t length);
    void match(const CharacterUtil::TypedRuneArray& typed_runes, size_t unicode_offset,
               size_t length, Hit& search_hit);
    void fillSegment(const char* text);
};

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
