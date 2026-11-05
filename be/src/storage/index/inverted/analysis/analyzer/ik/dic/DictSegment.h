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

#include <parallel_hashmap/phmap.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

namespace doris::segment_v2 {

// The trie a dictionary file is loaded into, one segment per character of its words. It only
// exists while the dictionary loads: FlatDict copies it into flat arrays, which the analysis
// reads, and the tree is freed then.
class DictSegment {
private:
    static constexpr size_t ARRAY_LENGTH_LIMIT = 3;
    // The segments that exist, for the test that a loaded Dictionary keeps none.
    static inline std::atomic<size_t> live_count_ {0};

    int32_t key_char_;
    std::vector<std::unique_ptr<DictSegment>> children_array_;
    phmap::flat_hash_map<int32_t, std::unique_ptr<DictSegment>> children_map_;

    DictSegment* lookforSegment(int32_t key_char, bool create_if_missing);
    bool is_word_end_ = false;
    size_t store_size_ {0};

public:
    explicit DictSegment(int32_t key_char);
    ~DictSegment();

    DictSegment(const DictSegment&) = delete;
    DictSegment& operator=(const DictSegment&) = delete;

    static size_t liveCount() { return live_count_.load(std::memory_order_relaxed); }

    int32_t keyChar() const { return key_char_; }
    bool isWordEnd() const { return is_word_end_; }
    bool hasNextNode() const;
    size_t childCount() const { return store_size_; }
    // The child for `key_char`, or nullptr.
    const DictSegment* findChild(int32_t key_char) const;
    // Calls `visitor` with each child, in no particular order.
    template <typename Visitor>
    void forEachChild(Visitor&& visitor) const {
        if (store_size_ <= ARRAY_LENGTH_LIMIT) {
            for (size_t i = 0; i < store_size_; ++i) {
                visitor(*children_array_[i]);
            }
        } else {
            for (const auto& [key, child] : children_map_) {
                visitor(*child);
            }
        }
    }

    void fillSegment(const char* text);
};

} // namespace doris::segment_v2
