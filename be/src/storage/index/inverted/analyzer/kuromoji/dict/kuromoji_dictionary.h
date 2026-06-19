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
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/darts.h"
#include "storage/index/inverted/analyzer/kuromoji/dict/kuromoji_dict_format.h"

namespace doris::segment_v2::kuromoji {

// One read-only mmap region; unmaps on destruction.
class MappedFile {
public:
    MappedFile() = default;
    ~MappedFile();
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;

    Status open(const std::string& path);
    const uint8_t* data() const { return _data; }
    std::size_t size() const { return _size; }

private:
    const uint8_t* _data = nullptr;
    std::size_t _size = 0;
};

// Read-only kuromoji dictionary backed by four mmapped files. After load() it is
// immutable and safe to share across threads. Returned string_views/pointers are
// valid only for the dictionary's lifetime. This is the query API the Phase-2
// Viterbi tokenizer consumes.
class KuromojiDictionary {
public:
    struct PrefixMatch {
        uint32_t trie_value;
        uint32_t length; // bytes consumed from the input
    };

    static Status load(const std::string& dir, std::unique_ptr<KuromojiDictionary>* out);

    // Process-wide, per-directory cache: loads the dictionary at `dir` once and
    // returns a stable pointer valid for the process lifetime. Returns nullptr if
    // `dir` has no valid dictionary (the failure is logged and cached). Thread-safe.
    static const KuromojiDictionary* get_or_load(const std::string& dir);

    void common_prefix_search(const char* text, std::size_t len,
                              std::vector<PrefixMatch>* out) const;

    // System dictionary.
    WordIdRun run_for_value(uint32_t trie_value) const { return _runs[trie_value]; }
    const WordEntry& word(uint32_t word_id) const { return _entries[word_id]; }
    std::string_view feature(const WordEntry& e) const {
        return feature_at(_features, _features_bytes, e.feature_offset);
    }

    // Connection costs.
    int16_t connection_cost(uint32_t forward_id, uint32_t backward_id) const {
        return ::doris::segment_v2::kuromoji::connection_cost(_cells, _forward_size, forward_id,
                                                              backward_id);
    }

    // Character definitions.
    uint8_t char_category(char32_t cp) const {
        return cp < 0x10000 ? _catmap[cp] : static_cast<uint8_t>(CAT_DEFAULT);
    }
    bool is_invoke(char32_t cp) const { return _defs[char_category(cp)].invoke != 0; }
    bool is_group(char32_t cp) const { return _defs[char_category(cp)].group != 0; }

    // Unknown-word dictionary.
    WordIdRun unknown_run(uint8_t category) const { return _unk_runs[category]; }
    const WordEntry& unknown_word(uint32_t word_id) const { return _unk_entries[word_id]; }
    std::string_view unknown_feature(const WordEntry& e) const {
        return feature_at(_unk_features, _unk_features_bytes, e.feature_offset);
    }

private:
    static std::string_view feature_at(const uint8_t* blob, uint64_t blob_bytes, uint32_t off);
    static Status check_header(const uint8_t* p, std::size_t size, KmjFileKind kind);
    Status map_system(const std::string& path);
    Status map_matrix(const std::string& path);
    Status map_chardef(const std::string& path);
    Status map_unkdict(const std::string& path);

    MappedFile _system_map;
    MappedFile _matrix_map;
    MappedFile _chardef_map;
    MappedFile _unk_map;

    // system
    Darts::DoubleArray _trie;
    const WordIdRun* _runs = nullptr;
    const WordEntry* _entries = nullptr;
    const uint8_t* _features = nullptr;
    uint64_t _features_bytes = 0;
    // matrix
    const int16_t* _cells = nullptr;
    uint32_t _forward_size = 0;
    // chardef
    const uint8_t* _catmap = nullptr;
    const CategoryDef* _defs = nullptr;
    // unk
    const WordIdRun* _unk_runs = nullptr;
    const WordEntry* _unk_entries = nullptr;
    const uint8_t* _unk_features = nullptr;
    uint64_t _unk_features_bytes = 0;
};

} // namespace doris::segment_v2::kuromoji
