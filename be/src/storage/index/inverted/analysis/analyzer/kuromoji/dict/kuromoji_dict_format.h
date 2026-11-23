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

namespace doris::segment_v2::inverted_index::kuromoji {

// "DORISKMJ" as 8 bytes; little-endian only.
inline constexpr char KMJ_MAGIC[8] = {'D', 'O', 'R', 'I', 'S', 'K', 'M', 'J'};
inline constexpr uint32_t KMJ_FORMAT_VERSION = 1;

enum KmjFileKind : uint32_t {
    KMJ_KIND_SYSTEM = 1,
    KMJ_KIND_MATRIX = 2,
    KMJ_KIND_CHARDEF = 3,
    KMJ_KIND_UNKDICT = 4,
};

// Canonical character categories (ordinals are stable on disk). Mirrors Lucene's set.
enum CharCategory : uint8_t {
    CAT_NGRAM = 0,
    CAT_DEFAULT = 1,
    CAT_SPACE = 2,
    CAT_SYMBOL = 3,
    CAT_NUMERIC = 4,
    CAT_ALPHA = 5,
    CAT_CYRILLIC = 6,
    CAT_GREEK = 7,
    CAT_HIRAGANA = 8,
    CAT_KATAKANA = 9,
    CAT_KANJI = 10,
    CAT_KANJINUMERIC = 11,
    CAT_CLASS_COUNT = 12,
};

#pragma pack(push, 1)
// Common 32-byte file header at offset 0 of every .bin. All ints little-endian.
struct KmjFileHeader {
    char magic[8];           // KMJ_MAGIC
    uint32_t format_version; // KMJ_FORMAT_VERSION
    uint32_t file_kind;      // KmjFileKind
    uint64_t file_size;      // total file bytes (sanity vs fstat)
    uint8_t reserved[8];     // zero
};

// system.bin sub-header (follows KmjFileHeader). Offsets are absolute from file start.
struct KmjSystemHeader {
    uint64_t trie_offset;
    uint64_t trie_bytes; // Darts array (4-byte units)
    uint64_t runs_offset;
    uint64_t runs_count; // WordIdRun[runs_count], indexed by trie value
    uint64_t entries_offset;
    uint64_t entries_count; // WordEntry[entries_count], indexed by word id
    uint64_t features_offset;
    uint64_t features_bytes; // length-prefixed UTF-8 blob
};

struct KmjMatrixHeader {
    uint32_t forward_size;  // right-context cardinality
    uint32_t backward_size; // left-context cardinality
    uint64_t cells_offset;  // int16[forward_size*backward_size], row-major by backward_id
};

struct KmjCharDefHeader {
    uint32_t class_count; // == CAT_CLASS_COUNT for IPADIC
    uint32_t reserved;
    uint64_t catmap_offset; // uint8[0x10000], one category per BMP code point
    uint64_t defs_offset;   // CategoryDef[class_count]
};

struct KmjUnkHeader {
    uint32_t class_count;
    uint32_t reserved;
    uint64_t runs_offset; // WordIdRun[class_count], indexed by category ordinal
    uint64_t entries_offset;
    uint64_t entries_count; // WordEntry[entries_count]
    uint64_t features_offset;
    uint64_t features_bytes;
};

// 12 bytes. Indexed by word id (a plain 0-based index here).
struct WordEntry {
    int16_t left_id;
    int16_t right_id;
    int16_t word_cost;
    uint16_t pad;            // keep 4-byte alignment; reserved
    uint32_t feature_offset; // byte offset into the features blob; 0xFFFFFFFF == none
};

// 8 bytes. system.bin: indexed by trie value. unkdict.bin: indexed by category ordinal.
struct WordIdRun {
    uint32_t entry_start; // first word id in this run
    uint32_t count;       // number of entries in this run
};

// 4 bytes. Indexed by category ordinal.
struct CategoryDef {
    uint8_t invoke;  // 0/1
    uint8_t group;   // 0/1
    uint16_t length; // max grouping length (kept for fidelity; Lucene ignores it)
};
#pragma pack(pop)

inline constexpr uint32_t KMJ_NO_FEATURE = 0xFFFFFFFFU;

// Connection cost accessor (MeCab/Lucene convention, verbatim):
//   forward_id = left node's right-context-id; backward_id = right node's left-context-id.
inline int16_t connection_cost(const int16_t* cells, uint32_t forward_size, uint32_t forward_id,
                               uint32_t backward_id) {
    return cells[static_cast<uint64_t>(backward_id) * forward_size + forward_id];
}

} // namespace doris::segment_v2::inverted_index::kuromoji
