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
#include <string>
#include <vector>

#include "storage/index/inverted/spimi/byte_output.h"

namespace doris::segment_v2::inverted_index::spimi {

// Per-field metadata recorded in the segment's `.fnm`. Mirrors CLucene's
// `FieldInfo` 1:1 so the existing reader interprets the bits identically.
struct FieldInfoEntry {
    std::string name;
    bool is_indexed = true;
    bool store_term_vector = false;
    bool store_position_with_term_vector = false;
    bool store_offset_with_term_vector = false;
    bool omit_norms = true; // Doris fulltext columns do not score with BM25
    bool store_payloads = false;
    bool has_prox = true;      // fulltext supports phrase ⇒ positions are kept
    int32_t index_version = 0; // 0 = kV0 (no PFOR/dict-compress)
    int32_t flags = 0;         // only persisted at index_version >= kV3
};

// Reimplements CLucene's `FieldInfos::write(IndexOutput*)` byte-for-byte
// against a Doris-owned ByteOutput. Format (Lucene 2.x, FORMAT-tracked
// by the field bits):
//
//   vint   field_count
//   per field:
//     vint   name_length             (in wide chars)
//     bytes  name                    (modified UTF-8 from ByteOutput)
//     byte   bits                    (IS_INDEXED|STORE_TERMVECTOR|...|
//                                     TERM_FREQ_AND_POSITIONS|0x80 if V>V0)
//     vint   index_version           (only if index_version > kV0)
//     vint   flags                   (only if index_version >= kV3)
//
// The bit constants match CLucene's FieldInfos enum exactly:
//   IS_INDEXED                            = 0x01
//   STORE_TERMVECTOR                      = 0x02
//   STORE_POSITIONS_WITH_TERMVECTOR       = 0x04
//   STORE_OFFSET_WITH_TERMVECTOR          = 0x08
//   OMIT_NORMS                            = 0x10
//   STORE_PAYLOADS                        = 0x20
//   TERM_FREQ_AND_POSITIONS               = 0x40
//   <has-version-tag>                     = 0x80
class FieldInfosWriter {
public:
    static constexpr uint8_t kIsIndexed = 0x01;
    static constexpr uint8_t kStoreTermVector = 0x02;
    static constexpr uint8_t kStorePositionsWithTermVector = 0x04;
    static constexpr uint8_t kStoreOffsetWithTermVector = 0x08;
    static constexpr uint8_t kOmitNorms = 0x10;
    static constexpr uint8_t kStorePayloads = 0x20;
    static constexpr uint8_t kTermFreqAndPositions = 0x40;
    static constexpr uint8_t kHasVersionTag = 0x80;

    static constexpr int32_t kIndexVersionV0 = 0;
    static constexpr int32_t kIndexVersionV1 = 1;
    static constexpr int32_t kIndexVersionV3 = 3;
    // V4 = pure SPIMI windowed `.frq`/`.prx` posting format. Segments written at
    // V4 emit the new outer mode bytes (kCodeModeSpimiWindowed / kProxWindowed);
    // the persisted per-field index_version is the durable format gate so a
    // V0..V3 reader never tries to parse a windowed block. See
    // window_frame_encoder.h for the byte layout.
    static constexpr int32_t kIndexVersionV4 = 4;

    explicit FieldInfosWriter(ByteOutput* out);

    FieldInfosWriter(const FieldInfosWriter&) = delete;
    FieldInfosWriter& operator=(const FieldInfosWriter&) = delete;

    // Emits the full `.fnm` payload (no trailing footer in this format).
    void Write(const std::vector<FieldInfoEntry>& fields);

private:
    static uint8_t ComputeBits(const FieldInfoEntry& fi);

    ByteOutput* _out;
};

} // namespace doris::segment_v2::inverted_index::spimi
