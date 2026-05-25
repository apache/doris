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

#include "storage/index/inverted/spimi/field_infos_writer.h"

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

FieldInfosWriter::FieldInfosWriter(LuceneOutput* out) : _out(out) {
    DCHECK(_out != nullptr);
}

uint8_t FieldInfosWriter::ComputeBits(const FieldInfoEntry& fi) {
    uint8_t bits = 0;
    if (fi.is_indexed) {
        bits |= kIsIndexed;
    }
    if (fi.store_term_vector) {
        bits |= kStoreTermVector;
    }
    if (fi.store_position_with_term_vector) {
        bits |= kStorePositionsWithTermVector;
    }
    if (fi.store_offset_with_term_vector) {
        bits |= kStoreOffsetWithTermVector;
    }
    if (fi.omit_norms) {
        bits |= kOmitNorms;
    }
    if (fi.store_payloads) {
        bits |= kStorePayloads;
    }
    if (fi.has_prox) {
        bits |= kTermFreqAndPositions;
    }
    if (fi.index_version > kIndexVersionV0) {
        bits |= kHasVersionTag;
    }
    return bits;
}

void FieldInfosWriter::Write(const std::vector<FieldInfoEntry>& fields) {
    _out->WriteVInt(static_cast<int32_t>(fields.size()));
    for (const auto& fi : fields) {
        // CLucene's FieldInfos::write encodes the name as a wide-char string
        // via writeString → writeVInt(length) + writeChars (modified UTF-8).
        // The wide-char length is the number of code points in the field
        // name, not bytes.
        const std::wstring wide_name = Utf8ToWide(fi.name);
        const auto wide_len = static_cast<int32_t>(wide_name.size());
        _out->WriteVInt(wide_len);
        _out->WriteSCharsFromWide(wide_name.data(), wide_len);

        const uint8_t bits = ComputeBits(fi);
        _out->WriteByte(bits);

        if (fi.index_version > kIndexVersionV0) {
            _out->WriteVInt(fi.index_version);
        }
        if (fi.index_version >= kIndexVersionV3) {
            _out->WriteVInt(fi.flags);
        }
    }
}

} // namespace doris::segment_v2::inverted_index::spimi
