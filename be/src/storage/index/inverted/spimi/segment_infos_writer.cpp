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

#include "storage/index/inverted/spimi/segment_infos_writer.h"

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

void SegmentInfosWriter::WriteWideString(ByteOutput* out, const std::string& utf8) {
    const std::wstring wide = Utf8ToWide(utf8);
    const auto len = static_cast<int32_t>(wide.size());
    out->WriteVInt(len);
    out->WriteSCharsFromWide(wide.data(), len);
}

void SegmentInfosWriter::WriteSegment(ByteOutput* out, const SegmentInfoEntry& seg) {
    WriteWideString(out, seg.name);
    out->WriteInt(seg.doc_count);
    out->WriteLong(seg.del_gen);
    out->WriteInt(seg.doc_store_offset);
    if (seg.doc_store_offset != -1) {
        WriteWideString(out, seg.doc_store_segment);
        out->WriteByte(seg.doc_store_is_compound_file ? 1U : 0U);
    }
    out->WriteByte(seg.has_single_norm_file ? 1U : 0U);
    // We never carry per-field separate norms files; emit the NO sentinel
    // so the reader knows there is no normGen array to follow.
    out->WriteInt(kNoNormGen);
    out->WriteByte(static_cast<uint8_t>(seg.is_compound_file));
}

void SegmentInfosWriter::WriteSegmentsN(ByteOutput* out, int64_t version, int32_t counter,
                                        const std::vector<SegmentInfoEntry>& segments) const {
    DCHECK(out != nullptr);
    out->WriteInt(kFormatSharedDocStore);
    out->WriteLong(version);
    out->WriteInt(counter);
    out->WriteInt(static_cast<int32_t>(segments.size()));
    for (const auto& seg : segments) {
        WriteSegment(out, seg);
    }
}

void SegmentInfosWriter::WriteSegmentsGen(ByteOutput* out, int64_t generation) const {
    DCHECK(out != nullptr);
    out->WriteInt(kFormatLockless);
    out->WriteLong(generation);
    out->WriteLong(generation);
}

} // namespace doris::segment_v2::inverted_index::spimi
