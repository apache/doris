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

#include "storage/index/inverted/spimi/segment_writer.h"

#include "common/logging.h"

namespace doris::segment_v2::inverted_index::spimi {

SegmentWriter::SegmentWriter(ByteOutput* tis_out, ByteOutput* tii_out, ByteOutput* frq_out,
                             ByteOutput* prx_out, int32_t index_interval, int32_t skip_interval,
                             int32_t max_skip_levels, bool omit_term_freq_and_positions)
        : _dict(tis_out, tii_out, index_interval, skip_interval),
          _encoder(frq_out, prx_out, skip_interval, max_skip_levels, omit_term_freq_and_positions) {
}

int64_t SegmentWriter::Emit(const SpimiPostingBuffer& buffer, int32_t field_number) {
    DCHECK(!_closed) << "SegmentWriter::Emit called after Close()";

    const auto& records = buffer.records();
    if (records.empty()) {
        return 0;
    }

    size_t i = 0;
    int64_t emitted = 0;
    while (i < records.size()) {
        const uint32_t text_ref = records[i].text_ref;

        // Find the end of this term's run (records sharing the text_ref).
        // Sort() groups them contiguously.
        size_t j = i + 1;
        while (j < records.size() && records[j].text_ref == text_ref) {
            ++j;
        }

        // Count distinct docs in [i, j).
        int32_t doc_freq = 1;
        for (size_t k = i + 1; k < j; ++k) {
            if (records[k].doc_id != records[k - 1].doc_id) {
                ++doc_freq;
            }
        }

        _encoder.StartTerm(doc_freq);

        size_t doc_start = i;
        while (doc_start < j) {
            const uint32_t doc_id = records[doc_start].doc_id;
            size_t doc_end = doc_start + 1;
            while (doc_end < j && records[doc_end].doc_id == doc_id) {
                ++doc_end;
            }
            const auto freq = static_cast<int32_t>(doc_end - doc_start);
            _encoder.StartDoc(static_cast<int32_t>(doc_id), freq);
            for (size_t k = doc_start; k < doc_end; ++k) {
                _encoder.AddPosition(static_cast<int32_t>(records[k].position));
            }
            _encoder.FinishDoc();
            doc_start = doc_end;
        }

        const TermInfo info = _encoder.FinishTerm();
        const std::string_view term_text = buffer.TermAt(text_ref);
        _dict.Add(field_number, term_text, info);

        ++_term_count;
        ++emitted;
        i = j;
    }

    return emitted;
}

void SegmentWriter::Close() {
    if (_closed) {
        return;
    }
    _closed = true;
    _dict.Close();
}

} // namespace doris::segment_v2::inverted_index::spimi
