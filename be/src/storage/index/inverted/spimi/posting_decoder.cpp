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

#include "storage/index/inverted/spimi/posting_decoder.h"

#include <algorithm>

#include "common/logging.h"
#include "storage/index/inverted/spimi/byte_parser_error.h"
#include "storage/index/inverted/spimi/freq_prox_encoder.h"
#include "storage/index/inverted/spimi/pfor_encoder.h"

namespace doris::segment_v2::inverted_index::spimi {

namespace {

// Minimal byte-stream cursor (same pattern as term_docs_reader.cpp
// and term_enum.cpp — duplicated to keep each decoder self-contained).
class Cursor {
public:
    Cursor(const uint8_t* data, size_t len) : _data(data), _len(len) {}

    uint8_t ReadByte() {
        if (_pos >= _len) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder: read past end of buffer");
        }
        return _data[_pos++];
    }
    int32_t ReadVInt() {
        uint32_t v = 0;
        uint32_t shift = 0;
        while (true) {
            const uint8_t b = ReadByte();
            v |= static_cast<uint32_t>(b & 0x7FU) << shift;
            if ((b & 0x80U) == 0) {
                break;
            }
            shift += 7;
            if (shift >= 32U) [[unlikely]] {
                SPIMI_THROW_CORRUPT("PostingDecoder VInt: shift overflow");
            }
        }
        return static_cast<int32_t>(v);
    }
    void ReadInto(std::vector<uint8_t>* out, size_t n) {
        if (_pos + n > _len || _pos + n < _pos) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder ReadInto: bounds violation");
        }
        out->insert(out->end(), _data + _pos, _data + _pos + n);
        _pos += n;
    }
    size_t pos() const { return _pos; }

private:
    const uint8_t* _data;
    size_t _len;
    size_t _pos = 0;
};

// Decodes consecutive PFOR sub-blocks until `count` values are
// recovered.  Same logic as `DecodePforRun` in term_docs_reader.cpp.
std::vector<uint32_t> DecodePforRun(Cursor& cur, int32_t count) {
    std::vector<uint32_t> values;
    constexpr size_t kSafeReserveCap = 1U << 24;
    values.reserve(std::min(static_cast<size_t>(count), kSafeReserveCap));
    int32_t collected = 0;
    while (collected < count) {
        const size_t mark = cur.pos();
        const int32_t n = cur.ReadVInt();
        if (n <= 0 || n > count - collected) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR sub-block count out of range");
        }
        const uint8_t width = cur.ReadByte() & 0x3FU;
        if (width == 0 || width > 32U) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR invalid bit width");
        }
        const size_t bit_bytes = (static_cast<size_t>(n) * width + 7U) / 8U;
        std::vector<uint8_t> block;
        block.reserve((cur.pos() - mark) + bit_bytes);
        // Re-emit the VInt(n) bytes.
        {
            uint32_t vn = static_cast<uint32_t>(n);
            while ((vn & ~0x7FU) != 0) {
                block.push_back(static_cast<uint8_t>((vn & 0x7FU) | 0x80U));
                vn >>= 7U;
            }
            block.push_back(static_cast<uint8_t>(vn));
        }
        block.push_back(width);
        cur.ReadInto(&block, bit_bytes);
        std::vector<uint32_t> sub;
        SpimiPforDecoder::DecodeBlockFromBytes(block, &sub);
        if (sub.size() != static_cast<size_t>(n)) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder PFOR sub-block decoded count mismatch");
        }
        values.insert(values.end(), sub.begin(), sub.end());
        collected += n;
    }
    if (collected != count) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder PFOR run total mismatch");
    }
    return values;
}

// Decodes position deltas from the `.prx` stream for all documents.
// The `.prx` format for a term is:
//   for each doc d_i (in ascending order):
//     for each position p_j (in ascending order within d_i):
//       vint  position_delta_j   (delta resets to 0 at every new doc)
//
// `docs` must already have doc_id and freq populated.
void DecodePositions(const uint8_t* prx_data, size_t prx_length,
                     std::vector<DecodedDoc>& docs) {
    if (prx_data == nullptr || prx_length == 0) {
        return;
    }
    Cursor prx(prx_data, prx_length);
    for (auto& doc : docs) {
        doc.positions.reserve(static_cast<size_t>(doc.freq));
        int32_t last_pos = 0;
        for (int32_t j = 0; j < doc.freq; ++j) {
            const int32_t delta = prx.ReadVInt();
            last_pos += delta;
            doc.positions.push_back(last_pos);
        }
    }
}

} // namespace

std::vector<DecodedDoc> PostingDecoder::Decode(const uint8_t* frq_data, size_t frq_length,
                                                const uint8_t* prx_data, size_t prx_length,
                                                int32_t doc_freq, bool has_prox) {
    if (doc_freq <= 0 || frq_length == 0U) [[unlikely]] {
        SPIMI_THROW_CORRUPT("PostingDecoder: bad doc_freq / buffer length");
    }

    Cursor cur(frq_data, frq_length);
    const uint8_t mode = cur.ReadByte();

    std::vector<DecodedDoc> docs;
    constexpr size_t kSafeReserveCap = 1U << 24;
    docs.reserve(std::min(static_cast<size_t>(doc_freq), kSafeReserveCap));

    if (mode == FreqProxEncoder::kCodeModeDefault) {
        const int32_t recorded = cur.ReadVInt();
        if (recorded != doc_freq) [[unlikely]] {
            SPIMI_THROW_CORRUPT("PostingDecoder .frq kDefault docCount disagrees with .tis");
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            DecodedDoc d;
            if (has_prox) {
                const uint32_t code = static_cast<uint32_t>(cur.ReadVInt());
                last_doc += static_cast<int32_t>(code >> 1U);
                d.freq = ((code & 1U) != 0) ? 1 : cur.ReadVInt();
            } else {
                last_doc += static_cast<int32_t>(cur.ReadVInt());
                d.freq = 1;
            }
            d.doc_id = last_doc;
            docs.push_back(std::move(d));
        }
    } else if (mode == FreqProxEncoder::kCodeModeSpimiPfor) {
        const auto doc_deltas = DecodePforRun(cur, doc_freq);
        std::vector<uint32_t> freqs;
        if (has_prox) {
            freqs = DecodePforRun(cur, doc_freq);
        }
        int32_t last_doc = 0;
        for (int32_t i = 0; i < doc_freq; ++i) {
            DecodedDoc d;
            last_doc += static_cast<int32_t>(doc_deltas[static_cast<size_t>(i)]);
            d.doc_id = last_doc;
            d.freq = has_prox ? static_cast<int32_t>(freqs[static_cast<size_t>(i)]) : 1;
            docs.push_back(std::move(d));
        }
    } else {
        SPIMI_THROW_CORRUPT("PostingDecoder: unknown .frq CodeMode byte");
    }

    // Decode positions from the .prx stream.
    if (has_prox) {
        DecodePositions(prx_data, prx_length, docs);
    }

    return docs;
}

} // namespace doris::segment_v2::inverted_index::spimi
