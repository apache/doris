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

#include "storage/index/inverted/snii/format/norms_pod.h"

#include <algorithm>
#include <cstring>
#include <limits>
#include <string_view>

#include "common/check.h"
#include "storage/index/inverted/snii/common/slice.h"
#include "storage/index/inverted/snii/encoding/byte_source.h"
#include "storage/index/inverted/snii/encoding/section_framer.h"
#include "storage/index/inverted/snii/encoding/varint.h"
#include "storage/index/inverted/snii/format/format_constants.h"
#include "storage/index/inverted/snii/format/null_bitmap.h"

namespace doris::snii::format {

// Storage is a unity build, so the helpers of this file live in their own namespace instead of
// an anonymous one that other files merged into the same TU may also use.
namespace norms_pod_detail {
namespace {

Status sparse_corrupted(std::string_view message) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(message);
}

// [u8 mode][varint64 doc_count][varint64 value_count]
size_t sparse_header_size(uint64_t doc_count, uint64_t value_count) {
    return 1 + varint_len(doc_count) + varint_len(value_count);
}

// Calls fn(begin, end) for each range of rows between the ascending `null_docids`.
template <typename Fn>
void for_each_gap(size_t doc_count, std::span<const uint32_t> null_docids, Fn&& fn) {
    size_t begin = 0;
    for (const uint32_t null_docid : null_docids) {
        DCHECK_GE(null_docid, begin);
        DCHECK_LT(null_docid, doc_count);
        if (begin < null_docid) {
            fn(begin, static_cast<size_t>(null_docid));
        }
        begin = static_cast<size_t>(null_docid) + 1;
    }
    if (begin < doc_count) {
        fn(begin, doc_count);
    }
}

} // namespace
} // namespace norms_pod_detail

void NormsPodWriter::finish(ByteSink* sink) const {
    finish(norms_, sink);
}

void NormsPodWriter::finish(std::span<const uint8_t> norms, ByteSink* sink) {
    // Build inner payload: [varint64 doc_count][raw norm bytes].
    ByteSink payload;
    const size_t payload_size = varint_len(norms.size()) + norms.size();
    payload.reserve(payload_size);
    payload.put_varint64(norms.size());
    payload.put_bytes(Slice(norms.data(), norms.size()));
    // Delegate outer framing to SectionFramer to append type+len+crc32c, avoiding manual checksum assembly.
    sink->reserve(dense_section_size(norms.size()));
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kNormsPod), payload.view());
}

Status NormsPodReader::open(Slice framed, NormsPodReader* out) {
    // framer handles CRC verify, truncation detection, and payload slicing.
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kNormsPod)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD section type mismatch");
    }
    if (!src.eof()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD trailing framed bytes");
    }

    // Parse inner payload: [varint64 doc_count][bytes].
    ByteSource payload(sec.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&doc_count));
    if (payload.position() != varint_len(doc_count)) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD non-canonical doc_count");
    }
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD doc_count overflows uint32");
    }
    // doc_count must exactly equal the remaining byte count (1 byte per doc).
    if (payload.remaining() != doc_count) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "norms POD length mismatch");
    }

    Slice bytes;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(doc_count), &bytes));
    out->doc_count_ = static_cast<uint32_t>(doc_count);
    out->norms_ = bytes.data();
    return Status::OK();
}

size_t NormsPodWriter::dense_section_size(uint64_t doc_count) {
    const size_t payload_size = varint_len(doc_count) + static_cast<size_t>(doc_count);
    return 1 + varint_len(payload_size) + payload_size + sizeof(uint32_t);
}

bool NormsPodWriter::sparse_values_constant(std::span<const uint8_t> norms,
                                            std::span<const uint32_t> null_docids) {
    const uint8_t* shared = nullptr;
    bool constant = true;
    norms_pod_detail::for_each_gap(norms.size(), null_docids, [&](size_t begin, size_t end) {
        if (shared == nullptr) {
            shared = &norms[begin];
        }
        constant = constant && std::all_of(norms.begin() + begin, norms.begin() + end,
                                           [&](uint8_t norm) { return norm == *shared; });
    });
    return shared != nullptr && constant;
}

size_t NormsPodWriter::sparse_section_size(uint64_t doc_count, uint64_t value_count,
                                           bool constant) {
    const uint64_t stored = constant ? 1 : value_count;
    const size_t payload_size = norms_pod_detail::sparse_header_size(doc_count, value_count) +
                                static_cast<size_t>(stored);
    return 1 + varint_len(payload_size) + payload_size + sizeof(uint32_t);
}

void NormsPodWriter::finish_sparse(std::span<const uint8_t> norms,
                                   std::span<const uint32_t> null_docids, bool constant,
                                   ByteSink* sink) {
    DCHECK_EQ(constant, sparse_values_constant(norms, null_docids));
    DORIS_CHECK_LE(null_docids.size(), norms.size());
    const uint64_t value_count = norms.size() - null_docids.size();
    const size_t section_size = sparse_section_size(norms.size(), value_count, constant);
    ByteSink payload;
    payload.reserve(section_size);
    payload.put_u8(
            static_cast<uint8_t>(constant ? SparseNormsMode::kConstant : SparseNormsMode::kValues));
    payload.put_varint64(norms.size());
    payload.put_varint64(value_count);
    bool shared_written = false;
    norms_pod_detail::for_each_gap(norms.size(), null_docids, [&](size_t begin, size_t end) {
        if (!constant) {
            payload.put_bytes(Slice(norms.data() + begin, end - begin));
        } else if (!shared_written) {
            payload.put_u8(norms[begin]);
            shared_written = true;
        }
    });
    sink->reserve(section_size);
    SectionFramer::write(*sink, static_cast<uint8_t>(SectionType::kNormsSparse), payload.view());
}

Status NormsPodReader::open_sparse(Slice framed, uint32_t doc_count, const NullBitmapReader* nulls,
                                   std::vector<uint8_t>* dense, NormsPodReader* out) {
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != static_cast<uint8_t>(SectionType::kNormsSparse)) {
        return norms_pod_detail::sparse_corrupted("sparse norms section type mismatch");
    }
    if (!src.eof()) {
        return norms_pod_detail::sparse_corrupted("sparse norms trailing framed bytes");
    }

    ByteSource payload(sec.payload);
    uint8_t mode = 0;
    uint64_t section_doc_count = 0;
    uint64_t value_count = 0;
    RETURN_IF_ERROR(payload.get_u8(&mode));
    RETURN_IF_ERROR(payload.get_varint64(&section_doc_count));
    RETURN_IF_ERROR(payload.get_varint64(&value_count));
    if (payload.position() !=
        norms_pod_detail::sparse_header_size(section_doc_count, value_count)) {
        return norms_pod_detail::sparse_corrupted("sparse norms non-canonical counts");
    }
    if (section_doc_count != doc_count) {
        return norms_pod_detail::sparse_corrupted("sparse norms doc count mismatch");
    }
    // The values belong to the non-NULL rows of this index: against another null bitmap they
    // would land on the wrong rows.
    if (nulls != nullptr && nulls->doc_count() != doc_count) {
        return norms_pod_detail::sparse_corrupted("sparse norms null bitmap doc count mismatch");
    }
    const uint64_t null_count = nulls == nullptr ? 0 : nulls->null_count();
    if (null_count > doc_count || value_count != doc_count - null_count) {
        return norms_pod_detail::sparse_corrupted("sparse norms value count mismatch");
    }
    const bool constant = mode == static_cast<uint8_t>(SparseNormsMode::kConstant);
    if (!constant && mode != static_cast<uint8_t>(SparseNormsMode::kValues)) {
        return norms_pod_detail::sparse_corrupted("sparse norms unknown mode");
    }
    if (constant && value_count == 0) {
        return norms_pod_detail::sparse_corrupted("sparse norms shared value without rows");
    }
    const uint64_t stored = constant ? 1 : value_count;
    if (payload.remaining() != stored) {
        return norms_pod_detail::sparse_corrupted("sparse norms length mismatch");
    }
    Slice bytes;
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(stored), &bytes));

    dense->assign(doc_count, kNullRowNorm);
    uint64_t placed = 0;
    bool overflow = false;
    auto place = [&](uint32_t begin, uint32_t end) {
        const uint64_t rows = end - begin;
        // A NULL docid at or past doc_count leaves more rows in range than the value count.
        if (overflow || placed + rows > value_count) {
            overflow = true;
            return;
        }
        if (constant) {
            std::memset(dense->data() + begin, bytes.data()[0], rows);
        } else {
            std::memcpy(dense->data() + begin, bytes.data() + placed, rows);
        }
        placed += rows;
    };
    if (nulls == nullptr) {
        place(0, doc_count);
    } else {
        nulls->for_each_non_null_run(place);
    }
    if (overflow || placed != value_count) {
        return norms_pod_detail::sparse_corrupted("sparse norms rows disagree with null bitmap");
    }
    out->doc_count_ = doc_count;
    out->norms_ = dense->data();
    return Status::OK();
}

} // namespace doris::snii::format
