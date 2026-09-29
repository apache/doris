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

#include "storage/index/snii/format/null_bitmap.h"

#include <algorithm>
#include <limits>
#include <span>
#include <vector>

#include "common/check.h"
// clang-format off
// CRoaring's public header defines ROARING_CONTAINER_T; its internal headers undefine it.
#include "roaring/roaring.hh"
#include "roaring/containers/array.h"
#include "roaring/containers/bitset.h"
#include "roaring/containers/run.h"
// clang-format on
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/index/snii/encoding/varint.h"

namespace doris::snii::format {

namespace {

constexpr uint32_t kPortableCookieNoRun = 12346;
constexpr uint16_t kPortableCookieRun = 12347;
constexpr uint32_t kMaxPortableContainers = uint32_t {1} << 16;

struct ParsedNullBitmap {
    uint32_t doc_count = 0;
    Slice roaring_bytes;
    uint32_t container_count = 0;
};

Status parse_null_bitmap(Slice framed, ParsedNullBitmap* out) {
    ByteSource src(framed);
    FramedSection sec;
    RETURN_IF_ERROR(SectionFramer::read(src, &sec));
    if (sec.type != kNullBitmapSectionType || src.remaining() != 0) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "null bitmap: invalid framed section");
    }

    ByteSource payload(sec.payload);
    uint64_t doc_count = 0;
    RETURN_IF_ERROR(payload.get_varint64(&doc_count));
    if (doc_count > std::numeric_limits<uint32_t>::max()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "null bitmap doc_count overflows uint32");
    }

    uint64_t roaring_size = 0;
    RETURN_IF_ERROR(payload.get_varint64(&roaring_size));
    if (roaring_size != payload.remaining()) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "null bitmap roaring_size differs from payload");
    }
    RETURN_IF_ERROR(payload.get_bytes(static_cast<size_t>(roaring_size), &out->roaring_bytes));

    ByteSource portable(out->roaring_bytes);
    uint32_t cookie = 0;
    RETURN_IF_ERROR(portable.get_fixed32(&cookie));
    if (static_cast<uint16_t>(cookie) == kPortableCookieRun) {
        out->container_count = (cookie >> 16) + 1;
    } else if (cookie == kPortableCookieNoRun) {
        RETURN_IF_ERROR(portable.get_fixed32(&out->container_count));
        if (out->container_count > kMaxPortableContainers) {
            return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                    "null bitmap: portable container count out of range");
        }
    } else {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "null bitmap: invalid portable cookie");
    }

    const char* data = reinterpret_cast<const char*>(out->roaring_bytes.data());
    const size_t size = out->roaring_bytes.size();
    const size_t probed = roaring::api::roaring_bitmap_portable_deserialize_size(data, size);
    if (probed == 0 || probed != size) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "null bitmap: malformed roaring container");
    }
    out->doc_count = static_cast<uint32_t>(doc_count);
    return Status::OK();
}

} // namespace

NullBitmapWriter::
        NullBitmapWriter() // NOLINT(modernize-use-equals-default): roaring type is incomplete in the header.
        : bitmap_(std::make_unique<roaring::Roaring>()) {}

NullBitmapWriter::~NullBitmapWriter() = default;

void NullBitmapWriter::add_null(uint32_t docid) {
    bitmap_->add(docid);
}

void NullBitmapWriter::add_many(std::span<const uint32_t> docids) {
    bitmap_->addMany(docids.size(), docids.data());
}

uint32_t NullBitmapWriter::null_count() const {
    return static_cast<uint32_t>(bitmap_->cardinality());
}

uint64_t NullBitmapWriter::build_memory_upper_bound(std::span<const uint32_t> sorted_docids) {
    if (sorted_docids.empty()) {
        return 0;
    }

    uint64_t container_count = 0;
    uint64_t sparse_container_count = 0;
    uint64_t sparse_value_count = 0;
    uint64_t dense_container_count = 0;
    size_t begin = 0;
    while (begin < sorted_docids.size()) {
        const uint32_t key = sorted_docids[begin] >> 16;
        size_t end = begin + 1;
        while (end < sorted_docids.size() && sorted_docids[end] >> 16 == key) {
            DCHECK_GT(sorted_docids[end], sorted_docids[end - 1]);
            ++end;
        }
        const uint64_t cardinality = end - begin;
        ++container_count;
        if (cardinality <= roaring::internal::DEFAULT_MAX_SIZE) {
            ++sparse_container_count;
            sparse_value_count += cardinality;
        } else {
            ++dense_container_count;
        }
        begin = end;
    }

    // roaring_array_t uses three parallel arrays. Account a minimum allocation
    // of four slots and old+replacement overlap at geometric growth.
    constexpr uint64_t kTopEntryBytes = sizeof(void*) + sizeof(uint16_t) + sizeof(uint8_t);
    const uint64_t top_capacity = std::max<uint64_t>(container_count, 4);
    const uint64_t top_array_peak = 3 * top_capacity * kTopEntryBytes;

    // Sparse array growth can retain the old uint16 array while allocating its
    // replacement. Eight bytes per live value covers both capacity slack and
    // replacement overlap. A dense container additionally covers the largest
    // geometric array capacity immediately before conversion and the new 8 KiB
    // bitset while both allocations are live.
    constexpr uint64_t kSparseValuePeakBytes = 8;
    constexpr uint64_t kBitsetBytes =
            roaring::internal::BITSET_CONTAINER_SIZE_IN_WORDS * sizeof(uint64_t);
    constexpr uint64_t kDenseArrayConversionCapacity = roaring::internal::DEFAULT_MAX_SIZE * 5 / 4;
    constexpr uint64_t kDenseArrayConversionBytes =
            kDenseArrayConversionCapacity * sizeof(uint16_t);
    const uint64_t sparse_peak =
            sparse_value_count * kSparseValuePeakBytes +
            sparse_container_count * sizeof(roaring::internal::array_container_t);
    const uint64_t dense_peak =
            dense_container_count * (kDenseArrayConversionBytes + kBitsetBytes +
                                     sizeof(roaring::internal::array_container_t) +
                                     sizeof(roaring::internal::bitset_container_t));
    return sizeof(roaring::Roaring) + top_array_peak + sparse_peak + dense_peak;
}

Status NullBitmapWriter::serialization_sizes(uint32_t doc_count,
                                             NullBitmapSerializationSizes* out) const {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "null bitmap: null serialization size output");
    }
    const size_t roaring_bytes = bitmap_->getSizeInBytes();
    const size_t prefix_bytes = varint_len(doc_count) + varint_len(roaring_bytes);
    if (roaring_bytes > std::numeric_limits<size_t>::max() - prefix_bytes) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "null bitmap: payload size overflows");
    }
    const size_t payload_bytes = prefix_bytes + roaring_bytes;
    const size_t envelope_bytes = 1 + varint_len(payload_bytes) + sizeof(uint32_t);
    if (payload_bytes > std::numeric_limits<size_t>::max() - envelope_bytes) {
        return Status::Error<ErrorCode::MEM_LIMIT_EXCEEDED, false>(
                "null bitmap: framed size overflows");
    }
    *out = {.roaring_bytes = roaring_bytes,
            .payload_bytes = payload_bytes,
            .framed_bytes = envelope_bytes + payload_bytes};
    return Status::OK();
}

Status NullBitmapWriter::finish(uint32_t doc_count, ByteSink* sink) const {
    if (sink == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>("null bitmap: null output sink");
    }
    NullBitmapSerializationSizes sizes;
    RETURN_IF_ERROR(serialization_sizes(doc_count, &sizes));

    // Serialize the Roaring bitmap to its portable on-disk form.
    std::vector<char> roaring_buf(sizes.roaring_bytes);
    bitmap_->write(roaring_buf.data());

    // Build inner payload: [varint64 doc_count][varint64 roaring_size][bytes].
    ByteSink payload;
    payload.reserve(sizes.payload_bytes);
    payload.put_varint64(doc_count);
    payload.put_varint64(sizes.roaring_bytes);
    payload.put_bytes(
            Slice(reinterpret_cast<const uint8_t*>(roaring_buf.data()), sizes.roaring_bytes));
    DORIS_CHECK_EQ(payload.size(), sizes.payload_bytes);

    // Delegate the type + len + crc32c envelope to SectionFramer.
    const size_t start = sink->size();
    sink->reserve(sizes.framed_bytes);
    SectionFramer::write(*sink, kNullBitmapSectionType, payload.view());
    DORIS_CHECK_EQ(sink->size() - start, sizes.framed_bytes);
    return Status::OK();
}

NullBitmapReader::
        NullBitmapReader() // NOLINT(modernize-use-equals-default): roaring type is incomplete in the header.
        : bitmap_(std::make_unique<roaring::Roaring>()) {}

NullBitmapReader::~NullBitmapReader() = default;

NullBitmapReader::NullBitmapReader(NullBitmapReader&&) noexcept = default;
NullBitmapReader& NullBitmapReader::operator=(NullBitmapReader&&) noexcept = default;

Status NullBitmapReader::open(Slice framed, NullBitmapReader* out) {
    ParsedNullBitmap parsed;
    RETURN_IF_ERROR(parse_null_bitmap(framed, &parsed));
    *out->bitmap_ =
            roaring::Roaring::readSafe(reinterpret_cast<const char*>(parsed.roaring_bytes.data()),
                                       parsed.roaring_bytes.size());
    out->doc_count_ = parsed.doc_count;
    return Status::OK();
}

Status NullBitmapReader::decoded_memory_bytes(Slice framed, uint64_t* out) {
    if (out == nullptr) {
        return Status::Error<ErrorCode::INVALID_ARGUMENT, false>(
                "null bitmap: null decoded memory output");
    }
    ParsedNullBitmap parsed;
    RETURN_IF_ERROR(parse_null_bitmap(framed, &parsed));

    constexpr uint64_t kContainerObjectBytes =
            std::max({sizeof(roaring::internal::array_container_t),
                      sizeof(roaring::internal::bitset_container_t),
                      sizeof(roaring::internal::run_container_t)});
    constexpr uint64_t kContainerMetadataBytes =
            sizeof(void*) + sizeof(uint16_t) + sizeof(uint8_t) + kContainerObjectBytes;
    constexpr uint64_t kFixedBytes =
            sizeof(roaring::Roaring) + sizeof(roaring::api::roaring_bitmap_t);
    const uint64_t container_bytes =
            static_cast<uint64_t>(parsed.container_count) * kContainerMetadataBytes;
    if (parsed.roaring_bytes.size() >
        std::numeric_limits<uint64_t>::max() - container_bytes - kFixedBytes) {
        return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(
                "null bitmap: decoded memory size overflows");
    }
    *out = parsed.roaring_bytes.size() + container_bytes + kFixedBytes;
    return Status::OK();
}

bool NullBitmapReader::is_null(uint32_t docid) const {
    return bitmap_->contains(docid);
}

uint32_t NullBitmapReader::null_count() const {
    return static_cast<uint32_t>(bitmap_->cardinality());
}

void NullBitmapReader::copy_to(roaring::Roaring* out) const {
    *out = *bitmap_;
}

void NullBitmapReader::append_docids(std::vector<uint32_t>& out) const {
    for (uint32_t docid : *bitmap_) {
        out.push_back(docid);
    }
}

} // namespace doris::snii::format
