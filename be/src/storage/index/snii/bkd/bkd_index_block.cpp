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

#include "storage/index/snii/bkd/bkd_index_block.h"

#include <algorithm>
#include <cstring>
#include <limits>

#include "storage/index/snii/bkd/bkd_format.h"
#include "storage/index/snii/encoding/byte_source.h"
#include "storage/index/snii/encoding/section_framer.h"
#include "storage/types.h"

namespace doris::snii::bkd {

namespace {

// The field types a native BKD index can be built for: exactly the non-string
// instantiations of InvertedIndexColumnWriter. A string type is excluded because
// it has no fixed-width sortable-bytes representation (INV-2).
//
// field_type is read from disk, so an unrecognised value must be rejected HERE
// and never cast into FieldType and passed to field_type_size(), which
// LOG(FATAL)s on anything outside its own switch -- that would turn a
// recoverable index downgrade into a node crash (design 8).
constexpr FieldType kIndexableFieldTypes[] = {
        FieldType::OLAP_FIELD_TYPE_BOOL,         FieldType::OLAP_FIELD_TYPE_TINYINT,
        FieldType::OLAP_FIELD_TYPE_SMALLINT,     FieldType::OLAP_FIELD_TYPE_INT,
        FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT, FieldType::OLAP_FIELD_TYPE_BIGINT,
        FieldType::OLAP_FIELD_TYPE_LARGEINT,     FieldType::OLAP_FIELD_TYPE_FLOAT,
        FieldType::OLAP_FIELD_TYPE_DOUBLE,       FieldType::OLAP_FIELD_TYPE_DECIMAL,
        FieldType::OLAP_FIELD_TYPE_DECIMAL32,    FieldType::OLAP_FIELD_TYPE_DECIMAL64,
        FieldType::OLAP_FIELD_TYPE_DECIMAL128I,  FieldType::OLAP_FIELD_TYPE_DECIMAL256,
        FieldType::OLAP_FIELD_TYPE_DATE,         FieldType::OLAP_FIELD_TYPE_DATETIME,
        FieldType::OLAP_FIELD_TYPE_DATEV2,       FieldType::OLAP_FIELD_TYPE_DATETIMEV2,
        FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ,  FieldType::OLAP_FIELD_TYPE_IPV4,
        FieldType::OLAP_FIELD_TYPE_IPV6,
};

// Resolves a raw on-disk field_type code and the bytes_per_dim it implies.
// Compares against the enumerators as integers rather than casting the untrusted
// code into the enum first.
bool resolve_field_type(uint32_t raw, FieldType* type, uint32_t* bytes_per_dim) {
    const auto* match = std::ranges::find(kIndexableFieldTypes, raw, [](FieldType candidate) {
        return static_cast<uint32_t>(candidate);
    });
    if (match == std::ranges::end(kIndexableFieldTypes)) {
        return false;
    }
    *type = *match;
    *bytes_per_dim = static_cast<uint32_t>(field_type_size(*match));
    return true;
}

// Every rejection of untrusted bytes funnels through here. Disk data is NOT an
// invariant, so none of these may be a DORIS_CHECK: the caller downgrades to a
// scan, it does not abort the process (design 8).
Status corrupted(std::string_view what) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("bkd_index: {}", what);
}

// Upper bound on the encoded size of one leaf-directory row: a varint64 offset
// delta plus a varint32 count. Used only to size the write buffer.
constexpr size_t kMaxLeafDirectoryRowBytes = 10 + 5;
// Upper bound on the encoded header: fixed32 magic plus eight varints.
constexpr size_t kMaxHeaderBytes = 4 + 8 * 10;

// Decodes the fixed header and establishes its self-consistency (design 5.1).
// Everything downstream -- array strides, allocation sizes, the KeyCoder the
// query side resolves -- is derived from these fields, so they are checked before
// a single array byte is touched.
Status decode_header(ByteSource* src, BkdIndexHeader* header) {
    uint32_t magic = 0;
    RETURN_IF_ERROR(src->get_fixed32(&magic));
    if (magic != kBkdIndexMagic) {
        return corrupted("bad magic");
    }

    // Read before anything else is interpreted: a future layout may differ field
    // by field, so the capability answer must not depend on the rest parsing.
    uint32_t format_version = 0;
    RETURN_IF_ERROR(src->get_varint32(&format_version));
    if (format_version > kSupportedVersion) {
        // A capability boundary, NOT damage: the caller reports "index
        // unavailable" and falls back to a scan (design 3).
        return Status::Error<ErrorCode::INVERTED_INDEX_NOT_SUPPORTED, false>(
                "bkd_index: format_version {} is above the supported {}", format_version,
                kSupportedVersion);
    }
    if (format_version == 0) {
        // No binary ever wrote version 0, so this is damage rather than a format
        // from the future.
        return corrupted("format_version is zero");
    }
    header->format_version = format_version;

    RETURN_IF_ERROR(src->get_varint32(&header->flags));
    RETURN_IF_ERROR(src->get_varint32(&header->bytes_per_dim));
    uint32_t raw_field_type = 0;
    RETURN_IF_ERROR(src->get_varint32(&raw_field_type));
    RETURN_IF_ERROR(src->get_varint64(&header->point_count));
    RETURN_IF_ERROR(src->get_varint32(&header->doc_count));
    RETURN_IF_ERROR(src->get_varint32(&header->leaf_count));
    RETURN_IF_ERROR(src->get_varint32(&header->points_per_leaf));

    uint32_t expected_bytes_per_dim = 0;
    if (!resolve_field_type(raw_field_type, &header->field_type, &expected_bytes_per_dim)) {
        return corrupted("field_type is not an indexable numeric type");
    }
    // INV-2: the fixed width is what makes the split array binary-searchable and
    // the build-time record memcmp-comparable. A width that disagrees with the
    // recorded type would read every array at the wrong stride.
    if (header->bytes_per_dim != expected_bytes_per_dim) {
        return corrupted("bytes_per_dim disagrees with field_type");
    }
    // points_per_leaf is the only quantity that can bound a leaf's count, and a
    // leaf's count is what sizes the doc id vector in leaf_codec. It must itself
    // be bounded before anything downstream leans on it. The builder always
    // records a non-zero value (bkd_builder.cpp DORIS_CHECK_GT in create, and
    // finish writes it unconditionally -- including for the empty index), so
    // rejecting zero cannot reject a legitimate file.
    if (header->points_per_leaf == 0 || header->points_per_leaf > kMaxPointsPerLeaf) {
        return corrupted("points_per_leaf is outside the supported range");
    }
    return Status::OK();
}

// Non-decreasing rather than strictly increasing: a value spanning several leaves
// makes consecutive leaves start at the same value. Comparison is unsigned
// byte-wise from offset 0 (INV-1), i.e. plain memcmp over the sortable bytes.
// An unordered array would silently route the binary search to the wrong leaf --
// wrong results with no error -- so it must never survive open.
Status validate_split_order(Slice splits, size_t bytes_per_dim, uint32_t leaf_count) {
    for (uint32_t i = 1; i + 1 < leaf_count; ++i) {
        const uint8_t* previous = splits.data() + (i - 1) * bytes_per_dim;
        const uint8_t* current = splits.data() + i * bytes_per_dim;
        if (std::memcmp(previous, current, bytes_per_dim) > 0) {
            return corrupted("split_values are not in ascending order");
        }
    }
    return Status::OK();
}

// Leaf directory: delta-varint64 offsets, then varint32 counts (design 5.1).
// Both arrays are validated against the header here so the query path can index
// them unchecked.
Status decode_leaf_directory(ByteSource* src, const BkdIndexHeader& header, uint64_t data_length,
                             std::vector<LeafRef>* leaves) {
    std::vector<LeafRef> decoded(header.leaf_count);

    uint64_t offset = 0;
    for (uint32_t i = 0; i < header.leaf_count; ++i) {
        uint64_t delta = 0;
        RETURN_IF_ERROR(src->get_varint64(&delta));
        // The first delta is the absolute offset of leaf 0; from the second leaf
        // on a zero delta would mean two leaves sharing a start.
        if (i != 0 && delta == 0) {
            return corrupted("leaf offsets are not strictly increasing");
        }
        if (delta > std::numeric_limits<uint64_t>::max() - offset) {
            return corrupted("leaf offsets overflow");
        }
        offset += delta;
        decoded[i].offset = offset;
    }
    // `offset` is now the last leaf's offset. Bounding it against the companion
    // sub-file here is what lets a leaf read skip re-validating its offset.
    if (offset > data_length) {
        return corrupted("last leaf offset is beyond the bkd_data length");
    }

    uint64_t total_points = 0;
    for (uint32_t i = 0; i < header.leaf_count; ++i) {
        uint32_t count = 0;
        RETURN_IF_ERROR(src->get_varint32(&count));
        // The sum identity below is not enough on its own: point_count and the
        // counts can be inflated TOGETHER and still agree, leaving the leaf
        // decode allocation sized by a number that came straight off disk. This
        // is the invariant bkd_types.h documents on LeafRef::count; enforcing it
        // here is what makes it true rather than aspirational.
        if (count > header.points_per_leaf) {
            return corrupted("a leaf count exceeds points_per_leaf");
        }
        decoded[i].count = count;
        // leaf_count and each count are uint32, so the sum cannot overflow 64 bits.
        total_points += count;
    }
    if (total_points != header.point_count) {
        return corrupted("leaf counts do not sum to point_count");
    }
    *leaves = std::move(decoded);
    return Status::OK();
}

} // namespace

void encode_bkd_index_block(const BkdIndexHeader& header, Slice min_value, Slice max_value,
                            Slice split_values, std::span<const LeafRef> leaves, ByteSink* sink) {
    DORIS_CHECK(sink != nullptr);
    DORIS_CHECK_EQ(header.format_version, kFormatVersion);
    DORIS_CHECK_EQ(leaves.size(), static_cast<size_t>(header.leaf_count));

    FieldType field_type {};
    uint32_t expected_bytes_per_dim = 0;
    DORIS_CHECK(resolve_field_type(static_cast<uint32_t>(header.field_type), &field_type,
                                   &expected_bytes_per_dim));
    DORIS_CHECK_EQ(header.bytes_per_dim, expected_bytes_per_dim);

    ByteSink payload;
    payload.reserve(kMaxHeaderBytes + min_value.size() + max_value.size() + split_values.size() +
                    leaves.size() * kMaxLeafDirectoryRowBytes);
    payload.put_fixed32(kBkdIndexMagic);
    payload.put_varint32(header.format_version);
    payload.put_varint32(header.flags);
    payload.put_varint32(header.bytes_per_dim);
    payload.put_varint32(static_cast<uint32_t>(header.field_type));
    payload.put_varint64(header.point_count);
    payload.put_varint32(header.doc_count);
    payload.put_varint32(header.leaf_count);
    payload.put_varint32(header.points_per_leaf);

    if (header.leaf_count == 0) {
        // The empty index is header-only (design 5.3).
        DORIS_CHECK_EQ(header.point_count, 0);
        DORIS_CHECK(min_value.empty());
        DORIS_CHECK(max_value.empty());
        DORIS_CHECK(split_values.empty());
    } else {
        const size_t bytes_per_dim = header.bytes_per_dim;
        DORIS_CHECK_EQ(min_value.size(), bytes_per_dim);
        DORIS_CHECK_EQ(max_value.size(), bytes_per_dim);
        DORIS_CHECK_EQ(split_values.size(), (header.leaf_count - 1) * bytes_per_dim);
        payload.put_bytes(min_value);
        payload.put_bytes(max_value);
        payload.put_bytes(split_values);

        // The reader binary-searches this array and indexes the leaf directory
        // without re-checking, so both orderings are asserted where they are
        // produced. Comparison is unsigned byte-wise from offset 0 (INV-1), which
        // is exactly memcmp over the sortable bytes.
        for (uint32_t i = 1; i + 1 < header.leaf_count; ++i) {
            DORIS_CHECK_LE(std::memcmp(split_values.data() + (i - 1) * bytes_per_dim,
                                       split_values.data() + i * bytes_per_dim, bytes_per_dim),
                           0);
        }

        uint64_t previous_offset = 0;
        uint64_t total_points = 0;
        for (size_t i = 0; i < leaves.size(); ++i) {
            const uint64_t offset = leaves[i].offset;
            // Strictly increasing from the second leaf on; the first delta is the
            // absolute offset of leaf 0 inside bkd_data.
            DORIS_CHECK(i == 0 || offset > previous_offset);
            payload.put_varint64(offset - previous_offset);
            previous_offset = offset;
            total_points += leaves[i].count;
        }
        DORIS_CHECK_EQ(total_points, header.point_count);
        for (const LeafRef& leaf : leaves) {
            payload.put_varint32(leaf.count);
        }
    }

    // The type + length + crc32c envelope comes from the framer; nothing here
    // hand-rolls a checksum.
    SectionFramer::write(*sink, kBkdIndexSectionType, payload.view());
}

Status BkdIndexBlockReader::open(Slice framed, uint64_t data_length, BkdIndexBlockReader* out) {
    DORIS_CHECK(out != nullptr);

    ByteSource src(framed);
    FramedSection section;
    RETURN_IF_ERROR(SectionFramer::read(src, &section));
    if (!src.eof()) {
        return corrupted("trailing bytes after the framed section");
    }
    if (section.type != kBkdIndexSectionType) {
        return corrupted("section type is not kBkdIndexSectionType");
    }
    return out->decode_payload(section.payload, data_length);
}

Status BkdIndexBlockReader::decode_payload(Slice payload, uint64_t data_length) {
    ByteSource src(payload);
    BkdIndexHeader header;
    RETURN_IF_ERROR(decode_header(&src, &header));

    if (header.leaf_count == 0) {
        // The empty index (design 5.3): legal, explicit, header-only.
        if (header.point_count != 0) {
            return corrupted("empty index carries a non-zero point_count");
        }
        if (!src.eof()) {
            return corrupted("empty index carries trailing payload bytes");
        }
        header_ = header;
        bounds_.clear();
        split_values_.clear();
        leaves_.clear();
        return Status::OK();
    }

    const uint64_t bytes_per_dim = header.bytes_per_dim;
    const uint64_t leaf_count = header.leaf_count;
    // Bound leaf_count against the bytes actually present BEFORE anything is
    // sized by it: min + max + one split value per gap, plus at least one byte
    // for each leaf's offset delta and count. A damaged leaf_count would
    // otherwise drive a multi-gigabyte reservation.
    const uint64_t minimum_remaining = (leaf_count + 1) * bytes_per_dim + 2 * leaf_count;
    if (minimum_remaining > src.remaining()) {
        return corrupted("leaf_count exceeds what the payload can describe");
    }

    Slice bounds;
    RETURN_IF_ERROR(src.get_bytes(2 * static_cast<size_t>(bytes_per_dim), &bounds));

    Slice splits;
    RETURN_IF_ERROR(src.get_bytes(static_cast<size_t>((leaf_count - 1) * bytes_per_dim), &splits));
    RETURN_IF_ERROR(
            validate_split_order(splits, static_cast<size_t>(bytes_per_dim), header.leaf_count));

    std::vector<LeafRef> leaves;
    RETURN_IF_ERROR(decode_leaf_directory(&src, header, data_length, &leaves));
    if (!src.eof()) {
        return corrupted("trailing payload bytes after the leaf directory");
    }

    // Everything validated: commit. Members are only written once the whole
    // payload is known good, so a failed open leaves no half-decoded state.
    header_ = header;
    bounds_.assign(bounds.data(), bounds.data() + bounds.size());
    split_values_.assign(splits.data(), splits.data() + splits.size());
    leaves_ = std::move(leaves);
    return Status::OK();
}

size_t BkdIndexBlockReader::heap_bytes() const {
    return bounds_.capacity() + split_values_.capacity() + leaves_.capacity() * sizeof(LeafRef);
}

} // namespace doris::snii::bkd
