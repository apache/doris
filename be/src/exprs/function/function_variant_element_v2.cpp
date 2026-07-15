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

#include "exprs/function/function_variant_element_v2.h"

#include <limits>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/custom_allocator.h"
#include "util/variant/variant_block_builder.h"
#include "util/variant/variant_encoding.h"
#include "util/variant/variant_tracked_storage.h"

namespace doris {

namespace {

struct OwnedPathSegment {
    VariantElementV2PathSegment::Kind kind;
    VariantTrackedString key;
    int64_t index = 0;
};

} // namespace

struct ResolvedVariantElementV2Path::Impl {
    DorisVector<OwnedPathSegment> segments;
};

namespace variant_element_v2_internal {

bool is_outer_null(std::span<const uint8_t> outer_nulls, size_t row) noexcept {
    return !outer_nulls.empty() && outer_nulls[row] != 0;
}

// Resolves object keys once per dense metadata id and traverses arrays without metadata lookup.
class VariantPathV2BatchReader {
public:
    VariantPathV2BatchReader(const ColumnVariantV2& source,
                             const ResolvedVariantElementV2Path& path)
            : _source(source.read_view()),
              _path(path),
              _resolved_metadata(_source.metadata_count(), 0) {
        DORIS_CHECK_GT(_path.size(), 0);
        if (_source.metadata_count() != 0 &&
            _path.size() > std::numeric_limits<size_t>::max() / _source.metadata_count()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant path cache size overflows size_t");
        }
        _object_ids.assign(_source.metadata_count() * _path.size(), -1);
    }

    bool find_at(size_t row, VariantValueRef* const output) {
        DORIS_CHECK(output != nullptr);
        const uint32_t metadata_id = _source.metadata_id_at(row);
        _resolve_metadata(metadata_id);
        VariantValueRef current = _source.value_at(row);
        const size_t cache_base = static_cast<size_t>(metadata_id) * _path.size();
        for (size_t position = 0; position < _path.size(); ++position) {
            if (_path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
                const int64_t field_id = _object_ids[cache_base + position];
                if (current.basic_type() != VariantBasicType::OBJECT || field_id < 0 ||
                    !current.object_find_by_id(static_cast<uint32_t>(field_id), &current)) {
                    return false;
                }
            } else {
                if (current.basic_type() != VariantBasicType::ARRAY) {
                    return false;
                }
                const int64_t requested_index = _path.array_index_at(position);
                const int64_t element_count = current.num_elements();
                const int64_t resolved_index =
                        requested_index < 0 ? element_count + requested_index : requested_index;
                if (resolved_index < 0 || resolved_index >= element_count) {
                    return false;
                }
                current = current.array_at(static_cast<uint32_t>(resolved_index));
            }
        }
        *output = current;
        return true;
    }

    size_t metadata_count() const noexcept { return _source.metadata_count(); }

    uint32_t metadata_id_at(size_t row) const { return _source.metadata_id_at(row); }

    VariantMetadataRef metadata_at(uint32_t id) { return _source.metadata_at(id); }

private:
    void _resolve_metadata(uint32_t metadata_id) {
        if (_resolved_metadata[metadata_id] != 0) {
            return;
        }
        const VariantMetadataRef metadata = _source.metadata_at(metadata_id);
        const size_t base = static_cast<size_t>(metadata_id) * _path.size();
        for (size_t position = 0; position < _path.size(); ++position) {
            if (_path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
                _object_ids[base + position] = metadata.find_key(_path.object_key_at(position));
            }
        }
        _resolved_metadata[metadata_id] = 1;
    }

    ColumnVariantV2::ReadView _source;
    const ResolvedVariantElementV2Path& _path;
    DorisVector<int64_t> _object_ids;
    DorisVector<uint8_t> _resolved_metadata;
};

Status extract_encoded_variant_element(const ColumnVariantV2& source,
                                       const ResolvedVariantElementV2Path& path,
                                       std::span<const uint8_t> outer_nulls, ColumnPtr* output);

Status make_all_null_variant_element_result(size_t rows, ColumnPtr* output);

} // namespace variant_element_v2_internal

ResolvedVariantElementV2Path::ResolvedVariantElementV2Path(std::unique_ptr<Impl> impl)
        : _impl(std::move(impl)) {}

ResolvedVariantElementV2Path::~ResolvedVariantElementV2Path() = default;
ResolvedVariantElementV2Path::ResolvedVariantElementV2Path(
        ResolvedVariantElementV2Path&&) noexcept = default;
ResolvedVariantElementV2Path& ResolvedVariantElementV2Path::operator=(
        ResolvedVariantElementV2Path&&) noexcept = default;

size_t ResolvedVariantElementV2Path::size() const noexcept {
    return _impl->segments.size();
}

VariantElementV2PathSegment::Kind ResolvedVariantElementV2Path::kind_at(size_t position) const {
    DORIS_CHECK_LT(position, size()) << "Variant element path position is out of range";
    return _impl->segments[position].kind;
}

StringRef ResolvedVariantElementV2Path::object_key_at(size_t position) const {
    DORIS_CHECK(kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY)
            << "Variant element path segment is not an object key";
    const auto& key = _impl->segments[position].key;
    return {key.data(), key.size()};
}

int64_t ResolvedVariantElementV2Path::array_index_at(size_t position) const {
    DORIS_CHECK(kind_at(position) == VariantElementV2PathSegment::Kind::ARRAY_INDEX)
            << "Variant element path segment is not an array index";
    return _impl->segments[position].index;
}

Status resolve_variant_element_v2_path(
        std::span<const VariantElementV2PathSegment> segments,
        // Mutable smart-pointer output is published only after full path validation.
        // NOLINTNEXTLINE(readability-non-const-parameter)
        std::unique_ptr<ResolvedVariantElementV2Path>* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant V2 resolved path output is null");
    }
    if (segments.empty()) {
        return Status::InvalidArgument("Variant V2 element path must not be empty");
    }

    auto impl = std::make_unique<ResolvedVariantElementV2Path::Impl>();
    impl->segments.reserve(segments.size());
    for (const VariantElementV2PathSegment& segment : segments) {
        OwnedPathSegment owned {.kind = segment.kind(), .key = {}, .index = segment.index()};
        if (segment.kind() == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
            if (segment.key().size != 0 && segment.key().data == nullptr) {
                return Status::InvalidArgument("Variant V2 object path key has a null pointer");
            }
            if (segment.key().size != 0) {
                owned.key.assign(segment.key().data, segment.key().size);
            }
        }
        impl->segments.push_back(std::move(owned));
    }

    auto candidate = std::unique_ptr<ResolvedVariantElementV2Path>(
            new ResolvedVariantElementV2Path(std::move(impl)));
    output->swap(candidate);
    return Status::OK();
}

Status extract_variant_element_v2(const ColumnVariantV2& source,
                                  const ResolvedVariantElementV2Path& path,
                                  // Mutable smart-pointer output is published only on success.
                                  // NOLINTNEXTLINE(readability-non-const-parameter)
                                  std::span<const uint8_t> outer_nulls, ColumnPtr* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant V2 element output is null");
    }
    if (path.size() == 0) {
        return Status::InvalidArgument("Variant V2 element path must not be empty");
    }
    if (!outer_nulls.empty() && outer_nulls.size() != source.size()) {
        return Status::InvalidArgument("Variant V2 outer null map has {} rows, expected {}",
                                       outer_nulls.size(), source.size());
    }

    ColumnPtr candidate;
    try {
        if (!source.is_typed()) {
            RETURN_IF_ERROR(variant_element_v2_internal::extract_encoded_variant_element(
                    source, path, outer_nulls, &candidate));
        } else {
            // A typed Variant is one scalar root value per row. String payloads are strings, not
            // JSON documents, so every non-empty object/array path is absent for all typed roots.
            RETURN_IF_ERROR(variant_element_v2_internal::make_all_null_variant_element_result(
                    source.size(), &candidate));
        }
    } catch (const Exception& exception) {
        if (exception.code() == ErrorCode::CORRUPTION) {
            return Status::InvalidArgument("Invalid Variant V2 input: {}", exception.message());
        }
        return exception.to_status();
    }
    if (!candidate || candidate->size() != source.size()) {
        return Status::InternalError("Variant V2 element kernel produced {} rows, expected {}",
                                     candidate ? candidate->size() : 0, source.size());
    }
    output->swap(candidate);
    return Status::OK();
}

namespace variant_element_v2_internal {
namespace {

constexpr uint32_t UNMAPPED_METADATA = std::numeric_limits<uint32_t>::max();

struct ExtractedRows {
    VariantTrackedString metadata_bytes;
    DorisVector<uint32_t> metadata_offsets {0};
    DorisVector<uint32_t> metadata_ids;
    VariantTrackedString value_bytes;
    DorisVector<uint32_t> value_offsets {0};

    ColumnVariantV2::EncodedDataView view() const {
        return {.metadata_bytes = {metadata_bytes.data(), metadata_bytes.size()},
                .metadata_offsets = metadata_offsets,
                .meta_ids = metadata_ids,
                .value_bytes = {value_bytes.data(), value_bytes.size()},
                .value_offsets = value_offsets};
    }
};

uint32_t append_bytes(VariantTrackedString& destination, StringRef source,
                      std::string_view description) {
    if (source.size > std::numeric_limits<uint32_t>::max() - destination.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant element {} exceeds the ColumnString uint32 byte limit",
                        description);
    }
    if (source.size != 0) {
        destination.append(source.data, source.size);
    }
    return static_cast<uint32_t>(destination.size());
}

uint32_t append_metadata(ExtractedRows& rows, VariantMetadataRef metadata) {
    rows.metadata_offsets.push_back(
            append_bytes(rows.metadata_bytes, {metadata.data, metadata.size}, "metadata"));
    return static_cast<uint32_t>(rows.metadata_offsets.size() - 2);
}

void append_value(ExtractedRows& rows, VariantValueRef value, uint32_t metadata_id) {
    rows.value_offsets.push_back(append_bytes(rows.value_bytes, {value.data, value.size}, "value"));
    rows.metadata_ids.push_back(metadata_id);
}

ColumnPtr wrap_result(ExtractedRows rows, MutableColumnPtr nulls) {
    auto values = ColumnVariantV2::create();
    values->insert_encoded_rows(rows.view());
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

} // namespace

Status extract_encoded_variant_element(const ColumnVariantV2& source,
                                       const ResolvedVariantElementV2Path& path,
                                       std::span<const uint8_t> outer_nulls,
                                       ColumnPtr* const output) {
    VariantPathV2BatchReader reader(source, path);
    const size_t metadata_count = reader.metadata_count();
    DorisVector<uint32_t> output_metadata_ids(metadata_count, UNMAPPED_METADATA);

    VariantBlockBuilder null_builder(VariantBlockBuilder::ReserveHint {.rows = 1});
    auto null_row = null_builder.begin_row();
    null_row.add_null();
    null_row.finish();
    VariantEncodedBlock null_block = null_builder.finish_block();
    const VariantValueRef null_value = null_block.value_at(0);

    ExtractedRows rows;
    rows.metadata_ids.reserve(source.size());
    rows.value_offsets.reserve(source.size() + 1);
    auto nulls = ColumnUInt8::create();
    nulls->reserve(source.size());
    uint32_t null_metadata_id = UNMAPPED_METADATA;

    auto ensure_null_metadata = [&]() {
        if (null_metadata_id == UNMAPPED_METADATA) {
            null_metadata_id = append_metadata(rows, null_value.metadata);
        }
        return null_metadata_id;
    };

    for (size_t row = 0; row < source.size(); ++row) {
        if (is_outer_null(outer_nulls, row)) {
            append_value(rows, null_value, ensure_null_metadata());
            nulls->insert_value(1);
            continue;
        }

        const uint32_t source_metadata_id = reader.metadata_id_at(row);
        if (output_metadata_ids[source_metadata_id] == UNMAPPED_METADATA) {
            output_metadata_ids[source_metadata_id] =
                    append_metadata(rows, reader.metadata_at(source_metadata_id));
        }

        VariantValueRef current;
        if (reader.find_at(row, &current)) {
            append_value(rows, current, output_metadata_ids[source_metadata_id]);
            nulls->insert_value(0);
        } else {
            append_value(rows, null_value, output_metadata_ids[source_metadata_id]);
            nulls->insert_value(1);
        }
    }

    ColumnPtr candidate = wrap_result(std::move(rows), std::move(nulls));
    output->swap(candidate);
    return Status::OK();
}

Status make_all_null_variant_element_result(size_t rows, ColumnPtr* const output) {
    VariantBlockBuilder builder(VariantBlockBuilder::ReserveHint {.rows = rows});
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    VariantEncodedBlock block = builder.finish_block();
    auto values = ColumnVariantV2::create();
    values->insert_encoded_block(block.view());
    ColumnPtr candidate = ColumnNullable::create(std::move(values), ColumnUInt8::create(rows, 1));
    output->swap(candidate);
    return Status::OK();
}

} // namespace variant_element_v2_internal

} // namespace doris
