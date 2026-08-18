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
#include "core/pod_array.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "core/value/variant/variant_selected_value_builder.h"

namespace doris {

namespace {

struct OwnedPathSegment {
    VariantElementV2PathSegment::Kind kind;
    PaddedPODArray<char> key;
    int64_t index = 0;
};

} // namespace

struct ResolvedVariantElementV2Path::Impl {
    DorisVector<OwnedPathSegment> segments;
};

namespace {

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

    bool find_at(size_t row, VariantRef* const output) {
        DORIS_CHECK(output != nullptr);
        const uint32_t metadata_id = _source.metadata_id_at(row);
        _resolve_metadata(metadata_id);
        VariantRef current = _source.value_at(row);
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

std::optional<ColumnPtr> extract_shredded_typed_variant_element(
        const ColumnVariantV2& source, const ResolvedVariantElementV2Path& path,
        std::span<const uint8_t> outer_nulls) {
    DorisVector<VariantShreddedPathSegment> shredded_path;
    shredded_path.reserve(path.size());
    for (size_t position = 0; position < path.size(); ++position) {
        VariantShreddedPathSegment segment;
        if (path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
            segment.kind = VariantShreddedPathSegment::Kind::OBJECT_KEY;
            segment.key = path.object_key_at(position);
        } else {
            segment.kind = VariantShreddedPathSegment::Kind::ARRAY_INDEX;
            segment.index = path.array_index_at(position);
        }
        shredded_path.push_back(segment);
    }

    auto match = source.find_shredded_typed_value(shredded_path);
    if (!match.has_value()) {
        return std::nullopt;
    }
    const ColumnPtr& matched_column = match->normalized ? match->normalized : match->column;
    const auto& leaf = assert_cast<const ColumnNullable&>(*matched_column);
    auto nulls = leaf.get_null_map_column().clone_resized(source.size());
    auto& null_data = assert_cast<ColumnUInt8&>(*nulls).get_data();
    for (size_t row = 0; row < source.size(); ++row) {
        null_data[row] =
                static_cast<uint8_t>(null_data[row] != 0 || is_outer_null(outer_nulls, row));
    }

    if (match->normalized) {
        return ColumnNullable::create(leaf.get_nested_column_ptr(), std::move(nulls));
    }

    // The typed ColumnVariantV2 retains the exact decoded Parquet leaf. Only the SQL result null
    // map is produced here, so predicates and casts can consume the leaf without reconstructing
    // canonical Variant rows.
    auto values = ColumnVariantV2::create_typed(match->column, match->type);
    return ColumnNullable::create(std::move(values), std::move(nulls));
}

Status make_all_null_variant_element_result(size_t rows, ColumnPtr* output);

} // namespace

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
                owned.key.assign(segment.key().data, segment.key().data + segment.key().size);
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
        if (source.is_shredded()) {
            if (auto typed = extract_shredded_typed_variant_element(source, path, outer_nulls)) {
                candidate = std::move(*typed);
            } else {
                RETURN_IF_ERROR(
                        extract_encoded_variant_element(source, path, outer_nulls, &candidate));
            }
        } else if (!source.is_typed()) {
            RETURN_IF_ERROR(extract_encoded_variant_element(source, path, outer_nulls, &candidate));
        } else {
            // A typed Variant is one scalar root value per row. String payloads are strings, not
            // JSON documents, so every non-empty object/array path is absent for all typed roots.
            RETURN_IF_ERROR(make_all_null_variant_element_result(source.size(), &candidate));
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

namespace {

Status extract_encoded_variant_element(const ColumnVariantV2& source,
                                       const ResolvedVariantElementV2Path& path,
                                       std::span<const uint8_t> outer_nulls,
                                       ColumnPtr* const output) {
    VariantPathV2BatchReader reader(source, path);
    VariantSelectedValueBuilder builder(source.size());
    for (size_t row = 0; row < source.size(); ++row) {
        VariantRef current;
        if (is_outer_null(outer_nulls, row) || !reader.find_at(row, &current)) {
            builder.append_missing();
            continue;
        }
        builder.append_selected(current);
    }
    ColumnPtr candidate = builder.finish();
    output->swap(candidate);
    return Status::OK();
}

Status make_all_null_variant_element_result(size_t rows, ColumnPtr* const output) {
    VariantSelectedValueBuilder builder(rows);
    for (size_t row_index = 0; row_index < rows; ++row_index) {
        builder.append_missing();
    }
    ColumnPtr candidate = builder.finish();
    output->swap(candidate);
    return Status::OK();
}

} // namespace

} // namespace doris
