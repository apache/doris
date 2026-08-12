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

#include <algorithm>
#include <atomic>
#include <limits>
#include <optional>
#include <utility>

#include "common/check.h"
#include "common/exception.h"
#include "core/column/column_const.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/column/variant_v2/variant_shredded_path.h"
#include "core/custom_allocator.h"
#include "core/data_type/data_type_nullable.h"
#include "core/pod_array.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"
#include "util/json/path_in_data.h"

namespace doris {

namespace {

#ifdef BE_TEST
std::atomic<size_t> shredded_path_inspection_count {0};
#endif

struct OwnedPathSegment {
    VariantElementV2PathSegment::Kind kind;
    PaddedPODArray<char> key;
    int64_t index = 0;
};

} // namespace

struct ResolvedVariantElementV2Path::Impl {
    DorisVector<OwnedPathSegment> segments;
    size_t object_key_count = 0;
    std::optional<PathInData> object_path;
};

class VariantElementV2ResultBuilder final {
public:
    static ColumnPtr build(ColumnString::MutablePtr metadatas,
                           ColumnVariantV2::MetadataIdsColumn::MutablePtr metadata_ids,
                           ColumnString::MutablePtr values, MutableColumnPtr nulls) {
        return ColumnNullable::create(
                ColumnVariantV2::create_encoded_from_valid_parts(
                        std::move(metadatas), std::move(metadata_ids), std::move(values)),
                std::move(nulls));
    }
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
            : VariantPathV2BatchReader(source.read_view(), path, false) {
        DORIS_CHECK(_source.is_encoded());
    }

    static VariantPathV2BatchReader shredded_residual(ColumnVariantV2::ReadView source,
                                                      const ResolvedVariantElementV2Path& path) {
        DORIS_CHECK(source.is_shredded());
        return {source, path, true};
    }

    bool find_at(size_t row, VariantRef* const output) {
        DORIS_CHECK(output != nullptr);
        const uint32_t metadata_id = metadata_id_at(row);
        _resolve_metadata(metadata_id);
        VariantRef current = value_at(row);
        const size_t cache_base = static_cast<size_t>(metadata_id) * _object_key_count;
        size_t object_position = 0;
        for (size_t position = 0; position < _path.size(); ++position) {
            if (_path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
                const int64_t field_id = _object_ids[cache_base + object_position];
                ++object_position;
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

    size_t metadata_count() const noexcept {
        return _read_residual ? _source.residual_metadata_count() : _source.metadata_count();
    }

    uint32_t metadata_id_at(size_t row) const {
        return _read_residual ? _source.residual_metadata_id_at(row) : _source.metadata_id_at(row);
    }

    VariantMetadataRef metadata_at(uint32_t id) const {
        return _read_residual ? _source.residual_metadata_at(id) : _source.metadata_at(id);
    }

private:
    VariantRef value_at(size_t row) const {
        return _read_residual ? _source.residual_value_at(row) : _source.value_at(row);
    }

    VariantPathV2BatchReader(ColumnVariantV2::ReadView source,
                             const ResolvedVariantElementV2Path& path, bool read_residual)
            : _source(source),
              _path(path),
              _read_residual(read_residual),
              _object_key_count(path.object_key_count()) {
        DORIS_CHECK_GT(_path.size(), 0);
        if (metadata_count() != 0 &&
            _object_key_count > std::numeric_limits<size_t>::max() / metadata_count()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant path cache size overflows size_t");
        }
    }

    void _resolve_metadata(uint32_t metadata_id) {
        if (_object_key_count == 0) {
            return;
        }
        if (_resolved_metadata.empty()) {
            _resolved_metadata.assign(metadata_count(), 0);
            _object_ids.assign(metadata_count() * _object_key_count, -1);
        }
        DORIS_CHECK_LT(metadata_id, _resolved_metadata.size());
        if (_resolved_metadata[metadata_id] != 0) {
            return;
        }
        const VariantMetadataRef metadata = metadata_at(metadata_id);
        const size_t base = static_cast<size_t>(metadata_id) * _object_key_count;
        size_t object_position = 0;
        for (size_t position = 0; position < _path.size(); ++position) {
            if (_path.kind_at(position) == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
                _object_ids[base + object_position] =
                        metadata.find_key(_path.object_key_at(position));
                ++object_position;
            }
        }
        _resolved_metadata[metadata_id] = 1;
    }

    ColumnVariantV2::ReadView _source;
    const ResolvedVariantElementV2Path& _path;
    bool _read_residual = false;
    size_t _object_key_count = 0;
    DorisVector<int64_t> _object_ids;
    DorisVector<uint8_t> _resolved_metadata;
};

Status extract_encoded_variant_element(const ColumnVariantV2& source,
                                       const ResolvedVariantElementV2Path& path,
                                       std::span<const uint8_t> outer_nulls, ColumnPtr* output);

Status extract_variant_element_with_reader(size_t rows_count, VariantPathV2BatchReader& reader,
                                           std::span<const uint8_t> outer_nulls, ColumnPtr* output);

Status extract_shredded_residual_variant_element(const ColumnVariantV2::ReadView& source,
                                                 const ResolvedVariantElementV2Path& path,
                                                 std::span<const uint8_t> outer_nulls,
                                                 ColumnPtr* output);

Status extract_shredded_variant_element(const ColumnVariantV2& source,
                                        const ResolvedVariantElementV2Path& path,
                                        std::span<const uint8_t> outer_nulls, ColumnPtr* output);

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

size_t ResolvedVariantElementV2Path::object_key_count() const noexcept {
    return _impl->object_key_count;
}

const PathInData* ResolvedVariantElementV2Path::object_path() const noexcept {
    return _impl->object_path.has_value() ? &*_impl->object_path : nullptr;
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
    PathInData::Parts object_path_parts;
    object_path_parts.reserve(segments.size());
    bool is_object_path = true;
    for (const VariantElementV2PathSegment& segment : segments) {
        OwnedPathSegment owned {.kind = segment.kind(), .key = {}, .index = segment.index()};
        if (segment.kind() == VariantElementV2PathSegment::Kind::OBJECT_KEY) {
            if (segment.key().size != 0 && segment.key().data == nullptr) {
                return Status::InvalidArgument("Variant V2 object path key has a null pointer");
            }
            if (segment.key().size != 0) {
                owned.key.assign(segment.key().data, segment.key().data + segment.key().size);
            }
            ++impl->object_key_count;
            if (is_object_path) {
                const std::string_view key =
                        segment.key().size == 0
                                ? std::string_view {}
                                : std::string_view(segment.key().data, segment.key().size);
                object_path_parts.emplace_back(key, false, 0);
            }
        } else {
            is_object_path = false;
        }
        impl->segments.push_back(std::move(owned));
    }
    if (is_object_path) {
        impl->object_path.emplace(object_path_parts);
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
        if (source.is_encoded()) {
            RETURN_IF_ERROR(extract_encoded_variant_element(source, path, outer_nulls, &candidate));
        } else if (source.is_typed()) {
            // A typed Variant is one scalar root value per row. String payloads are strings, not
            // JSON documents, so every non-empty object/array path is absent for all typed roots.
            RETURN_IF_ERROR(make_all_null_variant_element_result(source.size(), &candidate));
        } else {
            RETURN_IF_ERROR(
                    extract_shredded_variant_element(source, path, outer_nulls, &candidate));
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

Status build_variant_element_v2_path_plan(
        std::span<const ColumnWithTypeAndName> selectors,
        // The immutable plan is published only after every selector is validated and owned.
        // NOLINTNEXTLINE(readability-non-const-parameter)
        std::shared_ptr<const VariantElementV2PathPlan>* output) {
    if (output == nullptr) {
        return Status::InvalidArgument("Variant V2 path plan output is null");
    }
    if (selectors.empty()) {
        return Status::InvalidArgument("Variant V2 path plan requires selectors");
    }

    DorisVector<VariantElementV2PathSegment> segments;
    segments.reserve(selectors.size());
    for (const ColumnWithTypeAndName& selector : selectors) {
        if (!selector.column || !selector.type) {
            return Status::InvalidArgument("Variant V2 path selector column or type is null");
        }
        const auto* constant = check_and_get_column<ColumnConst>(selector.column.get());
        if (constant == nullptr) {
            return Status::InvalidArgument("Variant V2 path selector must be constant");
        }
        // Read the single owned Const payload directly. Expanding every selector to block row
        // count would turn path planning into O(rows * path_depth) copying.
        const IColumn* selector_column = &constant->get_data_column();
        if (selector_column->is_null_at(0)) {
            output->reset(new VariantElementV2PathPlan(nullptr, true));
            return Status::OK();
        }
        if (const auto* nullable = check_and_get_column<ColumnNullable>(*selector_column)) {
            selector_column = &nullable->get_nested_column();
        }

        const PrimitiveType selector_type = remove_nullable(selector.type)->get_primitive_type();
        if (is_string_type(selector_type)) {
            segments.push_back(
                    VariantElementV2PathSegment::object_key(selector_column->get_data_at(0)));
        } else if (is_int_or_bool(selector_type)) {
            const int64_t sql_index = selector_column->get_int(0);
            if (sql_index == 0) {
                output->reset(new VariantElementV2PathPlan(nullptr, true));
                return Status::OK();
            }
            segments.push_back(VariantElementV2PathSegment::array_index(
                    sql_index > 0 ? sql_index - 1 : sql_index));
        } else {
            return Status::InvalidArgument("unsupported Variant V2 path selector type {}",
                                           selector.type->get_name());
        }
    }

    std::unique_ptr<ResolvedVariantElementV2Path> path;
    RETURN_IF_ERROR(resolve_variant_element_v2_path(segments, &path));
    std::shared_ptr<const ResolvedVariantElementV2Path> shared_path(std::move(path));
    output->reset(new VariantElementV2PathPlan(std::move(shared_path), false));
    return Status::OK();
}

Status try_extract_variant_element_v2_path(
        const ColumnPtr& source, const VariantElementV2PathPlan& plan,
        // Output and applicability are published only after validating their pointers.
        // NOLINTNEXTLINE(readability-non-const-parameter)
        ColumnPtr* output, bool* applied) {
    if (output == nullptr || applied == nullptr) {
        return Status::InvalidArgument("Variant V2 path adapter output is null");
    }
    *applied = false;
    if (!source) {
        return Status::InvalidArgument("Variant V2 path adapter source is null");
    }

    const auto* constant_source = check_and_get_column<ColumnConst>(source.get());
    const ColumnPtr physical_source =
            constant_source == nullptr ? source : constant_source->get_data_column().get_ptr();
    const IColumn* physical = physical_source.get();
    std::span<const uint8_t> outer_nulls;
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        outer_nulls = nullable->get_null_map_data();
        physical = &nullable->get_nested_column();
    }
    const auto* variant = check_and_get_column<ColumnVariantV2>(physical);
    if (variant == nullptr) {
        return Status::OK();
    }
    *applied = true;
    if (constant_source != nullptr) {
        DORIS_CHECK_EQ(variant->size(), 1)
                << "constant Variant V2 source payload must contain one row";
    }
    auto publish = [&](ColumnPtr candidate) {
        if (constant_source != nullptr) {
            candidate = ColumnConst::create(std::move(candidate), source->size());
        }
        output->swap(candidate);
        return Status::OK();
    };
    if (variant->empty()) {
        ColumnPtr candidate =
                ColumnNullable::create(ColumnVariantV2::create(), ColumnUInt8::create());
        return publish(std::move(candidate));
    }
    if (plan._always_null) {
        ColumnPtr candidate;
        RETURN_IF_ERROR(make_all_null_variant_element_result(variant->size(), &candidate));
        return publish(std::move(candidate));
    }
    DORIS_CHECK(static_cast<bool>(plan._resolved_path));
    ColumnPtr candidate;
    RETURN_IF_ERROR(
            extract_variant_element_v2(*variant, *plan._resolved_path, outer_nulls, &candidate));
    return publish(std::move(candidate));
}

Status try_extract_variant_element_v2_path(const ColumnPtr& source,
                                           std::span<const ColumnWithTypeAndName> selectors,
                                           ColumnPtr* output, bool* applied) {
    if (output == nullptr || applied == nullptr) {
        return Status::InvalidArgument("Variant V2 path adapter output is null");
    }
    *applied = false;
    if (!source) {
        return Status::InvalidArgument("Variant V2 path adapter source is null");
    }

    const IColumn* physical = source.get();
    if (const auto* constant = check_and_get_column<ColumnConst>(physical)) {
        physical = &constant->get_data_column();
    }
    if (const auto* nullable = check_and_get_column<ColumnNullable>(physical)) {
        physical = &nullable->get_nested_column();
    }
    if (check_and_get_column<ColumnVariantV2>(physical) == nullptr) {
        return Status::OK();
    }

    std::shared_ptr<const VariantElementV2PathPlan> plan;
    RETURN_IF_ERROR(build_variant_element_v2_path_plan(selectors, &plan));
    return try_extract_variant_element_v2_path(source, *plan, output, applied);
}

namespace {

constexpr uint32_t UNMAPPED_METADATA = std::numeric_limits<uint32_t>::max();

struct ExtractedRows {
    ColumnString::Chars metadata_bytes;
    PaddedPODArray<uint32_t> metadata_offsets;
    ColumnVariantV2::MetadataIdsColumn::Container metadata_ids;
    ColumnString::Chars value_bytes;
    PaddedPODArray<uint32_t> value_offsets;
};

uint32_t append_bytes(ColumnString::Chars& destination, StringRef source,
                      std::string_view description) {
    if (source.size > std::numeric_limits<uint32_t>::max() - destination.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant element {} exceeds the ColumnString uint32 byte limit",
                        description);
    }
    if (source.size != 0) {
        const auto* begin = reinterpret_cast<const ColumnString::Char*>(source.data);
        destination.insert(begin, begin + source.size);
    }
    return static_cast<uint32_t>(destination.size());
}

uint32_t append_metadata(ExtractedRows& rows, VariantMetadataRef metadata) {
    rows.metadata_offsets.push_back(
            append_bytes(rows.metadata_bytes, {metadata.data, metadata.size}, "metadata"));
    return static_cast<uint32_t>(rows.metadata_offsets.size() - 1);
}

uint32_t remap_metadata(ExtractedRows& rows, DorisVector<uint32_t>& output_metadata_ids,
                        uint32_t source_metadata_id, VariantMetadataRef metadata) {
    uint32_t& output_metadata_id = output_metadata_ids[source_metadata_id];
    if (output_metadata_id == UNMAPPED_METADATA) {
        output_metadata_id = append_metadata(rows, metadata);
    }
    return output_metadata_id;
}

void append_value(ExtractedRows& rows, VariantRef value, uint32_t metadata_id) {
    rows.value_offsets.push_back(append_bytes(rows.value_bytes, value.value, "value"));
    rows.metadata_ids.push_back(metadata_id);
}

ColumnPtr wrap_result(ExtractedRows rows, MutableColumnPtr nulls) {
    auto metadatas = ColumnString::create();
    metadatas->get_chars() = std::move(rows.metadata_bytes);
    metadatas->get_offsets() = std::move(rows.metadata_offsets);
    auto metadata_ids = ColumnVariantV2::MetadataIdsColumn::create();
    metadata_ids->get_data() = std::move(rows.metadata_ids);
    auto encoded_values = ColumnString::create();
    encoded_values->get_chars() = std::move(rows.value_bytes);
    encoded_values->get_offsets() = std::move(rows.value_offsets);
    return VariantElementV2ResultBuilder::build(std::move(metadatas), std::move(metadata_ids),
                                                std::move(encoded_values), std::move(nulls));
}

const PathInData& inspected_shredded_field_path(const ColumnVariantV2::ReadView& source,
                                                size_t index) {
#ifdef BE_TEST
    shredded_path_inspection_count.fetch_add(1, std::memory_order_relaxed);
#endif
    return source.shredded_field_path(index);
}

struct ShreddedFieldMatch {
    std::optional<size_t> exact;
    size_t descendant_begin = 0;
    size_t descendant_end = 0;
};

ShreddedFieldMatch find_shredded_fields(const ColumnVariantV2::ReadView& source,
                                        const PathInData& requested_path) {
    size_t first = 0;
    size_t last = source.shredded_field_count();
    while (first < last) {
        const size_t middle = first + (last - first) / 2;
        if (variant_shredded_path_less(inspected_shredded_field_path(source, middle),
                                       requested_path)) {
            first = middle + 1;
        } else {
            last = middle;
        }
    }

    ShreddedFieldMatch match {
            .exact = std::nullopt, .descendant_begin = first, .descendant_end = first};
    if (first == source.shredded_field_count()) {
        return match;
    }
    const PathInData& first_path = inspected_shredded_field_path(source, first);
    if (first_path.get_parts() == requested_path.get_parts()) {
        match.exact = first;
        return match;
    }
    if (requested_path.get_parts().size() >= first_path.get_parts().size() ||
        !variant_shredded_path_is_prefix(requested_path, first_path)) {
        return match;
    }

    match.descendant_end = first + 1;
    while (match.descendant_end < source.shredded_field_count()) {
        const PathInData& field_path = inspected_shredded_field_path(source, match.descendant_end);
        if (!variant_shredded_path_is_prefix(requested_path, field_path)) {
            break;
        }
        ++match.descendant_end;
    }
    return match;
}

Status extract_encoded_variant_element(const ColumnVariantV2& source,
                                       const ResolvedVariantElementV2Path& path,
                                       std::span<const uint8_t> outer_nulls,
                                       ColumnPtr* const output) {
    VariantPathV2BatchReader reader(source, path);
    return extract_variant_element_with_reader(source.size(), reader, outer_nulls, output);
}

Status extract_variant_element_with_reader(size_t rows_count, VariantPathV2BatchReader& reader,
                                           std::span<const uint8_t> outer_nulls,
                                           ColumnPtr* const output) {
    const size_t metadata_count = reader.metadata_count();
    DorisVector<uint32_t> output_metadata_ids(metadata_count, UNMAPPED_METADATA);

    VariantBatchBuilder null_builder(VariantBatchBuilder::ReserveHint {.rows = 1});
    auto null_row = null_builder.begin_row();
    null_row.add_null();
    null_row.finish();
    VariantBatchBuilder null_block = null_builder.finish_batch();
    const VariantRef null_value = null_block.value_at(0);

    ExtractedRows rows;
    rows.metadata_ids.reserve(rows_count);
    rows.value_offsets.reserve(rows_count);
    auto nulls = ColumnUInt8::create();
    nulls->reserve(rows_count);
    uint32_t null_metadata_id = UNMAPPED_METADATA;

    auto ensure_null_metadata = [&]() {
        if (null_metadata_id == UNMAPPED_METADATA) {
            null_metadata_id = append_metadata(rows, null_value.metadata);
        }
        return null_metadata_id;
    };

    for (size_t row = 0; row < rows_count; ++row) {
        if (is_outer_null(outer_nulls, row)) {
            append_value(rows, null_value, ensure_null_metadata());
            nulls->insert_value(1);
            continue;
        }

        const uint32_t source_metadata_id = reader.metadata_id_at(row);
        const uint32_t output_metadata_id =
                remap_metadata(rows, output_metadata_ids, source_metadata_id,
                               reader.metadata_at(source_metadata_id));
        VariantRef current;
        if (reader.find_at(row, &current)) {
            append_value(rows, current, output_metadata_id);
            nulls->insert_value(0);
        } else {
            append_value(rows, null_value, output_metadata_id);
            nulls->insert_value(1);
        }
    }

    ColumnPtr candidate = wrap_result(std::move(rows), std::move(nulls));
    output->swap(candidate);
    return Status::OK();
}

Status extract_shredded_residual_variant_element(const ColumnVariantV2::ReadView& source,
                                                 const ResolvedVariantElementV2Path& path,
                                                 std::span<const uint8_t> outer_nulls,
                                                 ColumnPtr* const output) {
    VariantPathV2BatchReader reader = VariantPathV2BatchReader::shredded_residual(source, path);
    return extract_variant_element_with_reader(source.size(), reader, outer_nulls, output);
}

Status extract_exact_shredded_variant_element(const ColumnVariantV2& source_column,
                                              const ColumnVariantV2::ReadView& source,
                                              size_t field_index,
                                              const ResolvedVariantElementV2Path& path,
                                              std::span<const uint8_t> outer_nulls,
                                              ColumnPtr* const output) {
    DORIS_CHECK(source.is_shredded());
    VariantPathV2BatchReader residual = VariantPathV2BatchReader::shredded_residual(source, path);
    const ColumnVariantV2& field_values = source.shredded_field_values(field_index);
    DORIS_CHECK(!field_values.is_shredded());
    const ColumnUInt8& presence = source.shredded_field_presence(field_index);

    if (field_values.is_typed()) {
        auto result_nulls = ColumnUInt8::create();
        result_nulls->reserve(source.size());
        bool has_residual_conflict = false;
        for (size_t row = 0; row < source.size(); ++row) {
            if (is_outer_null(outer_nulls, row)) {
                result_nulls->insert_value(1);
            } else if (presence.get_data()[row] != 0) {
                // A present typed null is a Variant null value, not SQL NULL.
                result_nulls->insert_value(0);
            } else {
                VariantRef residual_value;
                const bool conflict = residual.find_at(row, &residual_value);
                if (conflict) {
                    has_residual_conflict = true;
                    break;
                }
                result_nulls->insert_value(1);
            }
        }
        if (!has_residual_conflict) {
            ColumnPtr candidate = ColumnNullable::create(
                    source_column.shredded_field_values(field_index).get_ptr(),
                    std::move(result_nulls));
            output->swap(candidate);
            return Status::OK();
        }
    }

    ColumnVariantV2::MutablePtr materialized_field;
    const ColumnVariantV2* encoded_field = &field_values;
    if (field_values.is_typed()) {
        materialized_field = field_values.materialize_encoded_range(0, field_values.size());
        encoded_field = materialized_field.get();
    }
    DORIS_CHECK(encoded_field->is_encoded());
    const ColumnVariantV2::ReadView field = encoded_field->read_view();

    VariantBatchBuilder null_builder(VariantBatchBuilder::ReserveHint {.rows = 1});
    auto null_row = null_builder.begin_row();
    null_row.add_null();
    null_row.finish();
    VariantBatchBuilder null_block = null_builder.finish_batch();
    const VariantRef null_value = null_block.value_at(0);

    ExtractedRows rows;
    rows.metadata_ids.reserve(source.size());
    rows.value_offsets.reserve(source.size() + 1);
    auto nulls = ColumnUInt8::create();
    nulls->reserve(source.size());
    DorisVector<uint32_t> field_output_metadata_ids(field.metadata_count(), UNMAPPED_METADATA);
    DorisVector<uint32_t> residual_output_metadata_ids(residual.metadata_count(),
                                                       UNMAPPED_METADATA);
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

        if (presence.get_data()[row] != 0) {
            const uint32_t source_metadata_id = field.metadata_id_at(row);
            const uint32_t output_metadata_id =
                    remap_metadata(rows, field_output_metadata_ids, source_metadata_id,
                                   field.metadata_at(source_metadata_id));
            append_value(rows, field.value_at(row), output_metadata_id);
            // A present Variant null is a value. Only the outer nullable marks SQL NULL/missing.
            nulls->insert_value(0);
            continue;
        }

        // A zero presence bit is not necessarily missing: rows excluded by a scalar/object
        // conflict still own the complete value in the residual encoded row.
        const uint32_t source_metadata_id = residual.metadata_id_at(row);
        const uint32_t output_metadata_id =
                remap_metadata(rows, residual_output_metadata_ids, source_metadata_id,
                               residual.metadata_at(source_metadata_id));
        VariantRef current;
        if (residual.find_at(row, &current)) {
            append_value(rows, current, output_metadata_id);
            nulls->insert_value(0);
        } else {
            append_value(rows, null_value, output_metadata_id);
            nulls->insert_value(1);
        }
    }

    ColumnPtr candidate = wrap_result(std::move(rows), std::move(nulls));
    output->swap(candidate);
    return Status::OK();
}

Status extract_ancestor_shredded_variant_element(const ColumnVariantV2& source_column,
                                                 const ColumnVariantV2::ReadView& source,
                                                 const ResolvedVariantElementV2Path& path,
                                                 const PathInData& requested_path,
                                                 size_t descendant_begin, size_t descendant_end,
                                                 std::span<const uint8_t> outer_nulls,
                                                 ColumnPtr* const output) {
    DORIS_CHECK(source.is_shredded());
    DORIS_CHECK_LT(descendant_begin, descendant_end);
    DORIS_CHECK_LE(descendant_end, source.shredded_field_count());
    // Project the requested residual subtree and shift only its descendant shredded fields. An active
    // shredded field supplies the otherwise-missing object anchor, so the next element_at call can
    // hit
    // the relative exact path without materializing the complete source S column.
    VariantPathV2BatchReader residual = VariantPathV2BatchReader::shredded_residual(source, path);
    VariantBatchBuilder residual_builder(VariantBatchBuilder::ReserveHint {.rows = source.size()});
    auto result_nulls = ColumnUInt8::create();
    result_nulls->reserve(source.size());

    const uint8_t* active_descendants = nullptr;
    ColumnUInt8::MutablePtr active_descendant_union;
    if (descendant_end == descendant_begin + 1) {
        active_descendants = source.shredded_field_presence(descendant_begin).get_data().data();
    } else {
        active_descendant_union = ColumnUInt8::create(source.size(), 0);
        auto& union_data = active_descendant_union->get_data();
        for (size_t field_index = descendant_begin; field_index < descendant_end; ++field_index) {
            const auto& presence = source.shredded_field_presence(field_index).get_data();
            for (size_t row_index = 0; row_index < source.size(); ++row_index) {
                union_data[row_index] |= presence[row_index];
            }
        }
        active_descendants = union_data.data();
    }

    for (size_t row_index = 0; row_index < source.size(); ++row_index) {
        const bool has_active_descendant = active_descendants[row_index] != 0;
        auto row = residual_builder.begin_row();
        if (is_outer_null(outer_nulls, row_index)) {
            // Nullable nested data is hidden, but the shared field presence remains active. Keep
            // an empty object anchor so the physical S child remains internally valid without
            // copying and masking every descendant presence column.
            if (has_active_descendant) {
                auto object = row.start_object();
                object.finish();
            } else {
                row.add_null();
            }
            result_nulls->insert_value(1);
            row.finish();
            continue;
        }

        VariantRef residual_value;
        if (residual.find_at(row_index, &residual_value)) {
            row.add_value(residual_value);
            result_nulls->insert_value(0);
            row.finish();
            continue;
        }

        if (has_active_descendant) {
            auto object = row.start_object();
            object.finish();
            result_nulls->insert_value(0);
        } else {
            row.add_null();
            result_nulls->insert_value(1);
        }
        row.finish();
    }

    auto projected_residual = ColumnVariantV2::create();
    projected_residual->insert_encoded_batch(residual_builder.finish_batch());
    const size_t requested_depth = requested_path.get_parts().size();
    ColumnPtr candidate =
            ColumnNullable::create(source_column.project_shredded_fields(
                                           std::move(projected_residual), descendant_begin,
                                           descendant_end - descendant_begin, requested_depth),
                                   std::move(result_nulls));
    output->swap(candidate);
    return Status::OK();
}

Status extract_shredded_variant_element(const ColumnVariantV2& source,
                                        const ResolvedVariantElementV2Path& path,
                                        std::span<const uint8_t> outer_nulls,
                                        ColumnPtr* const output) {
    const ColumnVariantV2::ReadView view = source.read_view();
    DORIS_CHECK(view.is_shredded());
    const PathInData* requested_path = path.object_path();
    if (requested_path != nullptr) {
        const ShreddedFieldMatch match = find_shredded_fields(view, *requested_path);
        if (match.exact.has_value()) {
            return extract_exact_shredded_variant_element(source, view, *match.exact, path,
                                                          outer_nulls, output);
        }
        if (match.descendant_begin != match.descendant_end) {
            return extract_ancestor_shredded_variant_element(
                    source, view, path, *requested_path, match.descendant_begin,
                    match.descendant_end, outer_nulls, output);
        }
    }

    // Shredded fields are scalar leaves at prefix-free object paths. Descendant, array, and
    // unrelated lookups therefore cannot read an active child; all possible values live in the
    // encoded residual. Avoid materializing and merging every shredded field.
    return extract_shredded_residual_variant_element(view, path, outer_nulls, output);
}

Status make_all_null_variant_element_result(size_t rows, ColumnPtr* const output) {
    const size_t physical_rows = std::min<size_t>(rows, 1);
    VariantBatchBuilder builder(VariantBatchBuilder::ReserveHint {.rows = physical_rows});
    for (size_t row_index = 0; row_index < physical_rows; ++row_index) {
        auto row = builder.begin_row();
        row.add_null();
        row.finish();
    }
    VariantBatchBuilder block = builder.finish_batch();
    auto values = ColumnVariantV2::create();
    values->insert_encoded_batch(block);
    ColumnPtr candidate =
            ColumnNullable::create(std::move(values), ColumnUInt8::create(physical_rows, 1));
    if (rows > 1) {
        candidate = ColumnConst::create(std::move(candidate), rows);
    }
    output->swap(candidate);
    return Status::OK();
}

} // namespace

#ifdef BE_TEST
void VariantElementV2TestAccess::reset_shredded_path_inspections() {
    shredded_path_inspection_count.store(0, std::memory_order_relaxed);
}

size_t VariantElementV2TestAccess::shredded_path_inspections() {
    return shredded_path_inspection_count.load(std::memory_order_relaxed);
}

bool VariantElementV2TestAccess::has_exact_shredded_path(const ColumnVariantV2& source,
                                                         const PathInData& requested_path) {
    return find_shredded_fields(source.read_view(), requested_path).exact.has_value();
}
#endif

} // namespace doris
