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

#include "core/column/variant_v2/variant_shredded_column_builder.h"

#include <algorithm>
#include <limits>
#include <optional>
#include <string_view>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/variant_shredded_path.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/variant/variant_batch_builder.h"
#include "core/value/variant/variant_parquet_encoding.h"

namespace doris {
namespace {

using Layout = DorisVector<VariantShreddedLayoutEntry>;

bool is_plain_object_part(const PathInData::Part& part) {
    return !part.is_nested && part.anonymous_array_level == 0;
}

bool is_integer(VariantPrimitiveId id) {
    return id == VariantPrimitiveId::INT8 || id == VariantPrimitiveId::INT16 ||
           id == VariantPrimitiveId::INT32 || id == VariantPrimitiveId::INT64;
}

template <typename Column>
bool append_checked_integer(IColumn& column, int64_t value) {
    using Native = typename Column::value_type;
    if (value < static_cast<int64_t>(std::numeric_limits<Native>::min()) ||
        value > static_cast<int64_t>(std::numeric_limits<Native>::max())) {
        return false;
    }
    assert_cast<Column&>(column).insert_value(static_cast<Native>(value));
    return true;
}

bool append_typed_value(IColumn& column, PrimitiveType type, VariantRef value) {
    // Keep only direct, lossless identities here. Scalars that need civil-time, decimal, IP, or
    // LARGEINT reconstruction return false so the caller keeps them losslessly in residual instead
    // of duplicating CAST conversion policy in the core builder.
    if (value.basic_type() == VariantBasicType::SHORT_STRING) {
        if (type != TYPE_CHAR && type != TYPE_VARCHAR && type != TYPE_STRING) {
            return false;
        }
        const StringRef text = value.get_string();
        assert_cast<ColumnString&>(column).insert_data(text.data, text.size);
        return true;
    }
    if (value.basic_type() != VariantBasicType::PRIMITIVE || value.is_null()) {
        return false;
    }

    const VariantPrimitiveId id = value.primitive_id();
    switch (type) {
    case TYPE_BOOLEAN:
        if (id != VariantPrimitiveId::TRUE_VALUE && id != VariantPrimitiveId::FALSE_VALUE) {
            return false;
        }
        assert_cast<ColumnUInt8&>(column).insert_value(value.get_bool());
        return true;
    case TYPE_TINYINT:
        return is_integer(id) && append_checked_integer<ColumnInt8>(column, value.get_int());
    case TYPE_SMALLINT:
        return is_integer(id) && append_checked_integer<ColumnInt16>(column, value.get_int());
    case TYPE_INT:
        return is_integer(id) && append_checked_integer<ColumnInt32>(column, value.get_int());
    case TYPE_BIGINT:
        if (!is_integer(id)) {
            return false;
        }
        assert_cast<ColumnInt64&>(column).insert_value(value.get_int());
        return true;
    case TYPE_FLOAT:
        if (id != VariantPrimitiveId::FLOAT) {
            return false;
        }
        assert_cast<ColumnFloat32&>(column).insert_value(value.get_float());
        return true;
    case TYPE_DOUBLE:
        if (id != VariantPrimitiveId::DOUBLE) {
            return false;
        }
        assert_cast<ColumnFloat64&>(column).insert_value(value.get_double());
        return true;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        if (id != VariantPrimitiveId::STRING) {
            return false;
        }
        const StringRef text = value.get_string();
        assert_cast<ColumnString&>(column).insert_data(text.data, text.size);
        return true;
    }
    default:
        return false;
    }
}

bool find_scalar_leaf(VariantRef root, const PathInData& path, VariantRef* leaf) {
    VariantRef current = root;
    for (const auto& part : path.get_parts()) {
        if (!is_plain_object_part(part) || current.basic_type() != VariantBasicType::OBJECT ||
            !current.object_find({part.key.data(), part.key.size()}, &current)) {
            return false;
        }
    }
    if (current.basic_type() == VariantBasicType::OBJECT ||
        current.basic_type() == VariantBasicType::ARRAY) {
        return false;
    }
    *leaf = current;
    return true;
}

void append_residual_value(VariantBatchBuilder::Row& output, VariantRef value,
                           std::span<const size_t> active_paths, size_t depth,
                           const Layout& layout) {
    if (active_paths.empty() || value.basic_type() != VariantBasicType::OBJECT) {
        output.add_value(value);
        return;
    }

    auto object = output.start_object();
    DorisVector<size_t> child_paths;
    for (uint32_t child_index = 0; child_index < value.num_elements(); ++child_index) {
        uint32_t field_id = 0;
        const VariantRef child = value.object_value_at(child_index, &field_id);
        const StringRef key = value.metadata.key_at(field_id);
        child_paths.clear();
        for (size_t path_index : active_paths) {
            const auto& parts = layout[path_index].path.get_parts();
            DCHECK_LT(depth, parts.size());
            const auto& part = parts[depth];
            if (is_plain_object_part(part) && part.key == std::string_view(key.data, key.size)) {
                child_paths.push_back(path_index);
            }
        }

        bool erase_child = false;
        for (size_t path_index : child_paths) {
            if (layout[path_index].path.get_parts().size() == depth + 1) {
                erase_child = true;
                break;
            }
        }
        if (erase_child) {
            continue;
        }

        object.add_key(key);
        if (child_paths.empty()) {
            output.add_value(child);
        } else {
            append_residual_value(output, child, child_paths, depth + 1, layout);
        }
    }
    object.finish();
}

struct ShreddedFieldBuilder {
    ShreddedFieldBuilder(const VariantShreddedLayoutEntry& entry, size_t rows)
            : path(entry.path),
              scalar_type(remove_nullable(entry.scalar_type)),
              presence(ColumnUInt8::create()),
              expected_rows(rows),
              typed_possible(
                      is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {
        presence->reserve(rows);
    }

    // NOLINTNEXTLINE(readability-make-member-function-const) -- initializes the value builders.
    void ensure_typed_builders() {
        DORIS_CHECK(typed_possible);
        if (!typed_values) {
            typed_values = scalar_type->create_column();
            typed_nulls = ColumnUInt8::create();
            typed_values->reserve(expected_rows);
            typed_nulls->reserve(expected_rows);
        }
    }

    bool has_materialized_source() const noexcept { return materialized_values != nullptr; }

    void bind_materialized_source(const IColumn& values, const uint8_t* nulls,
                                  MutableColumnPtr owner) {
        DORIS_CHECK(!has_materialized_source());
        DORIS_CHECK(typed_possible);
        DORIS_CHECK(!typed_values && !typed_nulls)
                << "materialized source must be bound before row-local typed allocation";
        DORIS_CHECK_EQ(values.size(), expected_rows);
        DORIS_CHECK(scalar_type->check_column(values).ok());
        if (owner) {
            const IColumn* owned_values = owner.get();
            if (const auto* nullable = check_and_get_column<ColumnNullable>(owned_values)) {
                owned_values = &nullable->get_nested_column();
            }
            DORIS_CHECK_EQ(owned_values, &values)
                    << "materialized owner does not contain the bound native column";
        }
        DORIS_CHECK_LE(expected_rows, std::numeric_limits<uint32_t>::max());
        materialized_values = &values;
        materialized_nulls = nulls;
        materialized_owner = std::move(owner);
        if (!materialized_owner) {
            materialized_rows.reserve(expected_rows);
        }
    }

    void append_materialized(size_t row) {
        DORIS_CHECK(has_materialized_source());
        DORIS_CHECK_LT(row, expected_rows);
        DORIS_CHECK(materialized_nulls == nullptr || materialized_nulls[row] == 0)
                << "null materialized values must be represented as missing";
        if (materialized_owner) {
            DORIS_CHECK_EQ(row, presence->size())
                    << "owned materialized rows must retain their physical row alignment";
        }
        presence->insert_value(1);
        if (!materialized_owner) {
            materialized_rows.push_back(static_cast<uint32_t>(row));
        }
    }

    void append_encoded(std::optional<VariantRef> value) {
        auto encoded_row = encoded_values.begin_row();
        if (value.has_value()) {
            encoded_row.add_value(*value);
        } else {
            encoded_row.add_null();
        }
        encoded_row.finish();
    }

    void append_missing() {
        presence->insert_value(0);
        if (has_materialized_source()) {
            DORIS_CHECK_NE(expected_rows, 0);
            if (!materialized_owner) {
                materialized_rows.push_back(0);
            }
            return;
        }
        if (typed_possible) {
            ensure_typed_builders();
            typed_values->insert_default();
            typed_nulls->insert_value(1);
        } else {
            append_encoded(std::nullopt);
        }
    }

    bool append_selected(VariantRef value) {
        DORIS_CHECK(!has_materialized_source());
        DORIS_CHECK(value.basic_type() != VariantBasicType::OBJECT);
        DORIS_CHECK(value.basic_type() != VariantBasicType::ARRAY);
        if (!typed_possible) {
            return false;
        }
        ensure_typed_builders();
        if (value.is_null()) {
            presence->insert_value(1);
            typed_values->insert_default();
            typed_nulls->insert_value(1);
            return true;
        }
        if (!append_typed_value(*typed_values, scalar_type->get_primitive_type(), value)) {
            return false;
        }
        presence->insert_value(1);
        typed_nulls->insert_value(0);
        return true;
    }

    // NOLINTNEXTLINE(readability-make-member-function-const) -- consumes the value builders.
    MutableColumnPtr finish_values() {
        if (has_materialized_source()) {
            DORIS_CHECK_EQ(presence->size(), expected_rows);
            if (materialized_owner) {
                if (check_and_get_column<ColumnNullable>(materialized_owner.get()) != nullptr) {
                    return ColumnVariantV2::create_typed(std::move(materialized_owner),
                                                         scalar_type);
                }
            }
            MutableColumnPtr values;
            if (materialized_owner) {
                values = std::move(materialized_owner);
                DORIS_CHECK_EQ(values.get(), materialized_values);
            } else {
                DORIS_CHECK_EQ(materialized_rows.size(), expected_rows);
                values = scalar_type->create_column();
                if (!materialized_rows.empty()) {
                    values->insert_indices_from(
                            *materialized_values, materialized_rows.data(),
                            materialized_rows.data() + materialized_rows.size());
                }
            }
            auto nulls = ColumnUInt8::create();
            nulls->get_data().resize(expected_rows);
            for (size_t row = 0; row < expected_rows; ++row) {
                nulls->get_data()[row] = presence->get_data()[row] == 0 ? 1 : 0;
            }
            MutableColumnPtr nullable = ColumnNullable::create(std::move(values), std::move(nulls));
            return ColumnVariantV2::create_typed(std::move(nullable), scalar_type);
        }
        if (typed_possible) {
            ensure_typed_builders();
            MutableColumnPtr nullable =
                    ColumnNullable::create(std::move(typed_values), std::move(typed_nulls));
            return ColumnVariantV2::create_typed(std::move(nullable), scalar_type);
        }
        VariantBatchBuilder encoded = encoded_values.finish_batch();
        auto column = ColumnVariantV2::create();
        column->insert_encoded_batch(encoded);
        return column;
    }

    PathInData path;
    DataTypePtr scalar_type;
    ColumnUInt8::MutablePtr presence;
    MutableColumnPtr typed_values;
    ColumnUInt8::MutablePtr typed_nulls;
    VariantBatchBuilder encoded_values;
    const IColumn* materialized_values = nullptr;
    const uint8_t* materialized_nulls = nullptr;
    MutableColumnPtr materialized_owner;
    DorisVector<uint32_t> materialized_rows;
    size_t expected_rows;
    bool typed_possible;
};

} // namespace

bool VariantShreddedColumnBuilder::supports_direct_typed_append(PrimitiveType type) noexcept {
    switch (type) {
    case TYPE_BOOLEAN:
    case TYPE_TINYINT:
    case TYPE_SMALLINT:
    case TYPE_INT:
    case TYPE_BIGINT:
    case TYPE_FLOAT:
    case TYPE_DOUBLE:
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return true;
    default:
        return false;
    }
}

struct VariantShreddedColumnBuilder::Batch::Impl {
    Impl(const Layout& input_layout, const DorisVector<size_t>& input_raw_path_order, size_t rows)
            : layout(input_layout),
              raw_path_order(input_raw_path_order),
              selected(layout.size(), 0) {
        field_builders.reserve(layout.size());
        for (const auto& entry : layout) {
            field_builders.emplace_back(entry, rows);
        }
        active_paths.reserve(layout.size());
    }

    Layout layout;
    DorisVector<ShreddedFieldBuilder> field_builders;
    DorisVector<size_t> raw_path_order;
    DorisVector<uint8_t> selected;
    DorisVector<size_t> active_paths;
    DorisVector<char> scalar_scratch;
    size_t completed_rows = 0;
};

VariantShreddedColumnBuilder::Batch::Batch(const DorisVector<VariantShreddedLayoutEntry>& layout,
                                           const DorisVector<size_t>& raw_path_order, size_t rows)
        : _impl(std::make_unique<Impl>(layout, raw_path_order, rows)) {}

VariantShreddedColumnBuilder::Batch::~Batch() = default;
VariantShreddedColumnBuilder::Batch::Batch(Batch&&) noexcept = default;
VariantShreddedColumnBuilder::Batch& VariantShreddedColumnBuilder::Batch::operator=(
        Batch&&) noexcept = default;

std::optional<size_t> VariantShreddedColumnBuilder::Batch::find_path(
        const PathInData& path) const noexcept {
    const auto candidate =
            std::lower_bound(_impl->layout.begin(), _impl->layout.end(), path,
                             [](const VariantShreddedLayoutEntry& entry, const PathInData& needle) {
                                 return variant_shredded_path_less(entry.path, needle);
                             });
    if (candidate == _impl->layout.end() || candidate->path.get_parts() != path.get_parts()) {
        return std::nullopt;
    }
    return static_cast<size_t>(candidate - _impl->layout.begin());
}

std::optional<size_t> VariantShreddedColumnBuilder::Batch::find_raw_path(
        StringRef path, uint32_t depth) const noexcept {
    const std::string_view raw_path =
            path.size == 0 ? std::string_view {} : std::string_view(path.data, path.size);
    const auto candidate = std::lower_bound(
            _impl->raw_path_order.begin(), _impl->raw_path_order.end(),
            std::pair {raw_path, static_cast<size_t>(depth)},
            [this](size_t index, const std::pair<std::string_view, size_t>& needle) {
                const PathInData& entry = _impl->layout[index].path;
                const int raw_comparison = entry.get_path().compare(needle.first);
                return raw_comparison < 0 ||
                       (raw_comparison == 0 && entry.get_parts().size() < needle.second);
            });
    for (auto current = candidate; current != _impl->raw_path_order.end(); ++current) {
        const size_t index = *current;
        const PathInData& entry = _impl->layout[index].path;
        if (entry.get_path().compare(raw_path) != 0 || entry.get_parts().size() != depth) {
            break;
        }
        if (std::ranges::all_of(entry.get_parts(), is_plain_object_part)) {
            return index;
        }
    }
    return std::nullopt;
}

bool VariantShreddedColumnBuilder::Batch::append_value(size_t path_index, VariantRef value) {
    DORIS_CHECK_LT(path_index, _impl->field_builders.size());
    if (_impl->field_builders[path_index].has_materialized_source()) {
        return false;
    }
    if (value.basic_type() == VariantBasicType::OBJECT ||
        value.basic_type() == VariantBasicType::ARRAY) {
        return false;
    }
    DORIS_CHECK_EQ(_impl->selected[path_index], 0);
    if (!_impl->field_builders[path_index].append_selected(value)) {
        return false;
    }
    _impl->selected[path_index] = 1;
    return true;
}

void VariantShreddedColumnBuilder::Batch::append_scalar(size_t path_index,
                                                        const VariantScalarRef& scalar) {
    _impl->scalar_scratch.resize(scalar.encoded_size());
    scalar.write_physical(_impl->scalar_scratch.data(), _impl->scalar_scratch.size());
    const VariantRef value {.metadata = {.data = VARIANT_EMPTY_METADATA.data(),
                                         .size = VARIANT_EMPTY_METADATA.size()},
                            .value = {_impl->scalar_scratch.data(), _impl->scalar_scratch.size()}};
    DORIS_CHECK(append_value(path_index, value));
}

void VariantShreddedColumnBuilder::Batch::bind_materialized_source(size_t path_index,
                                                                   const IColumn& values,
                                                                   const uint8_t* nulls,
                                                                   MutableColumnPtr owner) {
    DORIS_CHECK_EQ(_impl->completed_rows, 0);
    DORIS_CHECK_LT(path_index, _impl->field_builders.size());
    _impl->field_builders[path_index].bind_materialized_source(values, nulls, std::move(owner));
}

void VariantShreddedColumnBuilder::Batch::append_materialized(size_t path_index,
                                                              size_t source_row) {
    DORIS_CHECK_LT(path_index, _impl->field_builders.size());
    DORIS_CHECK_EQ(_impl->selected[path_index], 0);
    _impl->field_builders[path_index].append_materialized(source_row);
    _impl->selected[path_index] = 1;
}

void VariantShreddedColumnBuilder::Batch::append_root(VariantRef root,
                                                      VariantBatchBuilder::Row& residual,
                                                      bool extract) {
    _impl->active_paths.clear();
    if (extract) {
        for (size_t path_index = 0; path_index < _impl->layout.size(); ++path_index) {
            if (_impl->field_builders[path_index].has_materialized_source()) {
                continue;
            }
            VariantRef leaf;
            if (find_scalar_leaf(root, _impl->layout[path_index].path, &leaf) &&
                append_value(path_index, leaf)) {
                _impl->active_paths.push_back(path_index);
            }
        }
    }
    append_residual_value(residual, root, _impl->active_paths, 0, _impl->layout);
}

void VariantShreddedColumnBuilder::Batch::finish_row() {
    for (size_t path_index = 0; path_index < _impl->field_builders.size(); ++path_index) {
        if (_impl->selected[path_index] == 0) {
            _impl->field_builders[path_index].append_missing();
        }
        _impl->selected[path_index] = 0;
    }
    ++_impl->completed_rows;
}

ColumnVariantV2::MutablePtr VariantShreddedColumnBuilder::Batch::finish(
        ColumnVariantV2::MutablePtr residual) {
    DORIS_CHECK(static_cast<bool>(residual));
    DORIS_CHECK(residual->is_encoded());
    DORIS_CHECK_EQ(residual->size(), _impl->completed_rows);
    ColumnVariantV2::ShreddedFields fields;
    fields.reserve(_impl->field_builders.size());
    for (auto& field_builder : _impl->field_builders) {
        fields.emplace_back(std::move(field_builder.path), field_builder.finish_values(),
                            std::move(field_builder.presence));
    }
    return VariantShreddedColumnBuilder::publish_validated(std::move(residual), std::move(fields));
}

ColumnVariantV2::MutablePtr VariantShreddedColumnBuilder::publish_validated(
        ColumnVariantV2::MutablePtr residual, ColumnVariantV2::ShreddedFields fields) {
    return ColumnVariantV2::_create_shredded_from_valid_parts(std::move(residual),
                                                              std::move(fields));
}

VariantShreddedColumnBuilder::VariantShreddedColumnBuilder(
        DorisVector<VariantShreddedLayoutEntry> layout) {
    if (layout.empty()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant shredded layout must contain at least one path");
    }
    _layout.reserve(layout.size());
    for (auto& entry : layout) {
        if (entry.path.empty()) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant shredded layout cannot contain the whole-root path");
        }
        if (!entry.scalar_type) {
            throw Exception(ErrorCode::INVALID_ARGUMENT,
                            "Variant shredded layout path {} has no planned scalar type",
                            entry.path.get_path());
        }
        _layout.push_back({PathInData(entry.path.get_parts()), remove_nullable(entry.scalar_type)});
    }
    std::ranges::sort(_layout, [](const auto& left, const auto& right) {
        return variant_shredded_path_less(left.path, right.path);
    });
    for (size_t index = 1; index < _layout.size(); ++index) {
        if (variant_shredded_path_is_prefix(_layout[index - 1].path, _layout[index].path)) {
            throw Exception(ErrorCode::INVALID_ARGUMENT, "Variant shredded paths {} and {} overlap",
                            _layout[index - 1].path.to_jsonpath(),
                            _layout[index].path.to_jsonpath());
        }
    }
    _raw_path_order.reserve(_layout.size());
    for (size_t index = 0; index < _layout.size(); ++index) {
        _raw_path_order.push_back(index);
    }
    std::ranges::sort(_raw_path_order, [this](size_t left, size_t right) {
        const PathInData& left_path = _layout[left].path;
        const PathInData& right_path = _layout[right].path;
        if (left_path.get_path() != right_path.get_path()) {
            return left_path.get_path() < right_path.get_path();
        }
        if (left_path.get_parts().size() != right_path.get_parts().size()) {
            return left_path.get_parts().size() < right_path.get_parts().size();
        }
        return variant_shredded_path_less(left_path, right_path);
    });
}

ColumnVariantV2::MutablePtr VariantShreddedColumnBuilder::build(
        const ColumnVariantV2& encoded, std::span<const NullMap::value_type> outer_nulls) const {
#ifdef BE_TEST
    ++_test_encoded_source_builds;
#endif
    if (!encoded.is_encoded()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant shredded column builder requires encoded input, got {}",
                        encoded.get_name());
    }
    if (!outer_nulls.empty() && outer_nulls.size() != encoded.size()) {
        throw Exception(ErrorCode::INVALID_ARGUMENT,
                        "Variant shredded column builder outer null map has {} rows, expected {}",
                        outer_nulls.size(), encoded.size());
    }

    Batch batch(_layout, _raw_path_order, encoded.size());
    VariantBatchBuilder residual_builder;
    for (size_t row = 0; row < encoded.size(); ++row) {
        const VariantRef root = encoded.get_value_ref(row);
        auto residual_row = residual_builder.begin_row();
        batch.append_root(root, residual_row, outer_nulls.empty() || outer_nulls[row] == 0);
        residual_row.finish();
        batch.finish_row();
    }

    VariantBatchBuilder residual_batch = residual_builder.finish_batch();
    auto residual = ColumnVariantV2::create();
    residual->insert_encoded_batch(residual_batch);
    return batch.finish(std::move(residual));
}

VariantShreddedColumnBuilder::Batch VariantShreddedColumnBuilder::begin_batch(size_t rows) const {
#ifdef BE_TEST
    ++_test_direct_batches;
#endif
    return Batch(_layout, _raw_path_order, rows);
}

} // namespace doris
