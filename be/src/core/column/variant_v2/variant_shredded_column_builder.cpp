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
#include <numeric>
#include <optional>
#include <string_view>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2_typed_column.h"
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
bool integer_fits(int64_t value) {
    using Native = typename Column::value_type;
    return value >= static_cast<int64_t>(std::numeric_limits<Native>::min()) &&
           value <= static_cast<int64_t>(std::numeric_limits<Native>::max());
}

bool can_append_typed_value(PrimitiveType type, VariantRef value) {
    // Keep only direct, lossless identities here. Scalars that need civil-time, decimal, IP, or
    // LARGEINT reconstruction return false so the caller keeps them losslessly in residual instead
    // of duplicating CAST conversion policy in the core builder.
    if (value.basic_type() == VariantBasicType::SHORT_STRING) {
        return type == TYPE_CHAR || type == TYPE_VARCHAR || type == TYPE_STRING;
    }
    if (value.basic_type() != VariantBasicType::PRIMITIVE || value.is_null()) {
        return false;
    }

    const VariantPrimitiveId id = value.primitive_id();
    switch (type) {
    case TYPE_BOOLEAN:
        return id == VariantPrimitiveId::TRUE_VALUE || id == VariantPrimitiveId::FALSE_VALUE;
    case TYPE_TINYINT:
        return is_integer(id) && integer_fits<ColumnInt8>(value.get_int());
    case TYPE_SMALLINT:
        return is_integer(id) && integer_fits<ColumnInt16>(value.get_int());
    case TYPE_INT:
        return is_integer(id) && integer_fits<ColumnInt32>(value.get_int());
    case TYPE_BIGINT:
        return is_integer(id);
    case TYPE_FLOAT:
        return id == VariantPrimitiveId::FLOAT;
    case TYPE_DOUBLE:
        return id == VariantPrimitiveId::DOUBLE;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING:
        return id == VariantPrimitiveId::STRING;
    default:
        return false;
    }
}

void append_typed_value(IColumn& column, PrimitiveType type, VariantRef value) {
    DCHECK(can_append_typed_value(type, value));
    if (value.basic_type() == VariantBasicType::SHORT_STRING) {
        const StringRef text = value.get_string();
        assert_cast<ColumnString&>(column).insert_data(text.data, text.size);
        return;
    }

    switch (type) {
    case TYPE_BOOLEAN:
        assert_cast<ColumnUInt8&>(column).insert_value(value.get_bool());
        return;
    case TYPE_TINYINT:
        assert_cast<ColumnInt8&>(column).insert_value(static_cast<int8_t>(value.get_int()));
        return;
    case TYPE_SMALLINT:
        assert_cast<ColumnInt16&>(column).insert_value(static_cast<int16_t>(value.get_int()));
        return;
    case TYPE_INT:
        assert_cast<ColumnInt32&>(column).insert_value(static_cast<int32_t>(value.get_int()));
        return;
    case TYPE_BIGINT:
        assert_cast<ColumnInt64&>(column).insert_value(value.get_int());
        return;
    case TYPE_FLOAT:
        assert_cast<ColumnFloat32&>(column).insert_value(value.get_float());
        return;
    case TYPE_DOUBLE:
        assert_cast<ColumnFloat64&>(column).insert_value(value.get_double());
        return;
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const StringRef text = value.get_string();
        assert_cast<ColumnString&>(column).insert_data(text.data, text.size);
        return;
    }
    default:
        DORIS_CHECK(false) << "unsupported direct Variant typed append " << type;
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
              expected_rows(rows),
              typed_possible(
                      is_supported_variant_typed_identity(scalar_type->get_primitive_type())) {}

    void ensure_presence(size_t completed_rows) {
        DORIS_CHECK_LT(completed_rows, expected_rows);
        if (!presence) {
            presence = ColumnUInt8::create();
            presence->reserve(expected_rows);
            presence->insert_many_defaults(completed_rows);
        }
        DORIS_CHECK_EQ(presence->size(), completed_rows);
    }

    // NOLINTNEXTLINE(readability-make-member-function-const) -- lazily initializes value builders.
    void ensure_typed_builders(size_t completed_rows) {
        DORIS_CHECK(typed_possible);
        if (!typed_values) {
            typed_values = scalar_type->create_column();
            typed_nulls = ColumnUInt8::create();
            typed_values->reserve(expected_rows);
            typed_nulls->reserve(expected_rows);
            typed_values->insert_many_defaults(completed_rows);
            typed_nulls->insert_many_vals(1, completed_rows);
        }
        DORIS_CHECK_EQ(typed_values->size(), completed_rows);
        DORIS_CHECK_EQ(typed_nulls->size(), completed_rows);
    }

    bool has_materialized_source() const noexcept { return materialized_values != nullptr; }
    bool is_activated() const noexcept { return static_cast<bool>(presence); }

    void discard_materialized_source() {
        DORIS_CHECK(has_materialized_source());
        materialized_values = nullptr;
        materialized_nulls = nullptr;
        materialized_owner.reset();
        materialized_rows.clear();
    }

    template <typename Callback>
    void visit_materialized_scalar(size_t source_row, Callback&& callback) const {
        DORIS_CHECK(has_materialized_source());
        DORIS_CHECK_LT(source_row, expected_rows);
        DORIS_CHECK(materialized_nulls == nullptr || materialized_nulls[source_row] == 0);
        dispatch_variant_typed_column(*materialized_values, scalar_type->get_primitive_type(),
                                      [&]<PrimitiveType Type>(const auto& column) {
                                          with_variant_typed_scalar<Type>(
                                                  column, source_row,
                                                  static_cast<uint8_t>(scalar_type->get_scale()),
                                                  std::forward<Callback>(callback));
                                      });
    }

    void append_encoded_null() {
        auto row = encoded_values->begin_row();
        row.add_null();
        row.finish();
    }

    void append_encoded_value(VariantRef value) {
        auto row = encoded_values->begin_row();
        row.add_value(value);
        row.finish();
    }

    void append_encoded_materialized(size_t source_row) {
        auto row = encoded_values->begin_row();
        visit_materialized_scalar(source_row,
                                  [&](const VariantScalarRef& value) { row.add_scalar(value); });
        row.finish();
    }

    void promote_to_encoded(size_t completed_rows) {
        DORIS_CHECK(!encoded_values);
        DORIS_CHECK_EQ(presence->size(), completed_rows);
        encoded_values = std::make_unique<VariantBatchBuilder>(VariantBatchBuilder::ReserveHint {
                .rows = expected_rows,
        });
        if (has_materialized_source()) {
            for (size_t row = 0; row < completed_rows; ++row) {
                if (presence->get_data()[row] == 0) {
                    append_encoded_null();
                } else {
                    const size_t source_row = materialized_owner || materialized_rows.empty()
                                                      ? row
                                                      : static_cast<size_t>(materialized_rows[row]);
                    append_encoded_materialized(source_row);
                }
            }
        } else if (typed_values) {
            DORIS_CHECK_EQ(typed_values->size(), completed_rows);
            DORIS_CHECK_EQ(typed_nulls->size(), completed_rows);
            dispatch_variant_typed_column(
                    *typed_values, scalar_type->get_primitive_type(),
                    [&]<PrimitiveType Type>(const auto& column) {
                        for (size_t row = 0; row < completed_rows; ++row) {
                            if (typed_nulls->get_data()[row] != 0) {
                                append_encoded_null();
                            } else {
                                auto output = encoded_values->begin_row();
                                with_variant_typed_scalar<Type>(
                                        column, row, static_cast<uint8_t>(scalar_type->get_scale()),
                                        [&](const VariantScalarRef& value) {
                                            output.add_scalar(value);
                                        });
                                output.finish();
                            }
                        }
                    });
        } else {
            for (size_t row = 0; row < completed_rows; ++row) {
                append_encoded_null();
            }
        }
        typed_values.reset();
        typed_nulls.reset();
        materialized_rows.clear();
    }

    void bind_materialized_source(const IColumn& values, const uint8_t* nulls,
                                  MutableColumnPtr owner) {
        DORIS_CHECK(!has_materialized_source());
        DORIS_CHECK(typed_possible);
        DORIS_CHECK(!presence && !typed_values && !typed_nulls)
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
    }

    void append_materialized(size_t source_row, size_t completed_rows) {
        DORIS_CHECK(has_materialized_source());
        DORIS_CHECK_LT(source_row, expected_rows);
        DORIS_CHECK(materialized_nulls == nullptr || materialized_nulls[source_row] == 0)
                << "null materialized values must be represented as missing";
        if (materialized_owner) {
            DORIS_CHECK_EQ(source_row, completed_rows)
                    << "owned materialized rows must retain their physical row alignment";
        }
        ensure_presence(completed_rows);
        presence->insert_value(1);
        if (encoded_values) {
            append_encoded_materialized(source_row);
            return;
        }
        if (!materialized_owner && (!materialized_rows.empty() || source_row != completed_rows)) {
            if (materialized_rows.empty()) {
                materialized_rows.resize(completed_rows);
                std::iota(materialized_rows.begin(), materialized_rows.end(), uint32_t {0});
            }
            DORIS_CHECK_EQ(materialized_rows.size(), completed_rows);
            materialized_rows.push_back(static_cast<uint32_t>(source_row));
        }
    }

    bool append_materialized_range(size_t source_start, size_t length, size_t completed_rows) {
        DORIS_CHECK(has_materialized_source());
        DORIS_CHECK_EQ(source_start, completed_rows)
                << "bulk materialized rows must retain their physical row alignment";
        DORIS_CHECK_LE(source_start, expected_rows);
        DORIS_CHECK_LE(length, expected_rows - source_start);
        if (length == 0) {
            return false;
        }

        bool has_present = materialized_nulls == nullptr;
        if (!has_present) {
            has_present = std::any_of(materialized_nulls + source_start,
                                      materialized_nulls + source_start + length,
                                      [](uint8_t value) { return value == 0; });
        }
        if (!is_activated() && !has_present) {
            return false;
        }

        const bool was_activated = is_activated();
        ensure_presence(completed_rows);
        if (materialized_nulls == nullptr) {
            presence->insert_many_vals(1, length);
            if (encoded_values) {
                for (size_t source_row = source_start; source_row < source_start + length;
                     ++source_row) {
                    append_encoded_materialized(source_row);
                }
            }
        } else {
            auto& presence_data = presence->get_data();
            const size_t old_size = presence_data.size();
            presence_data.resize(old_size + length);
            for (size_t offset = 0; offset < length; ++offset) {
                const size_t source_row = source_start + offset;
                const bool present = materialized_nulls[source_row] == 0;
                presence_data[old_size + offset] = present ? 1 : 0;
                if (encoded_values) {
                    if (present) {
                        append_encoded_materialized(source_row);
                    } else {
                        append_encoded_null();
                    }
                }
            }
        }
        if (!materialized_owner && !materialized_rows.empty()) {
            materialized_rows.reserve(materialized_rows.size() + length);
            for (size_t source_row = source_start; source_row < source_start + length;
                 ++source_row) {
                materialized_rows.push_back(static_cast<uint32_t>(source_row));
            }
        }
        return !was_activated;
    }

    void append_missing(size_t completed_rows) {
        if (!is_activated()) {
            return;
        }
        DORIS_CHECK_EQ(presence->size(), completed_rows);
        presence->insert_value(0);
        if (encoded_values) {
            append_encoded_null();
            return;
        }
        if (has_materialized_source()) {
            DORIS_CHECK_NE(expected_rows, 0);
            if (!materialized_owner && !materialized_rows.empty()) {
                DORIS_CHECK_EQ(materialized_rows.size(), completed_rows);
                materialized_rows.push_back(static_cast<uint32_t>(completed_rows));
            }
            return;
        }
        DORIS_CHECK(typed_values && typed_nulls);
        typed_values->insert_default();
        typed_nulls->insert_value(1);
    }

    bool append_selected(VariantRef value, size_t completed_rows) {
        DORIS_CHECK(value.basic_type() != VariantBasicType::OBJECT);
        DORIS_CHECK(value.basic_type() != VariantBasicType::ARRAY);
        ensure_presence(completed_rows);
        if (encoded_values || !typed_possible || has_materialized_source() ||
            (!value.is_null() &&
             !can_append_typed_value(scalar_type->get_primitive_type(), value))) {
            if (!encoded_values) {
                promote_to_encoded(completed_rows);
            }
            append_encoded_value(value);
            presence->insert_value(1);
            return true;
        }
        ensure_typed_builders(completed_rows);
        if (value.is_null()) {
            presence->insert_value(1);
            typed_values->insert_default();
            typed_nulls->insert_value(1);
            return true;
        }
        append_typed_value(*typed_values, scalar_type->get_primitive_type(), value);
        presence->insert_value(1);
        typed_nulls->insert_value(0);
        return true;
    }

    // NOLINTNEXTLINE(readability-make-member-function-const) -- consumes the value builders.
    MutableColumnPtr finish_values() {
        DORIS_CHECK(is_activated());
        DORIS_CHECK_EQ(presence->size(), expected_rows);
        if (encoded_values) {
            VariantBatchBuilder encoded = encoded_values->finish_batch();
            auto result = ColumnVariantV2::create();
            result->insert_encoded_batch(encoded);
            return result;
        }
        if (has_materialized_source()) {
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
                values = scalar_type->create_column();
                if (materialized_rows.empty()) {
                    values->insert_range_from(*materialized_values, 0, expected_rows);
                } else {
                    DORIS_CHECK_EQ(materialized_rows.size(), expected_rows);
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
        DORIS_CHECK(typed_possible && typed_values && typed_nulls);
        DORIS_CHECK_EQ(typed_values->size(), expected_rows);
        DORIS_CHECK_EQ(typed_nulls->size(), expected_rows);
        MutableColumnPtr nullable =
                ColumnNullable::create(std::move(typed_values), std::move(typed_nulls));
        return ColumnVariantV2::create_typed(std::move(nullable), scalar_type);
    }

    PathInData path;
    DataTypePtr scalar_type;
    ColumnUInt8::MutablePtr presence;
    MutableColumnPtr typed_values;
    ColumnUInt8::MutablePtr typed_nulls;
    const IColumn* materialized_values = nullptr;
    const uint8_t* materialized_nulls = nullptr;
    MutableColumnPtr materialized_owner;
    DorisVector<uint32_t> materialized_rows;
    std::unique_ptr<VariantBatchBuilder> encoded_values;
    size_t expected_rows;
    bool typed_possible;
};

} // namespace

struct VariantShreddedColumnBuilder::Batch::Impl {
    Impl(const Layout& input_layout, const DorisVector<size_t>& input_raw_path_order, size_t rows)
            : layout(input_layout),
              raw_path_order(input_raw_path_order),
              selected(layout.size(), 0),
              expected_rows(rows) {
        field_builders.reserve(layout.size());
        for (const auto& entry : layout) {
            field_builders.emplace_back(entry, rows);
        }
        active_paths.reserve(layout.size());
        activated_path_indices.reserve(layout.size());
    }

    Layout layout;
    DorisVector<ShreddedFieldBuilder> field_builders;
    DorisVector<size_t> raw_path_order;
    DorisVector<uint8_t> selected;
    // Batch-global fields that have produced at least one value. Only these fields need per-row
    // missing maintenance; active_paths below is a separate row-local residual projection set.
    DorisVector<size_t> activated_path_indices;
    DorisVector<size_t> active_paths;
    size_t expected_rows;
    size_t completed_rows = 0;
    size_t materialized_source_count = 0;
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
    auto& field_builder = _impl->field_builders[path_index];
    if (value.basic_type() == VariantBasicType::OBJECT ||
        value.basic_type() == VariantBasicType::ARRAY) {
        return false;
    }
    DORIS_CHECK_EQ(_impl->selected[path_index], 0);
    const bool was_activated = field_builder.is_activated();
    DORIS_CHECK(field_builder.append_selected(value, _impl->completed_rows));
    if (!was_activated) {
        _impl->activated_path_indices.push_back(path_index);
    }
    _impl->selected[path_index] = 1;
    return true;
}

void VariantShreddedColumnBuilder::Batch::bind_materialized_source(size_t path_index,
                                                                   const IColumn& values,
                                                                   const uint8_t* nulls,
                                                                   MutableColumnPtr owner) {
    DORIS_CHECK_EQ(_impl->completed_rows, 0);
    DORIS_CHECK_LT(path_index, _impl->field_builders.size());
    DORIS_CHECK(!_impl->field_builders[path_index].has_materialized_source());
    _impl->field_builders[path_index].bind_materialized_source(values, nulls, std::move(owner));
    ++_impl->materialized_source_count;
}

void VariantShreddedColumnBuilder::Batch::append_materialized(size_t path_index,
                                                              size_t source_row) {
    DORIS_CHECK_LT(path_index, _impl->field_builders.size());
    DORIS_CHECK_EQ(_impl->selected[path_index], 0);
    auto& field_builder = _impl->field_builders[path_index];
    const bool was_activated = field_builder.is_activated();
    field_builder.append_materialized(source_row, _impl->completed_rows);
    if (!was_activated) {
        _impl->activated_path_indices.push_back(path_index);
    }
    _impl->selected[path_index] = 1;
}

void VariantShreddedColumnBuilder::Batch::append_bound_materialized_range(size_t source_start,
                                                                          size_t length) {
    DORIS_CHECK_EQ(_impl->materialized_source_count, _impl->field_builders.size());
    DORIS_CHECK_EQ(source_start, _impl->completed_rows);
    DORIS_CHECK_LE(source_start, _impl->expected_rows);
    DORIS_CHECK_LE(length, _impl->expected_rows - source_start);
    DORIS_CHECK(std::ranges::all_of(_impl->selected, [](uint8_t value) { return value == 0; }));
    for (size_t path_index = 0; path_index < _impl->field_builders.size(); ++path_index) {
        if (_impl->field_builders[path_index].append_materialized_range(source_start, length,
                                                                        _impl->completed_rows)) {
            _impl->activated_path_indices.push_back(path_index);
        }
    }
    _impl->completed_rows += length;
}

void VariantShreddedColumnBuilder::Batch::append_root(VariantRef root,
                                                      VariantBatchBuilder::Row& residual,
                                                      bool extract) {
    _impl->active_paths.clear();
    // Production assembly binds every layout path to its materialized column before the row loop.
    // In that common case none of the root residual's scalar leaves can own a child, so avoid an
    // otherwise empty O(rows * layout_paths) probe.
    if (extract && _impl->materialized_source_count != _impl->layout.size()) {
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
    DORIS_CHECK_LT(_impl->completed_rows, _impl->expected_rows)
            << "Variant shredded batch completed more rows than declared";
    for (size_t path_index : _impl->activated_path_indices) {
        if (_impl->selected[path_index] == 0) {
            _impl->field_builders[path_index].append_missing(_impl->completed_rows);
        }
        _impl->selected[path_index] = 0;
    }
    ++_impl->completed_rows;
}

ColumnVariantV2::MutablePtr VariantShreddedColumnBuilder::Batch::finish(
        ColumnVariantV2::MutablePtr residual) {
    DORIS_CHECK(static_cast<bool>(residual));
    DORIS_CHECK(residual->is_encoded());
    DORIS_CHECK_EQ(_impl->completed_rows, _impl->expected_rows)
            << "Variant shredded batch did not complete every declared row";
    DORIS_CHECK_EQ(residual->size(), _impl->completed_rows);
    ColumnVariantV2::ShreddedFields fields;
    fields.reserve(_impl->field_builders.size());
    for (auto& field_builder : _impl->field_builders) {
        if (!field_builder.is_activated()) {
            field_builder.presence = ColumnUInt8::create();
            field_builder.presence->insert_many_defaults(_impl->expected_rows);
            if (field_builder.has_materialized_source()) {
                DORIS_CHECK(field_builder.typed_possible);
                field_builder.discard_materialized_source();
            }
            if (field_builder.typed_possible) {
                field_builder.typed_values = field_builder.scalar_type->create_column();
                field_builder.typed_values->insert_many_defaults(_impl->expected_rows);
                field_builder.typed_nulls = ColumnUInt8::create();
                field_builder.typed_nulls->insert_many_vals(1, _impl->expected_rows);
            } else {
                field_builder.encoded_values =
                        std::make_unique<VariantBatchBuilder>(VariantBatchBuilder::ReserveHint {
                                .rows = _impl->expected_rows,
                        });
                for (size_t row = 0; row < _impl->expected_rows; ++row) {
                    field_builder.append_encoded_null();
                }
            }
        }
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
