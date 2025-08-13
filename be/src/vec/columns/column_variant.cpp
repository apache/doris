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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnVariant.cpp
// and modified by Doris

#include "vec/columns/column_variant.h"

#include <assert.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "util/defer_op.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_utils.h"
#include "util/simd/bits.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/unaligned.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/convert_field_to_type.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {
namespace {

#include "common/compile_check_begin.h"

DataTypePtr create_array_of_type(PrimitiveType type, size_t num_dimensions, bool is_nullable,
                                 int precision = -1, int scale = -1) {
    DataTypePtr result = type == PrimitiveType::INVALID_TYPE
                                 ? is_nullable ? make_nullable(std::make_shared<DataTypeNothing>())
                                               : std::dynamic_pointer_cast<IDataType>(
                                                         std::make_shared<DataTypeNothing>())
                                 : DataTypeFactory::instance().create_data_type(type, is_nullable,
                                                                                precision, scale);
    for (size_t i = 0; i < num_dimensions; ++i) {
        result = std::make_shared<DataTypeArray>(result);
        if (is_nullable) {
            // wrap array with nullable
            result = make_nullable(result);
        }
    }
    return result;
}

DataTypePtr get_base_type_of_array(const DataTypePtr& type) {
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray* last_array = nullptr;
    const auto* current_type = type.get();
    if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
        current_type = nullable->get_nested_type().get();
    }
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
        if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
            current_type = nullable->get_nested_type().get();
        }
    }
    return last_array ? last_array->get_nested_type() : type;
}

size_t get_number_of_dimensions(const IDataType& type) {
    int num_dimensions = 0;
    const auto* current_type = &type;
    if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
        current_type = nullable->get_nested_type().get();
    }
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        num_dimensions += 1;
        if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
            current_type = nullable->get_nested_type().get();
        }
    }
    return num_dimensions;
}
} // namespace

// current nested level is 2, inside column object
constexpr int CURRENT_SERIALIZE_NESTING_LEVEL = 2;

DataTypeSerDeSPtr ColumnVariant::Subcolumn::generate_data_serdes(DataTypePtr type, bool is_root) {
    // For the root column, there is no path, so there is no need to add extra '"'
    if (is_root) {
        return type->get_serde(CURRENT_SERIALIZE_NESTING_LEVEL - 1);
    } else {
        return type->get_serde(CURRENT_SERIALIZE_NESTING_LEVEL);
    }
}

ColumnVariant::Subcolumn::Subcolumn(MutableColumnPtr&& data_, DataTypePtr type, bool is_nullable_,
                                    bool is_root_)
        : least_common_type(type),
          is_nullable(is_nullable_),
          is_root(is_root_),
          num_rows(data_->size()) {
    data.push_back(std::move(data_));
    data_types.push_back(type);
    data_serdes.push_back(generate_data_serdes(type, is_root));
    DCHECK_EQ(data.size(), data_types.size());
    DCHECK_EQ(data.size(), data_serdes.size());
}

ColumnVariant::Subcolumn::Subcolumn(size_t size_, bool is_nullable_, bool is_root_)
        : least_common_type(std::make_shared<DataTypeNothing>()),
          is_nullable(is_nullable_),
          num_of_defaults_in_prefix(size_),
          is_root(is_root_),
          num_rows(size_) {}

size_t ColumnVariant::Subcolumn::Subcolumn::size() const {
    return num_rows + current_num_of_defaults;
}

size_t ColumnVariant::Subcolumn::Subcolumn::byteSize() const {
    size_t res = 0;
    for (const auto& part : data) {
        res += part->byte_size();
    }
    return res;
}

size_t ColumnVariant::Subcolumn::Subcolumn::allocatedBytes() const {
    size_t res = 0;
    for (const auto& part : data) {
        res += part->allocated_bytes();
    }
    return res;
}

void ColumnVariant::Subcolumn::insert(FieldWithDataType field) {
    FieldInfo info;
    info.precision = field.precision;
    info.scale = field.scale;
    info.scalar_type_id = field.base_scalar_type_id;
    info.num_dimensions = field.num_dimensions;
    insert(std::move(field.field), info);
}

void ColumnVariant::Subcolumn::insert(Field field) {
    FieldInfo info;
    schema_util::get_field_info(field, &info);
    insert(std::move(field), info);
}

void ColumnVariant::Subcolumn::add_new_column_part(DataTypePtr type) {
    data.push_back(type->create_column());
    least_common_type = LeastCommonType {type, is_root};
    data_types.push_back(type);
    data_serdes.push_back(generate_data_serdes(type, is_root));
    DCHECK_EQ(data.size(), data_types.size());
    DCHECK_EQ(data.size(), data_serdes.size());
}

void ColumnVariant::Subcolumn::insert(Field field, FieldInfo info) {
    if (field.is_null()) {
        insert_default();
        return;
    }
    auto from_type_id = info.scalar_type_id;
    auto from_dim = info.num_dimensions;
    auto least_common_type_id = least_common_type.get_base_type_id();
    auto least_common_type_dim = least_common_type.get_dimensions();
    bool type_changed = info.need_convert;
    if (data.empty()) {
        if (from_dim > 1) {
            add_new_column_part(create_array_of_type(PrimitiveType::TYPE_JSONB, 0, is_nullable));
            type_changed = true;
        } else {
            add_new_column_part(create_array_of_type(from_type_id, from_dim, is_nullable,
                                                     info.precision, info.scale));
        }
    } else {
        if (least_common_type_dim != from_dim) {
            add_new_column_part(create_array_of_type(PrimitiveType::TYPE_JSONB, 0, is_nullable));
            if (from_type_id != PrimitiveType::TYPE_JSONB || from_dim != 0) {
                type_changed = true;
            }
        } else {
            if (least_common_type_id != from_type_id &&
                schema_util::is_conversion_required_between_integers(from_type_id,
                                                                     least_common_type_id)) {
                type_changed = true;
                DataTypePtr new_least_common_base_type;
                get_least_supertype_jsonb(PrimitiveTypeSet {from_type_id, least_common_type_id},
                                          &new_least_common_base_type);
                if (new_least_common_base_type->get_primitive_type() != least_common_type_id) {
                    add_new_column_part(
                            create_array_of_type(new_least_common_base_type->get_primitive_type(),
                                                 least_common_type_dim, is_nullable));
                }
            }
        }
    }

    if (type_changed) {
        Field new_field;
        convert_field_to_type(field, *least_common_type.get(), &new_field);
        field = new_field;
    }
    ++num_rows;
    data.back()->insert(field);
}

static DataTypePtr create_array(PrimitiveType type, size_t num_dimensions) {
    if (type == PrimitiveType::INVALID_TYPE) {
        return std::make_shared<DataTypeNothing>();
    }
    DataTypePtr result_type = DataTypeFactory::instance().create_data_type(type, true);
    for (size_t i = 0; i < num_dimensions; ++i) {
        result_type = make_nullable(std::make_shared<DataTypeArray>(result_type));
    }
    return result_type;
}

// Recreates column with default scalar values and keeps sizes of arrays.
static ColumnPtr recreate_column_with_default_values(const ColumnPtr& column,
                                                     PrimitiveType scalar_type,
                                                     size_t num_dimensions) {
    const auto* column_array = check_and_get_column<ColumnArray>(remove_nullable(column).get());
    if (column_array != nullptr && num_dimensions != 0) {
        return make_nullable(ColumnArray::create(
                recreate_column_with_default_values(column_array->get_data_ptr(), scalar_type,
                                                    num_dimensions - 1),
                IColumn::mutate(column_array->get_offsets_ptr())));
    }

    return create_array(scalar_type, num_dimensions)
            ->create_column()
            ->clone_resized(column->size());
}

ColumnVariant::Subcolumn ColumnVariant::Subcolumn::clone_with_default_values(
        const FieldInfo& field_info) const {
    Subcolumn new_subcolumn(*this);
    new_subcolumn.least_common_type = LeastCommonType {
            create_array(field_info.scalar_type_id, field_info.num_dimensions), is_root};

    for (int i = 0; i < new_subcolumn.data.size(); ++i) {
        new_subcolumn.data[i] = recreate_column_with_default_values(
                new_subcolumn.data[i], field_info.scalar_type_id, field_info.num_dimensions);
        new_subcolumn.data_types[i] = create_array_of_type(field_info.scalar_type_id,
                                                           field_info.num_dimensions, is_nullable);
        new_subcolumn.data_serdes[i] = generate_data_serdes(new_subcolumn.data_types[i], false);
    }

    return new_subcolumn;
}

Field ColumnVariant::Subcolumn::get_last_field() const {
    if (data.empty()) {
        return Field();
    }

    const auto& last_part = data.back();
    assert(!last_part->empty());
    return (*last_part)[last_part->size() - 1];
}

void ColumnVariant::Subcolumn::insert_range_from(const Subcolumn& src, size_t start,
                                                 size_t length) {
    if (start + length > src.size()) {
        throw doris::Exception(
                ErrorCode::OUT_OF_BOUND,
                "Invalid range for insert_range_from: start={}, length={}, src.size={}", start,
                length, src.size());
    }
    size_t end = start + length;
    num_rows += length;
    if (data.empty()) {
        add_new_column_part(src.get_least_common_type());
    } else if (!least_common_type.get()->equals(*src.get_least_common_type())) {
        DataTypePtr new_least_common_type;
        get_least_supertype_jsonb(DataTypes {least_common_type.get(), src.get_least_common_type()},
                                  &new_least_common_type);
        if (!new_least_common_type->equals(*least_common_type.get())) {
            add_new_column_part(std::move(new_least_common_type));
        }
    }
    if (end <= src.num_of_defaults_in_prefix) {
        data.back()->insert_many_defaults(length);
        return;
    }
    if (start < src.num_of_defaults_in_prefix) {
        data.back()->insert_many_defaults(src.num_of_defaults_in_prefix - start);
    }
    auto insert_from_part = [&](const auto& column, const auto& column_type, size_t from,
                                size_t n) {
        if (from + n > column->size()) {
            throw doris::Exception(
                    ErrorCode::OUT_OF_BOUND,
                    "Invalid range for insert_range_from: from={}, n={}, column.size={}", from, n,
                    column->size());
        }
        if (column_type->equals(*least_common_type.get())) {
            data.back()->insert_range_from(*column, from, n);
            return;
        }
        /// If we need to insert large range, there is no sense to cut part of column and cast it.
        /// Casting of all column and inserting from it can be faster.
        /// Threshold is just a guess.
        if (n * 3 >= column->size()) {
            ColumnPtr casted_column;
            Status st = schema_util::cast_column({column, column_type, ""}, least_common_type.get(),
                                                 &casted_column);
            if (!st.ok()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string());
            }
            data.back()->insert_range_from(*casted_column, from, n);
            return;
        }
        auto casted_column = column->cut(from, n);
        Status st = schema_util::cast_column({casted_column, column_type, ""},
                                             least_common_type.get(), &casted_column);
        if (!st.ok()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string());
        }
        data.back()->insert_range_from(*casted_column, 0, n);
    };
    size_t pos = 0;
    size_t processed_rows = src.num_of_defaults_in_prefix;
    /// Find the first part of the column that intersects the range.
    while (pos < src.data.size() && processed_rows + src.data[pos]->size() < start) {
        processed_rows += src.data[pos]->size();
        ++pos;
    }
    /// Insert from the first part of column.
    if (pos < src.data.size() && processed_rows < start) {
        size_t part_start = start - processed_rows;
        size_t part_length = std::min(src.data[pos]->size() - part_start, end - start);
        insert_from_part(src.data[pos], src.data_types[pos], part_start, part_length);
        processed_rows += src.data[pos]->size();
        ++pos;
    }
    /// Insert from the parts of column in the middle of range.
    while (pos < src.data.size() && processed_rows + src.data[pos]->size() < end) {
        insert_from_part(src.data[pos], src.data_types[pos], 0, src.data[pos]->size());
        processed_rows += src.data[pos]->size();
        ++pos;
    }
    /// Insert from the last part of column if needed.
    if (pos < src.data.size() && processed_rows < end) {
        size_t part_end = end - processed_rows;
        insert_from_part(src.data[pos], src.data_types[pos], 0, part_end);
    }
}

bool ColumnVariant::Subcolumn::is_finalized() const {
    return current_num_of_defaults == 0 && num_of_defaults_in_prefix == 0 &&
           (data.empty() || (data.size() == 1));
}

void ColumnVariant::Subcolumn::resize(size_t n) {
    if (n == num_rows) {
        return;
    }
    if (n > num_rows) {
        insert_many_defaults(n - num_rows);
    } else {
        pop_back(num_rows - n);
    }
}

template <typename Func>
MutableColumnPtr ColumnVariant::apply_for_columns(Func&& func) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnVariant&>(*finalized);
        return finalized_object.apply_for_columns(std::forward<Func>(func));
    }
    auto new_root = func(get_root())->assume_mutable();
    auto res = ColumnVariant::create(_max_subcolumns_count, get_root_type(), std::move(new_root));
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_root) {
            continue;
        }
        auto new_subcolumn = func(subcolumn->data.get_finalized_column_ptr());
        if (!res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable(),
                                 subcolumn->data.get_least_common_type())) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "add path {} is error",
                                   subcolumn->path.get_path());
        }
    }
    auto sparse_column = func(serialized_sparse_column);
    res->serialized_sparse_column = sparse_column->assume_mutable();
    res->num_rows = res->serialized_sparse_column->size();
    ENABLE_CHECK_CONSISTENCY(res.get());
    return res;
}

void ColumnVariant::resize(size_t n) {
    if (n == num_rows) {
        return;
    }
    if (n > num_rows) {
        insert_many_defaults(n - num_rows);
    } else {
        for (auto& subcolumn : subcolumns) {
            subcolumn->data.pop_back(num_rows - n);
        }
        serialized_sparse_column->pop_back(num_rows - n);
    }
    num_rows = n;

    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::Subcolumn::finalize(FinalizeMode mode) {
    if (current_num_of_defaults) {
        insert_many_defaults(current_num_of_defaults);
        current_num_of_defaults = 0;
    }
    if (!is_root && data.size() == 1 && num_of_defaults_in_prefix == 0) {
        data[0] = data[0]->convert_to_full_column_if_const();
        return;
    }
    DataTypePtr to_type = least_common_type.get();
    if (mode == FinalizeMode::WRITE_MODE && is_root) {
        // Root always JSONB type in write mode
        to_type = is_nullable ? make_nullable(std::make_shared<MostCommonType>())
                              : std::make_shared<MostCommonType>();
        least_common_type = LeastCommonType {to_type, is_root};
    }
    auto result_column = to_type->create_column();
    if (num_of_defaults_in_prefix) {
        result_column->insert_many_defaults(num_of_defaults_in_prefix);
    }
    for (size_t i = 0; i < data.size(); ++i) {
        auto& part = data[i];
        auto from_type = data_types[i];
        part = part->convert_to_full_column_if_const();
        size_t part_size = part->size();
        if (!from_type->equals(*to_type)) {
            ColumnPtr ptr;
            Status st = schema_util::cast_column({part, from_type, ""}, to_type, &ptr);
            if (!st.ok()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string());
            }
            part = ptr->convert_to_full_column_if_const();
        }
        result_column->insert_range_from(*part, 0, part_size);
    }
    data = {std::move(result_column)};
    data_types = {std::move(to_type)};
    data_serdes = {(generate_data_serdes(data_types[0], is_root))};

    DCHECK_EQ(data.size(), data_types.size());
    DCHECK_EQ(data.size(), data_serdes.size());

    num_of_defaults_in_prefix = 0;
}

void ColumnVariant::Subcolumn::insert_default() {
    if (UNLIKELY(data.empty())) {
        ++num_of_defaults_in_prefix;
    } else {
        data.back()->insert_default();
    }
    ++num_rows;
}

void ColumnVariant::Subcolumn::insert_many_defaults(size_t length) {
    if (data.empty()) {
        num_of_defaults_in_prefix += length;
    } else {
        data.back()->insert_many_defaults(length);
    }
    num_rows += length;
}

void ColumnVariant::Subcolumn::pop_back(size_t n) {
    if (n > size()) {
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Invalid number of elements to pop: {}, size: {}", n, size());
    }
    num_rows -= n;
    size_t num_removed = 0;
    for (auto it = data.rbegin(); it != data.rend(); ++it) {
        if (n == 0) {
            break;
        }
        auto& column = *it;
        if (n < column->size()) {
            column->pop_back(n);
            n = 0;
        } else {
            ++num_removed;
            n -= column->size();
        }
    }
    size_t sz = data.size() - num_removed;
    data.resize(sz);
    data_types.resize(sz);
    data_serdes.resize(sz);
    // need to update least_common_type when pop_back a column from the last
    least_common_type = sz > 0 ? LeastCommonType {data_types[sz - 1]}
                               : LeastCommonType {std::make_shared<DataTypeNothing>()};
    num_of_defaults_in_prefix -= n;
}

IColumn& ColumnVariant::Subcolumn::get_finalized_column() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return *data[0];
}

const IColumn& ColumnVariant::Subcolumn::get_finalized_column() const {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return *data[0];
}

const ColumnPtr& ColumnVariant::Subcolumn::get_finalized_column_ptr() const {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return data[0];
}

ColumnPtr& ColumnVariant::Subcolumn::get_finalized_column_ptr() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return data[0];
}

void ColumnVariant::Subcolumn::remove_nullable() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    data[0] = doris::vectorized::remove_nullable(data[0]);
    least_common_type.remove_nullable();
}

ColumnVariant::Subcolumn::LeastCommonType::LeastCommonType(DataTypePtr type_, bool is_root)
        : type(std::move(type_)),
          base_type(get_base_type_of_array(type)),
          num_dimensions(get_number_of_dimensions(*type)) {
    least_common_type_serder = Subcolumn::generate_data_serdes(type, is_root);
    type_id = type->get_primitive_type();
    base_type_id = base_type->get_primitive_type();
}

ColumnVariant::ColumnVariant(int32_t max_subcolumns_count)
        : is_nullable(true), num_rows(0), _max_subcolumns_count(max_subcolumns_count) {
    subcolumns.create_root(Subcolumn(0, is_nullable, true /*root*/));
    ENABLE_CHECK_CONSISTENCY(this);
}

ColumnVariant::ColumnVariant(int32_t max_subcolumns_count, DataTypePtr root_type,
                             MutableColumnPtr&& root_column)
        : is_nullable(true),
          num_rows(root_column->size()),
          _max_subcolumns_count(max_subcolumns_count) {
    subcolumns.create_root(
            Subcolumn(std::move(root_column), root_type, is_nullable, true /*root*/));
    serialized_sparse_column->resize(num_rows);
    ENABLE_CHECK_CONSISTENCY(this);
}

ColumnVariant::ColumnVariant(int32_t max_subcolumns_count, Subcolumns&& subcolumns_)
        : is_nullable(true),
          subcolumns(std::move(subcolumns_)),
          num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->data.size()),
          _max_subcolumns_count(max_subcolumns_count) {
    if (max_subcolumns_count && subcolumns_.size() > max_subcolumns_count + 1) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "unmatched max subcolumns count:, max subcolumns count: {}, but "
                               "subcolumns count: {}",
                               max_subcolumns_count, subcolumns_.size());
    }
    serialized_sparse_column->resize(num_rows);
}

ColumnVariant::ColumnVariant(int32_t max_subcolumns_count, size_t size)
        : is_nullable(true), num_rows(0), _max_subcolumns_count(max_subcolumns_count) {
    subcolumns.create_root(Subcolumn(0, is_nullable, true /*root*/));
    insert_many_defaults(size);
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::check_consistency() const {
    CHECK(subcolumns.get_root() != nullptr);
    for (const auto& leaf : subcolumns) {
        if (num_rows != leaf->data.size()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "unmatched column: {}, expeted rows: {}, but meet: {}",
                                   leaf->path.get_path(), num_rows, leaf->data.size());
        }
    }
    if (num_rows != serialized_sparse_column->size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "unmatched sparse column:, expeted rows: {}, but meet: {}", num_rows,
                               serialized_sparse_column->size());
    }
}

size_t ColumnVariant::size() const {
    ENABLE_CHECK_CONSISTENCY(this);
    return num_rows;
}

MutableColumnPtr ColumnVariant::clone_resized(size_t new_size) const {
    if (new_size == 0) {
        return ColumnVariant::create(_max_subcolumns_count);
    }
    return apply_for_columns(
            [&](const ColumnPtr column) { return column->clone_resized(new_size); });
}

size_t ColumnVariant::byte_size() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.byteSize();
    }
    res += serialized_sparse_column->byte_size();
    return res;
}

size_t ColumnVariant::allocated_bytes() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.allocatedBytes();
    }
    res += serialized_sparse_column->allocated_bytes();
    return res;
}

void ColumnVariant::for_each_subcolumn(ColumnCallback callback) {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            callback(part);
        }
    }
    callback(serialized_sparse_column);
    // callback may be filter, so the row count may be changed
    num_rows = serialized_sparse_column->size();
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::insert_from(const IColumn& src, size_t n) {
    const auto* src_v = check_and_get_column<ColumnVariant>(src);
    return try_insert((*src_v)[n]);
}

void ColumnVariant::try_insert(const Field& field) {
    size_t old_size = size();
    const auto& object = field.get<const VariantMap&>();
    for (const auto& [key, value] : object) {
        if (!has_subcolumn(key)) {
            bool succ = add_sub_column(key, old_size);
            if (!succ) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to add sub column {}", key.get_path());
            }
        }
        auto* subcolumn = get_subcolumn(key);
        if (!subcolumn) {
            doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                             fmt::format("Failed to find sub column {}", key.get_path()));
        }
        if (value.base_scalar_type_id == PrimitiveType::INVALID_TYPE) {
            // deduce type info from field
            subcolumn->insert(value.field);
        } else {
            // use info from value
            subcolumn->insert(value);
        }
    }
    for (auto& entry : subcolumns) {
        if (old_size == entry->data.size()) {
            bool inserted = UNLIKELY(entry->path.has_nested_part() &&
                                     try_insert_default_from_nested(entry));
            if (!inserted) {
                entry->data.insert_default();
            }
        }
    }
    serialized_sparse_column->insert_default();
    ++num_rows;
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::insert_default() {
    for (auto& entry : subcolumns) {
        entry->data.insert_default();
    }
    serialized_sparse_column->insert_default();
    ++num_rows;
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::insert_many_defaults(size_t length) {
    for (auto& entry : subcolumns) {
        entry->data.insert_many_defaults(length);
    }
    serialized_sparse_column->resize(num_rows + length);
    num_rows += length;
    ENABLE_CHECK_CONSISTENCY(this);
}

bool ColumnVariant::Subcolumn::is_null_at(size_t n) const {
    if (least_common_type.get_base_type_id() == PrimitiveType::INVALID_TYPE) {
        return true;
    }
    size_t ind = n;
    if (ind < num_of_defaults_in_prefix) {
        return true;
    }

    ind -= num_of_defaults_in_prefix;
    for (const auto& part : data) {
        if (ind < part->size()) {
            const auto* nullable = check_and_get_column<ColumnNullable>(part.get());
            return nullable ? nullable->is_null_at(ind) : false;
        }
        ind -= part->size();
    }
    throw doris::Exception(ErrorCode::OUT_OF_BOUND, "Index ({}) for getting field is out of range",
                           n);
}

void ColumnVariant::Subcolumn::get(size_t n, FieldWithDataType& res) const {
    if (least_common_type.get_base_type_id() == PrimitiveType::INVALID_TYPE) {
        res = FieldWithDataType(Field());
        return;
    }
    if (is_finalized()) {
        res = least_common_type.get()->get_field_with_data_type(*data[0], n);
        return;
    }
    size_t ind = n;
    if (ind < num_of_defaults_in_prefix) {
        res = FieldWithDataType(Field());
        return;
    }

    ind -= num_of_defaults_in_prefix;
    for (size_t i = 0; i < data.size(); ++i) {
        const auto& part = data[i];
        const auto& part_type = data_types[i];
        if (ind < part->size()) {
            res = part_type->get_field_with_data_type(*part, ind);
            return;
        }

        ind -= part->size();
    }
    throw doris::Exception(ErrorCode::OUT_OF_BOUND, "Index ({}) for getting field is out of range",
                           n);
}

void ColumnVariant::Subcolumn::serialize_to_sparse_column(ColumnString* key, std::string_view path,
                                                          ColumnString* value, size_t row) {
    // no need insert
    if (least_common_type.get_base_type_id() == PrimitiveType::INVALID_TYPE) {
        return;
    }

    // no need insert
    if (row < num_of_defaults_in_prefix) {
        return;
    }

    // remove default
    row -= num_of_defaults_in_prefix;
    for (size_t i = 0; i < data.size(); ++i) {
        const auto& part = data[i];
        const auto& nullable_col =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*part);
        size_t current_column_size = nullable_col.get_null_map_data().size();
        if (row < current_column_size) {
            // no need null in sparse column
            if (!nullable_col.is_null_at(row)) {
                // insert key
                key->insert_data(path.data(), path.size());

                // every subcolumn is always Nullable
                auto nullable_serde =
                        std::static_pointer_cast<DataTypeNullableSerDe>(data_serdes[i]);
                // insert value
                ColumnString::Chars& chars = value->get_chars();
                nullable_serde->get_nested_serde()->write_one_cell_to_binary(
                        nullable_col.get_nested_column(), chars, row);
                value->get_offsets().push_back(chars.size());
            }
            return;
        }

        row -= current_column_size;
    }

    throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                           "Index ({}) for serialize to sparse column is out of range", row);
}

struct PackedUInt128 {
    // PackedInt128() : value(0) {}
    PackedUInt128() = default;

    PackedUInt128(const unsigned __int128& value_) { value = value_; }
    PackedUInt128& operator=(const unsigned __int128& value_) {
        value = value_;
        return *this;
    }
    PackedUInt128& operator=(const PackedUInt128& rhs) = default;

    uint128_t value;
} __attribute__((packed));

const NO_SANITIZE_UNDEFINED char* parse_binary_from_sparse_column(FieldType type, const char* data,
                                                                  Field& res, FieldInfo& info_res) {
    info_res.scalar_type_id = TabletColumn::get_primitive_type_by_field_type(type);
    const char* end = data;
    switch (type) {
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        size_t size = unaligned_load<size_t>(data);
        data += sizeof(size_t);
        res = Field::create_field<TYPE_STRING>(String(data, size));
        end = data + size;
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        Int8 v = unaligned_load<Int8>(data);
        res = Field::create_field<TYPE_TINYINT>(v);
        end = data + sizeof(Int8);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        Int16 v = unaligned_load<Int16>(data);
        res = Field::create_field<TYPE_SMALLINT>(v);
        end = data + sizeof(Int16);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        Int32 v = unaligned_load<Int32>(data);
        res = Field::create_field<TYPE_INT>(v);
        end = data + sizeof(Int32);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        Int64 v = unaligned_load<Int64>(data);
        res = Field::create_field<TYPE_BIGINT>(v);
        end = data + sizeof(Int64);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        PackedInt128 pack;
        memcpy(&pack, data, sizeof(PackedInt128));
        res = Field::create_field<TYPE_LARGEINT>(Int128(pack.value));
        end = data + sizeof(PackedInt128);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        Float32 v = unaligned_load<Float32>(data);
        res = Field::create_field<TYPE_FLOAT>(v);
        end = data + sizeof(Float32);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        Float64 v = unaligned_load<Float64>(data);
        res = Field::create_field<TYPE_DOUBLE>(v);
        end = data + sizeof(Float64);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        size_t size = unaligned_load<size_t>(data);
        data += sizeof(size_t);
        res = Field::create_field<TYPE_JSONB>(JsonbField(data, size));
        end = data + size;
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_ARRAY: {
        const size_t size = unaligned_load<size_t>(data);
        data += sizeof(size_t);
        res = Field::create_field<TYPE_ARRAY>(Array(size));
        auto& array = res.get<Array>();
        info_res.num_dimensions++;
        FieldType nested_filed_type = FieldType::OLAP_FIELD_TYPE_NONE;
        for (size_t i = 0; i < size; ++i) {
            Field nested_field;
            const auto nested_type =
                    static_cast<FieldType>(*reinterpret_cast<const uint8_t*>(data++));
            data = parse_binary_from_sparse_column(nested_type, data, nested_field, info_res);
            array[i] = std::move(nested_field);
            if (nested_type != FieldType::OLAP_FIELD_TYPE_NONE) {
                nested_filed_type = nested_type;
            }
        }
        info_res.scalar_type_id = TabletColumn::get_primitive_type_by_field_type(nested_filed_type);
        end = data;
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4: {
        IPv4 v = unaligned_load<IPv4>(data);
        res = Field::create_field<TYPE_IPV4>(v);
        end = data + sizeof(IPv4);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_IPV6: {
        PackedUInt128 pack;
        memcpy(&pack, data, sizeof(PackedUInt128));
        auto v = pack.value;
        res = Field::create_field<TYPE_IPV6>(v);
        end = data + sizeof(PackedUInt128);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        UInt32 v = unaligned_load<UInt32>(data);
        res = Field::create_field<TYPE_DATEV2>(v);
        end = data + sizeof(UInt32);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        const uint8_t scale = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        UInt64 v = unaligned_load<UInt64>(data);
        res = Field::create_field<TYPE_DATETIMEV2>(v);
        info_res.precision = -1;
        info_res.scale = static_cast<int>(scale);
        end = data + sizeof(UInt64);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        const uint8_t precision = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        const uint8_t scale = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        Int32 v = unaligned_load<Int32>(data);
        res = Field::create_field<TYPE_DECIMAL32>(Decimal32(v));
        info_res.precision = static_cast<int>(precision);
        info_res.scale = static_cast<int>(scale);
        end = data + sizeof(Int32);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        const uint8_t precision = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        const uint8_t scale = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        Int64 v = unaligned_load<Int64>(data);
        res = Field::create_field<TYPE_DECIMAL64>(Decimal64(v));
        info_res.precision = static_cast<int>(precision);
        info_res.scale = static_cast<int>(scale);
        end = data + sizeof(Int64);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        const uint8_t precision = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        const uint8_t scale = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        PackedInt128 pack;
        memcpy(&pack, data, sizeof(PackedInt128));
        res = Field::create_field<TYPE_DECIMAL128I>(Decimal128V3(pack.value));
        info_res.precision = static_cast<int>(precision);
        info_res.scale = static_cast<int>(scale);
        end = data + sizeof(PackedInt128);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        const uint8_t precision = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        const uint8_t scale = *reinterpret_cast<const uint8_t*>(data);
        data += sizeof(uint8_t);
        wide::Int256 v;
        memcpy(&v, data, sizeof(wide::Int256));
        res = Field::create_field<TYPE_DECIMAL256>(Decimal256(v));
        info_res.precision = static_cast<int>(precision);
        info_res.scale = static_cast<int>(scale);
        end = data + sizeof(wide::Int256);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        res = Field::create_field<TYPE_BOOLEAN>(*reinterpret_cast<const uint8_t*>(data));
        end = data + sizeof(uint8_t);
        break;
    }
    case FieldType::OLAP_FIELD_TYPE_NONE: {
        res = Field();
        end = data;
        break;
    }
    default:
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Type ({}) for deserialize_from_sparse_column is invalid", type);
    }
    return end;
}

std::pair<Field, FieldInfo> ColumnVariant::deserialize_from_sparse_column(const ColumnString* value,
                                                                          size_t row) {
    const auto& data_ref = value->get_data_at(row);
    const char* data = data_ref.data;
    DCHECK(data_ref.size > 1);
    const FieldType type = static_cast<FieldType>(*reinterpret_cast<const uint8_t*>(data++));
    Field res;
    FieldInfo info_res = {
            .scalar_type_id = TabletColumn::get_primitive_type_by_field_type(type),
            .have_nulls = false,
            .need_convert = false,
            .num_dimensions = 0,
    };
    const char* end = parse_binary_from_sparse_column(type, data, res, info_res);
    DCHECK_EQ(end - data_ref.data, data_ref.size)
            << "FieldType: " << (int)type << " data_ref.size: " << data_ref.size << " end: " << end
            << " data: " << data;
    return {std::move(res), std::move(info_res)};
}

Field ColumnVariant::operator[](size_t n) const {
    Field object;
    get(n, object);
    return object;
}

void ColumnVariant::get(size_t n, Field& res) const {
    if (UNLIKELY(n >= size())) {
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Index ({}) for getting field is out of range for size {}", n,
                               size());
    }
    res = Field::create_field<TYPE_VARIANT>(VariantMap());
    auto& object = res.get<VariantMap&>();

    for (const auto& entry : subcolumns) {
        FieldWithDataType field;
        entry->data.get(n, field);
        // Notice: we treat null as empty field, since we do not distinguish null and empty for Variant type.
        if (field.field.get_type() == PrimitiveType::TYPE_NULL) {
            continue;
        }
        object.try_emplace(PathInData(entry->path), std::move(field));
    }

    const auto& [path, value] = get_sparse_data_paths_and_values();
    const auto& sparse_column_offsets = serialized_sparse_column_offsets();
    size_t offset = sparse_column_offsets[n - 1];
    size_t end = sparse_column_offsets[n];
    // Iterator over [path, binary value]
    for (size_t i = offset; i != end; ++i) {
        const StringRef path_data = path->get_data_at(i);
        auto data = ColumnVariant::deserialize_from_sparse_column(value, i);
        // TODO support decimal type or etc...
        object.try_emplace(PathInData(path_data),
                           FieldWithDataType {.field = std::move(data.first),
                                              .base_scalar_type_id = data.second.scalar_type_id,
                                              .num_dimensions = static_cast<uint8_t>(
                                                      data.second.num_dimensions),
                                              .precision = data.second.precision,
                                              .scale = data.second.scale});
    }

    if (object.empty()) {
        res = Field();
    }
}

void ColumnVariant::add_nested_subcolumn(const PathInData& key, const FieldInfo& field_info,
                                         size_t new_size) {
    if (!key.has_nested_part()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Cannot add Nested subcolumn, because path doesn't contain Nested");
    }

    bool inserted = false;
    /// We find node that represents the same Nested type as @key.
    const auto* nested_node = subcolumns.find_best_match(key);
    // TODO a better way to handle following case:
    // {"a" : {"b" : [{"x" : 10}]}}
    // {"a" : {"b" : {"c": [{"y" : 10}]}}}
    // maybe a.b.c.y should not follow from a.b's nested data
    // If we have a nested subcolumn and it contains nested node in it's path
    if (nested_node &&
        Subcolumns::find_parent(nested_node, [](const auto& node) { return node.is_nested(); })) {
        /// Find any leaf of Nested subcolumn.
        const auto* leaf = Subcolumns::find_leaf(nested_node, [&](const auto&) { return true; });
        assert(leaf);

        /// Recreate subcolumn with default values and the same sizes of arrays.
        auto new_subcolumn = leaf->data.clone_with_default_values(field_info);

        /// It's possible that we have already inserted value from current row
        /// to this subcolumn. So, adjust size to expected.
        if (new_subcolumn.size() > new_size) {
            new_subcolumn.pop_back(new_subcolumn.size() - new_size);
        }

        assert(new_subcolumn.size() == new_size);
        inserted = subcolumns.add(key, new_subcolumn);
    } else {
        /// If node was not found just add subcolumn with empty arrays.
        inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));
    }

    if (!inserted) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn '{}' already exists",
                               key.get_path());
    }

    if (num_rows == 0) {
        num_rows = new_size;
    } else if (new_size != num_rows) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "Required size of subcolumn {} ({}) is inconsistent with column size ({})",
                key.get_path(), new_size, num_rows);
    }
}

void ColumnVariant::set_num_rows(size_t n) {
    num_rows = n;
}

bool ColumnVariant::try_add_new_subcolumn(const PathInData& path) {
    DCHECK(_max_subcolumns_count >= 0) << "max subcolumns count is: " << _max_subcolumns_count;
    if (subcolumns.get_root() == nullptr || path.empty()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "column object has no root or path is empty");
    }
    if (path.get_is_typed()) {
        return add_sub_column(path, num_rows);
    }
    if (path.has_nested_part()) {
        return add_sub_column(path, num_rows);
    }
    if (!_max_subcolumns_count ||
        (subcolumns.size() - typed_path_count - nested_path_count) < _max_subcolumns_count + 1) {
        return add_sub_column(path, num_rows);
    }

    return false;
}

void ColumnVariant::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& src_object = assert_cast<const ColumnVariant&>(src);
    ENABLE_CHECK_CONSISTENCY(&src_object);
    ENABLE_CHECK_CONSISTENCY(this);

    // First, insert src subcolumns
    // We can reach the limit of subcolumns, and in this case
    // the rest of subcolumns from src will be inserted into sparse column.
    std::map<std::string_view, Subcolumn> src_path_and_subcoumn_for_sparse_column;
    int idx_hint = 0;
    for (const auto& entry : src_object.subcolumns) {
        // Check if we already have such dense column path.
        if (auto* subcolumn = get_subcolumn(entry->path, idx_hint); subcolumn != nullptr) {
            subcolumn->insert_range_from(entry->data, start, length);
        } else if (try_add_new_subcolumn(entry->path)) {
            subcolumn = get_subcolumn(entry->path);
            DCHECK(subcolumn != nullptr);
            subcolumn->insert_range_from(entry->data, start, length);
        } else {
            CHECK(!entry->path.get_is_typed());
            CHECK(!entry->path.has_nested_part());
            src_path_and_subcoumn_for_sparse_column.emplace(entry->path.get_path(), entry->data);
        }
        ++idx_hint;
    }

    // Paths in sparse column are sorted, so paths from src_dense_column_path_for_sparse_column should be inserted properly
    // to keep paths sorted. Let's sort them in advance.
    std::vector<std::pair<std::string_view, Subcolumn>> sorted_src_subcolumn_for_sparse_column;
    auto it = src_path_and_subcoumn_for_sparse_column.begin();
    auto end = src_path_and_subcoumn_for_sparse_column.end();
    while (it != end) {
        sorted_src_subcolumn_for_sparse_column.emplace_back(it->first, it->second);
        ++it;
    }

    insert_from_sparse_column_and_fill_remaing_dense_column(
            src_object, std::move(sorted_src_subcolumn_for_sparse_column), start, length);

    num_rows += length;
    // finalize();
    ENABLE_CHECK_CONSISTENCY(this);
}

// std::map<std::string_view, Subcolumn>
void ColumnVariant::insert_from_sparse_column_and_fill_remaing_dense_column(
        const ColumnVariant& src,
        std::vector<std::pair<std::string_view, Subcolumn>>&&
                sorted_src_subcolumn_for_sparse_column,
        size_t start, size_t length) {
    /// Check if src object doesn't have any paths in serialized sparse column.
    const auto& src_serialized_sparse_column_offsets = src.serialized_sparse_column_offsets();
    if (src_serialized_sparse_column_offsets[start - 1] ==
        src_serialized_sparse_column_offsets[start + length - 1]) {
        size_t current_size = num_rows;

        /// If no src subcolumns should be inserted into sparse column, insert defaults.
        if (sorted_src_subcolumn_for_sparse_column.empty()) {
            serialized_sparse_column->resize(num_rows + length);
        } else {
            // Otherwise insert required src dense columns into sparse column.
            auto [sparse_column_keys, sparse_column_values] = get_sparse_data_paths_and_values();
            auto& sparse_column_offsets = serialized_sparse_column_offsets();
            for (size_t i = start; i != start + length; ++i) {
                // Paths in sorted_src_subcolumn_for_sparse_column are already sorted.
                for (auto& [path, subcolumn] : sorted_src_subcolumn_for_sparse_column) {
                    subcolumn.serialize_to_sparse_column(sparse_column_keys, path,
                                                         sparse_column_values, i);
                }
                // TODO add dcheck
                sparse_column_offsets.push_back(sparse_column_keys->size());
            }
        }

        // Insert default values in all remaining dense columns.
        for (const auto& entry : subcolumns) {
            if (entry->data.size() == current_size) {
                entry->data.insert_many_defaults(length);
            }
        }
        return;
    }

    // Src object column contains some paths in serialized sparse column in specified range.
    // Iterate over this range and insert all required paths into serialized sparse column or subcolumns.
    const auto& [src_sparse_column_path, src_sparse_column_values] =
            src.get_sparse_data_paths_and_values();
    auto [sparse_column_path, sparse_column_values] = get_sparse_data_paths_and_values();

    auto& sparse_column_offsets = serialized_sparse_column_offsets();
    for (size_t row = start; row != start + length; ++row) {
        size_t current_size = sparse_column_offsets.size();

        // Use separate index to iterate over sorted sorted_src_subcolumn_for_sparse_column.
        size_t sorted_src_subcolumn_for_sparse_column_idx = 0;
        size_t sorted_src_subcolumn_for_sparse_column_size =
                sorted_src_subcolumn_for_sparse_column.size();

        size_t offset = src_serialized_sparse_column_offsets[row - 1];
        size_t end = src_serialized_sparse_column_offsets[row];
        // Iterator over [path, binary value]
        for (size_t i = offset; i != end; ++i) {
            const StringRef src_sparse_path_string = src_sparse_column_path->get_data_at(i);
            const std::string_view src_sparse_path(src_sparse_path_string);
            // Check if we have this path in subcolumns.
            const PathInData column_path(src_sparse_path);
            if (auto* subcolumn = get_subcolumn(column_path); subcolumn != nullptr) {
                // Deserialize binary value into subcolumn from src serialized sparse column data.
                const auto& data =
                        ColumnVariant::deserialize_from_sparse_column(src_sparse_column_values, i);
                subcolumn->insert(data.first, data.second);
            } else {
                // Before inserting this path into sparse column check if we need to
                // insert subcolumns from sorted_src_subcolumn_for_sparse_column before.
                while (sorted_src_subcolumn_for_sparse_column_idx <
                               sorted_src_subcolumn_for_sparse_column_size &&
                       sorted_src_subcolumn_for_sparse_column
                                       [sorted_src_subcolumn_for_sparse_column_idx]
                                               .first < src_sparse_path) {
                    auto& [src_path, src_subcolumn] = sorted_src_subcolumn_for_sparse_column
                            [sorted_src_subcolumn_for_sparse_column_idx++];
                    src_subcolumn.serialize_to_sparse_column(sparse_column_path, src_path,
                                                             sparse_column_values, row);
                }

                /// Insert path and value from src sparse column to our sparse column.
                sparse_column_path->insert_from(*src_sparse_column_path, i);
                sparse_column_values->insert_from(*src_sparse_column_values, i);
            }
        }

        // Insert remaining dynamic paths from src_dynamic_paths_for_sparse_data.
        while (sorted_src_subcolumn_for_sparse_column_idx <
               sorted_src_subcolumn_for_sparse_column_size) {
            auto& [src_path, src_subcolumn] = sorted_src_subcolumn_for_sparse_column
                    [sorted_src_subcolumn_for_sparse_column_idx++];
            src_subcolumn.serialize_to_sparse_column(sparse_column_path, src_path,
                                                     sparse_column_values, row);
        }

        // All the sparse columns in this row are null.
        sparse_column_offsets.push_back(sparse_column_path->size());

        // Insert default values in all remaining dense columns.
        for (const auto& entry : subcolumns) {
            if (entry->data.size() == current_size) {
                entry->data.insert_default();
            }
        }
    }
}

MutableColumnPtr ColumnVariant::permute(const Permutation& perm, size_t limit) const {
    if (limit == 0) {
        limit = num_rows;
    } else {
        limit = std::min(num_rows, limit);
    }

    if (perm.size() < limit) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Size of permutation ({}) is less than required ({})", perm.size(),
                               limit);
        __builtin_unreachable();
    }

    if (limit == 0) {
        return ColumnVariant::create(_max_subcolumns_count);
    }

    return apply_for_columns([&](const ColumnPtr column) { return column->permute(perm, limit); });
}

void ColumnVariant::pop_back(size_t length) {
    for (auto& entry : subcolumns) {
        entry->data.pop_back(length);
    }
    serialized_sparse_column->pop_back(length);
    num_rows -= length;
    ENABLE_CHECK_CONSISTENCY(this);
}

const ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key) const {
    const auto* node = subcolumns.find_leaf(key);
    if (node == nullptr) {
        VLOG_DEBUG << "There is no subcolumn " << key.get_path();
        return nullptr;
    }
    return &node->data;
}

size_t ColumnVariant::Subcolumn::serialize_text_json(size_t n, BufferWritable& output,
                                                     DataTypeSerDe::FormatOptions opt) const {
    if (least_common_type.get_base_type_id() == PrimitiveType::INVALID_TYPE) {
        output.write(DataTypeSerDe::NULL_IN_COMPLEX_TYPE.data(),
                     DataTypeSerDe::NULL_IN_COMPLEX_TYPE.size());
        return DataTypeSerDe::NULL_IN_COMPLEX_TYPE.size();
    }

    size_t ind = n;
    if (ind < num_of_defaults_in_prefix) {
        output.write(DataTypeSerDe::NULL_IN_COMPLEX_TYPE.data(),
                     DataTypeSerDe::NULL_IN_COMPLEX_TYPE.size());
        return DataTypeSerDe::NULL_IN_COMPLEX_TYPE.size();
    }

    ind -= num_of_defaults_in_prefix;
    for (size_t i = 0; i < data.size(); ++i) {
        const auto& part = data[i];
        const auto& part_type_serde = data_serdes[i];

        if (ind < part->size()) {
            return part_type_serde->serialize_one_cell_to_json(*part, ind, output, opt);
        }

        ind -= part->size();
    }
    throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                           "Index ({}) for serializing JSON is out of range", n);
}

const ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn_with_cache(const PathInData& key,
                                                                        size_t key_index) const {
    // Optimization by caching the order of fields (which is almost always the same)
    // and a quick check to match the next expected field, instead of searching the hash table.
    if (_prev_positions.size() > key_index && _prev_positions[key_index].second != nullptr &&
        key == _prev_positions[key_index].first) {
        return _prev_positions[key_index].second;
    }
    const auto* subcolumn = get_subcolumn(key);
    if (key_index >= _prev_positions.size()) {
        _prev_positions.resize(key_index + 1);
    }
    if (subcolumn != nullptr) {
        _prev_positions[key_index] = std::make_pair(key, subcolumn);
    }
    return subcolumn;
}

ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key, size_t key_index) {
    return const_cast<ColumnVariant::Subcolumn*>(get_subcolumn_with_cache(key, key_index));
}

const ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key,
                                                             size_t key_index) const {
    return get_subcolumn_with_cache(key, key_index);
}

ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key) {
    const auto* node = subcolumns.find_leaf(key);
    if (node == nullptr) {
        VLOG_DEBUG << "There is no subcolumn " << key.get_path();
        return nullptr;
    }
    return &const_cast<Subcolumns::Node*>(node)->data;
}

bool ColumnVariant::has_subcolumn(const PathInData& key) const {
    return subcolumns.find_leaf(key) != nullptr;
}

bool ColumnVariant::add_sub_column(const PathInData& key, MutableColumnPtr&& subcolumn,
                                   DataTypePtr type) {
    size_t new_size = subcolumn->size();
    if (key.empty() && subcolumns.empty()) {
        // create root
        subcolumns.create_root(Subcolumn(std::move(subcolumn), type, is_nullable, true));
        num_rows = new_size;
        return true;
    }
    if (key.empty() && (!subcolumns.get_root()->is_scalar() || is_null_root() ||
                        subcolumns.get_root()->data.get_least_common_type()->get_primitive_type() ==
                                INVALID_TYPE)) {
        bool root_it_scalar = subcolumns.get_root()->is_scalar();
        // update root to scalar
        subcolumns.get_mutable_root()->modify_to_scalar(
                Subcolumn(std::move(subcolumn), type, is_nullable, true));
        if (!root_it_scalar) {
            subcolumns.add_leaf(subcolumns.get_root_ptr());
        }
        if (num_rows == 0) {
            num_rows = new_size;
        }
        return true;
    }
    bool inserted = subcolumns.add(key, Subcolumn(std::move(subcolumn), type, is_nullable));
    if (!inserted) {
        VLOG_DEBUG << "Duplicated sub column " << key.get_path();
        return false;
    }
    if (num_rows == 0) {
        num_rows = new_size;
    } else if (new_size != num_rows) {
        VLOG_DEBUG << "Size of subcolumn is in consistent with column";
        return false;
    }
    if (key.get_is_typed()) {
        typed_path_count++;
    }
    if (key.has_nested_part()) {
        nested_path_count++;
    }
    return true;
}

bool ColumnVariant::add_sub_column(const PathInData& key, size_t new_size) {
    if (key.empty() && subcolumns.empty()) {
        // create root
        subcolumns.create_root(Subcolumn(new_size, is_nullable, true));
        num_rows = new_size;
        return true;
    }
    if (key.empty() && (!subcolumns.get_root()->is_scalar())) {
        // update none scalar root column to scalar node
        subcolumns.get_mutable_root()->modify_to_scalar(Subcolumn(new_size, is_nullable, true));
        subcolumns.add_leaf(subcolumns.get_root_ptr());
        if (num_rows == 0) {
            num_rows = new_size;
        }
        return true;
    }
    bool inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));
    if (!inserted) {
        VLOG_DEBUG << "Duplicated sub column " << key.get_path();
        return false;
    }
    if (num_rows == 0) {
        num_rows = new_size;
    } else if (new_size != num_rows) {
        VLOG_DEBUG << "Size of subcolumn is in consistent with column";
        return false;
    }
    if (key.get_is_typed()) {
        typed_path_count++;
    }
    if (key.has_nested_part()) {
        nested_path_count++;
    }
    return true;
}

bool ColumnVariant::is_finalized() const {
    return std::all_of(subcolumns.begin(), subcolumns.end(),
                       [](const auto& entry) { return entry->data.is_finalized(); });
}

void ColumnVariant::Subcolumn::wrapp_array_nullable() {
    // Wrap array with nullable, treat empty array as null to elimate conflict at present
    auto& result_column = get_finalized_column_ptr();
    if (is_column<vectorized::ColumnArray>(result_column.get()) && !result_column->is_nullable()) {
        auto new_null_map = ColumnUInt8::create();
        new_null_map->reserve(result_column->size());
        auto& null_map_data = new_null_map->get_data();
        const auto* array = static_cast<const ColumnArray*>(result_column.get());
        for (size_t i = 0; i < array->size(); ++i) {
            null_map_data.push_back(array->is_default_at(i));
        }
        result_column = ColumnNullable::create(std::move(result_column), std::move(new_null_map));
        data_types[0] = make_nullable(data_types[0]);
        least_common_type = LeastCommonType {data_types[0], is_root};
    }
}

void ColumnVariant::serialize_one_row_to_string(int64_t row, std::string* output) const {
    auto tmp_col = ColumnString::create();
    VectorBufferWriter write_buffer(*tmp_col.get());
    if (is_scalar_variant()) {
        subcolumns.get_root()->data.serialize_text_json(row, write_buffer);
    } else {
        // TODO preallocate memory
        serialize_one_row_to_json_format(row, write_buffer, nullptr);
    }
    write_buffer.commit();
    auto str_ref = tmp_col->get_data_at(0);
    *output = std::string(str_ref.data, str_ref.size);
}

void ColumnVariant::serialize_one_row_to_string(int64_t row, BufferWritable& output) const {
    if (is_scalar_variant()) {
        subcolumns.get_root()->data.serialize_text_json(row, output);
        return;
    }
    serialize_one_row_to_json_format(row, output, nullptr);
}

/// Struct that represents elements of the JSON path.
/// "a.b.c" -> ["a", "b", "c"]
struct PathElements {
    explicit PathElements(const String& path) {
        const char* start = path.data();
        const char* end = start + path.size();
        const char* pos = start;
        const char* last_dot_pos = pos - 1;
        for (pos = start; pos != end; ++pos) {
            if (*pos == '.') {
                elements.emplace_back(last_dot_pos + 1, size_t(pos - last_dot_pos - 1));
                last_dot_pos = pos;
            }
        }

        elements.emplace_back(last_dot_pos + 1, size_t(pos - last_dot_pos - 1));
    }

    size_t size() const { return elements.size(); }

    std::vector<std::string_view> elements;
};

/// Struct that represents a prefix of a JSON path. Used during output of the JSON object.
struct Prefix {
    /// Shrink current prefix to the common prefix of current prefix and specified path.
    /// For example, if current prefix is a.b.c.d and path is a.b.e, then shrink the prefix to a.b.
    void shrink_to_common_prefix(const PathElements& path_elements) {
        /// Don't include last element in path_elements in the prefix.
        size_t i = 0;
        while (i != elements.size() && i != (path_elements.elements.size() - 1) &&
               elements[i].first == path_elements.elements[i]) {
            ++i;
        }
        elements.resize(i);
    }

    /// Check is_first flag in current object.
    bool is_first_in_current_object() const {
        if (elements.empty()) {
            return root_is_first_flag;
        }
        return elements.back().second;
    }

    /// Set flag is_first = false in current object.
    void set_not_first_in_current_object() {
        if (elements.empty()) {
            root_is_first_flag = false;
        } else {
            elements.back().second = false;
        }
    }

    size_t size() const { return elements.size(); }

    /// Elements of the prefix: (path element, is_first flag in this prefix).
    /// is_first flag indicates if we already serialized some key in the object with such prefix.
    std::vector<std::pair<std::string_view, bool>> elements;
    bool root_is_first_flag = true;
};

// skip empty nested json:
// 1. nested array with only nulls, eg. [null. null],todo: think a better way to deal distinguish array null value and real null value.
// 2. type is nothing
bool ColumnVariant::Subcolumn::is_empty_nested(size_t row) const {
    PrimitiveType base_type_id = least_common_type.get_base_type_id();
    const DataTypePtr& type = least_common_type.get();
    // check if it is empty nested json array, then skip
    if (base_type_id == PrimitiveType::TYPE_VARIANT) {
        DCHECK(type->equals(*ColumnVariant::NESTED_TYPE));
        FieldWithDataType field;
        get(row, field);
        if (field.field.get_type() == PrimitiveType::TYPE_ARRAY) {
            const auto& array = field.field.get<Array>();
            bool only_nulls_inside = true;
            for (const auto& elem : array) {
                if (elem.get_type() != PrimitiveType::TYPE_NULL) {
                    only_nulls_inside = false;
                    break;
                }
            }
            // if only nulls then skip
            return only_nulls_inside;
        }
    }
    // skip nothing type
    if (base_type_id == PrimitiveType::INVALID_TYPE) {
        return true;
    }
    return false;
}

bool ColumnVariant::is_visible_root_value(size_t nrow) const {
    if (is_null_root()) {
        return false;
    }
    const auto* root = subcolumns.get_root();
    if (root->data.is_null_at(nrow)) {
        return false;
    }
    if (root->data.least_common_type.get_base_type_id() == PrimitiveType::TYPE_VARIANT) {
        // nested field
        return !root->data.is_empty_nested(nrow);
    }
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_root) {
            continue; // Skip the root column
        }

        // If any non-root subcolumn is NOT null, set serialize_root to false and exit early
        if (!subcolumn->data.is_null_at(nrow)) {
            return false;
        }
    }
    size_t ind = nrow - root->data.num_of_defaults_in_prefix;
    // null value as empty json, todo: think a better way to disinguish empty json and null json.
    for (const auto& part : root->data.data) {
        if (ind < part->size()) {
            return !part->get_data_at(ind).empty();
        }
        ind -= part->size();
    }

    throw doris::Exception(ErrorCode::OUT_OF_BOUND, "Index ({}) for getting field is out of range",
                           nrow);
}

void ColumnVariant::serialize_one_row_to_json_format(int64_t row_num, BufferWritable& output,
                                                     bool* is_null) const {
    // root is not eighther null or empty, we should only process root value
    if (is_visible_root_value(row_num)) {
        subcolumns.get_root()->data.serialize_text_json(row_num, output);
        return;
    }
    const auto& column_map = assert_cast<const ColumnMap&>(*serialized_sparse_column);
    const auto& sparse_data_offsets = column_map.get_offsets();
    const auto [sparse_data_paths, sparse_data_values] = get_sparse_data_paths_and_values();
    size_t sparse_data_offset = sparse_data_offsets[static_cast<ssize_t>(row_num) - 1];
    size_t sparse_data_end = sparse_data_offsets[static_cast<ssize_t>(row_num)];

    // We need to convert the set of paths in this row to a JSON object.
    // To do it, we first collect all the paths from current row, then we sort them
    // and construct the resulting JSON object by iterating over sorted list of paths.
    // For example:
    // b.c, a.b, a.a, b.e, g, h.u.t -> a.a, a.b, b.c, b.e, g, h.u.t -> {"a" : {"a" : ..., "b" : ...}, "b" : {"c" : ..., "e" : ...}, "g" : ..., "h" : {"u" : {"t" : ...}}}.
    std::vector<String> sorted_paths;
    std::unordered_map<std::string, Subcolumn> subcolumn_path_map;
    sorted_paths.reserve(get_subcolumns().size() + (sparse_data_end - sparse_data_offset));
    for (const auto& subcolumn : get_subcolumns()) {
        // Skip root value, we have already processed it
        if (subcolumn->data.is_root) {
            continue;
        }

        // skip empty nested value
        if (subcolumn->data.is_empty_nested(row_num)) {
            continue;
        }
        /// We consider null value and absence of the path in a row as equivalent cases, because we cannot actually distinguish them.
        /// So, we don't output null values at all.
        if (!subcolumn->data.is_null_at(row_num)) {
            sorted_paths.emplace_back(subcolumn->path.get_path());
        }
        subcolumn_path_map.emplace(subcolumn->path.get_path(), subcolumn->data);
    }
    for (size_t i = sparse_data_offset; i != sparse_data_end; ++i) {
        auto path = sparse_data_paths->get_data_at(i).to_string();
        sorted_paths.emplace_back(path);
    }

    std::sort(sorted_paths.begin(), sorted_paths.end());

    output.write_char('{');
    size_t index_in_sparse_data_values = sparse_data_offset;
    // current_prefix represents the path of the object we are currently serializing keys in.
    Prefix current_prefix;
    for (const auto& path : sorted_paths) {
        PathElements path_elements(path);
        // Change prefix to common prefix between current prefix and current path.
        // If prefix changed (it can only decrease), close all finished objects.
        // For example:
        // Current prefix: a.b.c.d
        // Current path: a.b.e.f
        // It means now we have : {..., "a" : {"b" : {"c" : {"d" : ...
        // Common prefix will be a.b, so it means we should close objects a.b.c.d and a.b.c: {..., "a" : {"b" : {"c" : {"d" : ...}}
        // and continue serializing keys in object a.b
        size_t prev_prefix_size = current_prefix.size();
        current_prefix.shrink_to_common_prefix(path_elements);
        size_t prefix_size = current_prefix.size();
        if (prefix_size != prev_prefix_size) {
            size_t objects_to_close = prev_prefix_size - prefix_size;
            for (size_t i = 0; i != objects_to_close; ++i) {
                output.write_char('}');
            }
        }

        // Now we are inside object that has common prefix with current path.
        // We should go inside all objects in current path.
        // From the example above we should open object a.b.e:
        //  {..., "a" : {"b" : {"c" : {"d" : ...}}, "e" : {
        if (prefix_size + 1 < path_elements.size()) {
            for (size_t i = prefix_size; i != path_elements.size() - 1; ++i) {
                /// Write comma before the key if it's not the first key in this prefix.
                if (!current_prefix.is_first_in_current_object()) {
                    output.write_char(',');
                } else {
                    current_prefix.set_not_first_in_current_object();
                }

                output.write_json_string(path_elements.elements[i]);
                output.write_c_string(":{");

                // Update current prefix.
                current_prefix.elements.emplace_back(path_elements.elements[i], true);
            }
        }

        // Write comma before the key if it's not the first key in this prefix.
        if (!current_prefix.is_first_in_current_object()) {
            output.write_char(',');
        } else {
            current_prefix.set_not_first_in_current_object();
        }

        output.write_json_string(path_elements.elements.back());
        output.write_c_string(":");

        // Serialize value of current path.
        if (auto subcolumn_it = subcolumn_path_map.find(path);
            subcolumn_it != subcolumn_path_map.end()) {
            DataTypeSerDe::FormatOptions options;
            options.escape_char = '\\';
            subcolumn_it->second.serialize_text_json(row_num, output, options);
        } else {
            // To serialize value stored in shared data we should first deserialize it from binary format.
            Subcolumn tmp_subcolumn(0, true);
            const auto& data = ColumnVariant::deserialize_from_sparse_column(
                    sparse_data_values, index_in_sparse_data_values++);
            tmp_subcolumn.insert(data.first, data.second);
            DataTypeSerDe::FormatOptions options;
            options.escape_char = '\\';
            tmp_subcolumn.serialize_text_json(0, output, options);
        }
    }

    // Close all remaining open objects.
    for (size_t i = 0; i != current_prefix.elements.size(); ++i) {
        output.write_char('}');
    }
    output.write_char('}');
}

size_t ColumnVariant::Subcolumn::get_non_null_value_size() const {
    size_t res = 0;
    for (const auto& part : data) {
        const auto& null_data = assert_cast<const ColumnNullable&>(*part).get_null_map_data();
        res += simd::count_zero_num((int8_t*)null_data.data(), null_data.size());
    }
    return res;
}

Status ColumnVariant::serialize_sparse_columns(
        std::map<std::string_view, Subcolumn>&& remaing_subcolumns) {
    CHECK(is_finalized());

    clear_sparse_column();

    if (remaing_subcolumns.empty()) {
        serialized_sparse_column->resize(num_rows);
        return Status::OK();
    }
    serialized_sparse_column->reserve(num_rows);
    auto [sparse_column_keys, sparse_column_values] = get_sparse_data_paths_and_values();
    auto& sparse_column_offsets = serialized_sparse_column_offsets();

    // Fill the column map for each row
    for (size_t i = 0; i < num_rows; ++i) {
        for (auto& [path, subcolumn] : remaing_subcolumns) {
            subcolumn.serialize_to_sparse_column(sparse_column_keys, path, sparse_column_values, i);
        }

        // TODO add dcheck
        sparse_column_offsets.push_back(sparse_column_keys->size());
    }
    CHECK_EQ(serialized_sparse_column->size(), num_rows);
    return Status::OK();
}

void ColumnVariant::unnest(Subcolumns::NodePtr& entry, Subcolumns& res_subcolumns) const {
    entry->data.finalize();
    auto nested_column = entry->data.get_finalized_column_ptr()->assume_mutable();
    auto* nested_column_nullable = assert_cast<ColumnNullable*>(nested_column.get());
    auto* nested_column_array =
            assert_cast<ColumnArray*>(nested_column_nullable->get_nested_column_ptr().get());
    auto& offset = nested_column_array->get_offsets_ptr();

    auto* nested_object_nullable = assert_cast<ColumnNullable*>(
            nested_column_array->get_data_ptr()->assume_mutable().get());
    auto& nested_object_column =
            assert_cast<ColumnVariant&>(nested_object_nullable->get_nested_column());
    PathInData nested_path = entry->path;
    for (auto& nested_entry : nested_object_column.subcolumns) {
        if (nested_entry->data.least_common_type.get_base_type_id() ==
            PrimitiveType::INVALID_TYPE) {
            continue;
        }
        nested_entry->data.finalize();
        PathInDataBuilder path_builder;
        // format nested path
        path_builder.append(nested_path.get_parts(), false);
        path_builder.append(nested_entry->path.get_parts(), true);
        auto subnested_column = ColumnArray::create(
                ColumnNullable::create(nested_entry->data.get_finalized_column_ptr(),
                                       nested_object_nullable->get_null_map_column_ptr()),
                offset);
        auto nullable_subnested_column = ColumnNullable::create(
                std::move(subnested_column), nested_column_nullable->get_null_map_column_ptr());
        auto type = make_nullable(
                std::make_shared<DataTypeArray>(nested_entry->data.least_common_type.get()));
        Subcolumn subcolumn(nullable_subnested_column->assume_mutable(), type, is_nullable);
        res_subcolumns.add(path_builder.build(), subcolumn);
    }
}

void ColumnVariant::clear_sparse_column() {
#ifndef NDEBUG
    auto& sparse_column_offsets = serialized_sparse_column_offsets();
    if (sparse_column_offsets[num_rows - 1] != 0) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "sprase column has nonnull value");
    }
#endif

    serialized_sparse_column->clear();
}

Status ColumnVariant::convert_typed_path_to_storage_type(
        const std::unordered_map<std::string, TabletSchema::SubColumnInfo>& typed_paths) {
    for (auto&& entry : subcolumns) {
        if (auto it = typed_paths.find(entry->path.get_path()); it != typed_paths.end()) {
            CHECK(entry->data.is_finalized());
            vectorized::DataTypePtr storage_type =
                    vectorized::DataTypeFactory::instance().create_data_type(it->second.column);
            vectorized::DataTypePtr finalized_type = entry->data.get_least_common_type();
            auto current_column = entry->data.get_finalized_column_ptr()->get_ptr();
            if (!storage_type->equals(*finalized_type)) {
                RETURN_IF_ERROR(vectorized::schema_util::cast_column(
                        {current_column, finalized_type, ""}, storage_type, &current_column));
            }
            VLOG_DEBUG << "convert " << entry->path.get_path() << " from type"
                       << entry->data.get_least_common_type()->get_name() << " to "
                       << storage_type->get_name();
            entry->data.data[0] = current_column;
            entry->data.data_types[0] = storage_type;
            entry->data.data_serdes[0] = Subcolumn::generate_data_serdes(storage_type, false);
            entry->data.least_common_type = Subcolumn::LeastCommonType {storage_type, false};
        }
    }
    return Status::OK();
}

Status ColumnVariant::pick_subcolumns_to_sparse_column(
        const std::unordered_map<std::string, TabletSchema::SubColumnInfo>& typed_paths,
        bool variant_enable_typed_paths_to_sparse) {
    DCHECK(_max_subcolumns_count >= 0) << "max subcolumns count is: " << _max_subcolumns_count;

    // no need to pick subcolumns to sparse column, all subcolumns will be picked
    if (_max_subcolumns_count == 0) {
        return Status::OK();
    }

    // root column must be exsit in subcolumns
    // nested path count is the count of nested columns
    int64_t current_subcolumns_count = subcolumns.size() - 1 - nested_path_count;

    //  1000 count
    // b : 1500 typed path + 700 subcolumns -> 1200 count ()
    if (!variant_enable_typed_paths_to_sparse) {
        current_subcolumns_count -= typed_paths.size();
    }

    bool need_pick_subcolumn_to_sparse_column = current_subcolumns_count > _max_subcolumns_count;

    if (!need_pick_subcolumn_to_sparse_column) {
        return Status::OK();
    }
    Subcolumns new_subcolumns;
    if (auto* root = subcolumns.get_mutable_root(); root == nullptr) {
        CHECK(false);
    } else {
        new_subcolumns.create_root(root->data);
    }
    // picked to sparse columns
    std::set<std::string_view> selected_path;
    // pick subcolumns sort by size of none null values
    std::unordered_map<std::string_view, size_t> none_null_value_sizes;
    // 1. get the none null value sizes
    for (auto&& entry : subcolumns) {
        // root column is already in the subcolumns
        if (entry->data.is_root) {
            continue;
        }
        if ((!variant_enable_typed_paths_to_sparse &&
             typed_paths.find(entry->path.get_path()) != typed_paths.end()) ||
            entry->path.has_nested_part()) {
            VLOG_DEBUG << "pick " << entry->path.get_path() << " as typed column";
            new_subcolumns.add(entry->path, entry->data);
            continue;
        }
        size_t size = entry->data.get_non_null_value_size();
        if (size == 0) {
            continue;
        }
        none_null_value_sizes[entry->path.get_path()] = size;
    }
    // 2. sort by the size
    std::vector<std::pair<std::string_view, size_t>> sorted_by_size(none_null_value_sizes.begin(),
                                                                    none_null_value_sizes.end());
    std::sort(sorted_by_size.begin(), sorted_by_size.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });
    // 3. pick config::variant_max_subcolumns_count selected subcolumns
    for (size_t i = 0; i < std::min(size_t(_max_subcolumns_count), sorted_by_size.size()); ++i) {
        selected_path.insert(sorted_by_size[i].first);
    }
    std::map<std::string_view, Subcolumn> remaing_subcolumns;
    // add selected subcolumns to new_subcolumns, otherwise add to remaining_subcolumns
    for (auto&& entry : subcolumns) {
        if (entry->data.is_root) {
            continue;
        }
        if (selected_path.find(entry->path.get_path()) != selected_path.end()) {
            new_subcolumns.add(entry->path, entry->data);
            VLOG_DEBUG << "pick " << entry->path.get_path() << " as sub column";
        } else if (none_null_value_sizes.find(entry->path.get_path()) !=
                   none_null_value_sizes.end()) {
            VLOG_DEBUG << "pick " << entry->path.get_path() << " as sparse column";
            CHECK(!entry->path.has_nested_part());
            remaing_subcolumns.emplace(entry->path.get_path(), entry->data);
        }
    }
    ENABLE_CHECK_CONSISTENCY(this);
    RETURN_IF_ERROR(serialize_sparse_columns(std::move(remaing_subcolumns)));
    std::swap(subcolumns, new_subcolumns);
    ENABLE_CHECK_CONSISTENCY(this);
    return Status::OK();
}

void ColumnVariant::finalize(FinalizeMode mode) {
    if (is_finalized() && mode == FinalizeMode::READ_MODE) {
        _prev_positions.clear();
        ENABLE_CHECK_CONSISTENCY(this);
        return;
    }
    Subcolumns new_subcolumns;

    if (auto* root = subcolumns.get_mutable_root(); root == nullptr) {
        CHECK(false);
    } else {
        root->data.finalize(mode);
        new_subcolumns.create_root(root->data);
    }

    // finalize all subcolumns
    for (auto&& entry : subcolumns) {
        if (entry->data.is_root) {
            continue;
        }
        const auto& least_common_type = entry->data.get_least_common_type();

        /// Do not add subcolumns, which consists only from NULLs
        if (get_base_type_of_array(least_common_type)->get_primitive_type() ==
            PrimitiveType::INVALID_TYPE) {
            continue;
        }

        // unnest all nested columns, add them to new_subcolumns
        if (mode == FinalizeMode::WRITE_MODE && least_common_type->equals(*NESTED_TYPE)) {
            // reset counter
            unnest(entry, new_subcolumns);
            continue;
        }

        entry->data.finalize(mode);
        entry->data.wrapp_array_nullable();

        new_subcolumns.add(entry->path, entry->data);
    }
    // after unnest need to recalculate nested_path_count in new_subcolumns
    nested_path_count =
            std::count_if(new_subcolumns.begin(), new_subcolumns.end(),
                          [](const auto& entry) { return entry->path.has_nested_part(); });
    std::swap(subcolumns, new_subcolumns);
    _prev_positions.clear();
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::finalize() {
    static_cast<void>(finalize(FinalizeMode::READ_MODE));
}

void ColumnVariant::ensure_root_node_type(const DataTypePtr& expected_root_type) {
    auto& root = subcolumns.get_mutable_root()->data;
    if (!root.get_least_common_type()->equals(*expected_root_type)) {
        // make sure the root type is alawys as expected
        ColumnPtr casted_column;
        static_cast<void>(
                schema_util::cast_column(ColumnWithTypeAndName {root.get_finalized_column_ptr(),
                                                                root.get_least_common_type(), ""},
                                         expected_root_type, &casted_column));
        root.data[0] = casted_column;
        root.data_types[0] = expected_root_type;
        root.least_common_type = Subcolumn::LeastCommonType {expected_root_type, true};
        root.num_rows = casted_column->size();
    }
}

bool ColumnVariant::empty() const {
    return subcolumns.empty() || subcolumns.begin()->get()->path.get_path() == COLUMN_NAME_DUMMY;
}

ColumnPtr ColumnVariant::filter(const Filter& filter, ssize_t count) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnVariant&>(*finalized);
        return finalized_object.filter(filter, count);
    }
    if (num_rows == 0) {
        auto res = ColumnVariant::create(_max_subcolumns_count, count_bytes_in_filter(filter));
        ENABLE_CHECK_CONSISTENCY(res.get());
        return res;
    }
    auto new_root = get_root()->filter(filter, count)->assume_mutable();
    auto new_column =
            ColumnVariant::create(_max_subcolumns_count, get_root_type(), std::move(new_root));
    for (const auto& entry : subcolumns) {
        if (entry->data.is_root) {
            continue;
        }
        auto subcolumn = entry->data.get_finalized_column().filter(filter, -1);
        new_column->add_sub_column(entry->path, subcolumn->assume_mutable(),
                                   entry->data.get_least_common_type());
    }
    new_column->serialized_sparse_column = serialized_sparse_column->filter(filter, count);
    ENABLE_CHECK_CONSISTENCY(new_column.get());
    return new_column;
}

size_t ColumnVariant::filter(const Filter& filter) {
    if (!is_finalized()) {
        finalize();
    }
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        clear();
    } else {
        for (auto& subcolumn : subcolumns) {
            subcolumn->data.num_rows = count;
        }
        for_each_subcolumn([&](auto& part) {
            if (part->size() != count) {
                if (part->is_exclusive()) {
                    const auto result_size = part->filter(filter);
                    if (result_size != count) {
                        throw Exception(ErrorCode::INTERNAL_ERROR,
                                        "result_size not euqal with filter_size, result_size={}, "
                                        "filter_size={}",
                                        result_size, count);
                    }
                    CHECK_EQ(result_size, count);
                } else {
                    part = part->filter(filter, count);
                }
            }
        });
    }
    num_rows = count;
    ENABLE_CHECK_CONSISTENCY(this);
    return count;
}

void ColumnVariant::clear() {
    Subcolumns empty;
    // we must keep root column exist
    empty.create_root(Subcolumn(0, is_nullable, true));
    std::swap(empty, subcolumns);
    serialized_sparse_column->clear();
    num_rows = 0;
    _prev_positions.clear();
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::clear_subcolumns_data() {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            DCHECK_EQ(part->use_count(), 1);
            (*std::move(part)).clear();
        }
        entry->data.num_of_defaults_in_prefix = 0;
        entry->data.num_rows = 0;
    }
    serialized_sparse_column->clear();
    num_rows = 0;
    ENABLE_CHECK_CONSISTENCY(this);
}

void ColumnVariant::create_root(const DataTypePtr& type, MutableColumnPtr&& column) {
    if (num_rows == 0) {
        num_rows = column->size();
    }
    add_sub_column({}, std::move(column), type);
    if (serialized_sparse_column->empty()) {
        serialized_sparse_column->resize(num_rows);
    }
    ENABLE_CHECK_CONSISTENCY(this);
}

const DataTypePtr& ColumnVariant::get_most_common_type() {
    static auto type = make_nullable(std::make_shared<MostCommonType>());
    return type;
}

bool ColumnVariant::is_null_root() const {
    const auto* root = subcolumns.get_root();
    if (root == nullptr) {
        return true;
    }
    if (root->data.num_of_defaults_in_prefix == 0 &&
        (root->data.data.empty() ||
         root->data.get_least_common_type()->get_primitive_type() == PrimitiveType::INVALID_TYPE)) {
        return true;
    }
    return false;
}

// num_rows == 0, so we need to use NO_SANITIZE_UNDEFINED
bool NO_SANITIZE_UNDEFINED ColumnVariant::is_scalar_variant() const {
    const auto& sparse_offsets = serialized_sparse_column_offsets().data();
    // Only root itself is scalar, and no sparse data
    return !is_null_root() && subcolumns.get_leaves().size() == 1 &&
           subcolumns.get_root()->is_scalar() &&
           sparse_offsets[num_rows - 1] == 0; // no sparse data
}

const DataTypePtr ColumnVariant::NESTED_TYPE = std::make_shared<vectorized::DataTypeNullable>(
        std::make_shared<vectorized::DataTypeArray>(std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeVariant>(0))));

const DataTypePtr ColumnVariant::NESTED_TYPE_AS_ARRAY_OF_JSONB =
        std::make_shared<vectorized::DataTypeArray>(std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeJsonb>()));

DataTypePtr ColumnVariant::get_root_type() const {
    return subcolumns.get_root()->data.get_least_common_type();
}

void ColumnVariant::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                        const uint32_t* indices_end) {
    // optimize when src and this column are scalar variant, since try_insert is inefficiency
    const auto* src_v = check_and_get_column<ColumnVariant>(src);

    bool src_can_do_quick_insert =
            src_v != nullptr && src_v->is_scalar_variant() && src_v->is_finalized();
    // num_rows == 0 means this column is empty, we not need to check it type
    if (num_rows == 0 && src_can_do_quick_insert) {
        // add a new root column, and insert from src root column
        clear();
        add_sub_column({}, src_v->get_root()->clone_empty(), src_v->get_root_type());
        get_subcolumns().get_mutable_root()->data.num_rows = indices_end - indices_begin;
        get_root()->insert_indices_from(*src_v->get_root(), indices_begin, indices_end);
        serialized_sparse_column->insert_many_defaults(indices_end - indices_begin);
        num_rows += indices_end - indices_begin;
    } else if (src_can_do_quick_insert && is_scalar_variant() &&
               src_v->get_root_type()->equals(*get_root_type())) {
        get_root()->insert_indices_from(*src_v->get_root(), indices_begin, indices_end);
        serialized_sparse_column->insert_many_defaults(indices_end - indices_begin);
        get_subcolumns().get_mutable_root()->data.num_rows += indices_end - indices_begin;
        num_rows += indices_end - indices_begin;
    } else {
        for (const auto* x = indices_begin; x != indices_end; ++x) {
            try_insert(src[*x]);
        }
    }
    finalize();
}

// void ColumnVariant::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
//                                        const uint32_t* indices_end) {
//     for (const auto* x = indices_begin; x != indices_end; ++x) {
//         ColumnVariant::insert_from(src, *x);
//     }
//     finalize();
//     ENABLE_CHECK_CONSISTENCY(this);
// }

template <typename Func>
void ColumnVariant::for_each_imutable_column(Func&& callback) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnVariant&>(*finalized);
        finalized_object.for_each_imutable_column(callback);
        return;
    }
    for (const auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            callback(part);
        }
    }
    callback(serialized_sparse_column);
}

void ColumnVariant::update_hash_with_value(size_t n, SipHash& hash) const {
    for_each_imutable_column(
            [&](const ColumnPtr column) { return column->update_hash_with_value(n, hash); });
}

void ColumnVariant::update_hashes_with_value(uint64_t* __restrict hashes,
                                             const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_hashes_with_value(hashes, nullptr);
    });
}

void ColumnVariant::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                             const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_xxHash_with_value(start, end, hash, nullptr);
    });
}

void ColumnVariant::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type,
                                           uint32_t rows, uint32_t offset,
                                           const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_crcs_with_value(hash, type, rows, offset, nullptr);
    });
}

void ColumnVariant::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                          const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_crc_with_value(start, end, hash, nullptr);
    });
}

std::string ColumnVariant::debug_string() const {
    std::stringstream res;
    res << get_name() << "(num_row = " << num_rows;
    for (const auto& entry : subcolumns) {
        if (entry->data.is_finalized()) {
            res << "[column:" << entry->data.data[0]->dump_structure()
                << ",type:" << entry->data.data_types[0]->get_name()
                << ",path:" << entry->path.get_path() << "],";
        }
    }
    res << ")";
    return res.str();
}

Status ColumnVariant::sanitize() const {
    RETURN_IF_CATCH_EXCEPTION(check_consistency());
    std::unordered_set<std::string> subcolumn_set;
    // deduplicate subcolumns, example {"a.b" : 123, "a" : {"b" : 123}}
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_root) {
            continue;
        }
        if (subcolumn_set.contains(subcolumn->path.get_path())) {
            return Status::InvalidJsonPath("may contains duplicated entry : {}",
                                           subcolumn->path.get_path());
        }
        subcolumn_set.insert(subcolumn->path.get_path());
    }
    VLOG_DEBUG << "sanitized " << debug_string();
    return Status::OK();
}

ColumnVariant::Subcolumn ColumnVariant::Subcolumn::cut(size_t start, size_t length) const {
    Subcolumn new_subcolumn(0, is_nullable);
    new_subcolumn.insert_range_from(*this, start, length);
    return new_subcolumn;
}

const ColumnVariant::Subcolumns::Node* ColumnVariant::get_leaf_of_the_same_nested(
        const Subcolumns::NodePtr& entry) const {
    const auto* leaf = subcolumns.get_leaf_of_the_same_nested(
            entry->path,
            [&](const Subcolumns::Node& node) { return node.data.size() > entry->data.size(); });
    if (leaf && leaf->data.get_least_common_typeBase()->get_primitive_type() == INVALID_TYPE) {
        return nullptr;
    }
    return leaf;
}

bool ColumnVariant::try_insert_many_defaults_from_nested(const Subcolumns::NodePtr& entry) const {
    const auto* leaf = get_leaf_of_the_same_nested(entry);
    if (!leaf) {
        return false;
    }

    size_t old_size = entry->data.size();
    FieldInfo field_info = {
            .scalar_type_id = entry->data.least_common_type.get_base_type_id(),
            .num_dimensions = entry->data.get_dimensions(),
    };

    /// Cut the needed range from the found leaf
    /// and replace scalar values to the correct
    /// default values for given entry.
    auto new_subcolumn = leaf->data.cut(old_size, leaf->data.size() - old_size)
                                 .clone_with_default_values(field_info);

    entry->data.insert_range_from(new_subcolumn, 0, new_subcolumn.size());
    ENABLE_CHECK_CONSISTENCY(this);
    return true;
}

/// Visitor that keeps @num_dimensions_to_keep dimensions in arrays
/// and replaces all scalars or nested arrays to @replacement at that level.
class FieldVisitorReplaceScalars : public StaticVisitor<Field> {
public:
    FieldVisitorReplaceScalars(const Field& replacement_, size_t num_dimensions_to_keep_)
            : replacement(replacement_), num_dimensions_to_keep(num_dimensions_to_keep_) {}

    template <PrimitiveType T>
    Field apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) const {
        if constexpr (T == TYPE_ARRAY) {
            if (num_dimensions_to_keep == 0) {
                return replacement;
            }

            const size_t size = x.size();
            Array res(size);
            for (size_t i = 0; i < size; ++i) {
                res[i] = apply_visitor(
                        FieldVisitorReplaceScalars(replacement, num_dimensions_to_keep - 1), x[i]);
            }
            return Field::create_field<TYPE_ARRAY>(res);
        } else {
            return replacement;
        }
    }

private:
    const Field& replacement;
    size_t num_dimensions_to_keep;
};

bool ColumnVariant::try_insert_default_from_nested(const Subcolumns::NodePtr& entry) const {
    const auto* leaf = get_leaf_of_the_same_nested(entry);
    if (!leaf) {
        return false;
    }

    auto last_field = leaf->data.get_last_field();
    if (last_field.is_null()) {
        return false;
    }

    size_t leaf_num_dimensions = leaf->data.get_dimensions();
    size_t entry_num_dimensions = entry->data.get_dimensions();

    if (entry_num_dimensions > leaf_num_dimensions) {
        throw doris::Exception(
                ErrorCode::INVALID_ARGUMENT,
                "entry_num_dimensions > leaf_num_dimensions, entry_num_dimensions={}, "
                "leaf_num_dimensions={}",
                entry_num_dimensions, leaf_num_dimensions);
    }

    auto default_scalar = entry->data.get_least_common_type()->get_default();

    auto default_field = apply_visitor(
            FieldVisitorReplaceScalars(default_scalar, leaf_num_dimensions), last_field);
    entry->data.insert(std::move(default_field));
    return true;
}

size_t ColumnVariant::find_path_lower_bound_in_sparse_data(StringRef path,
                                                           const ColumnString& sparse_data_paths,
                                                           size_t start, size_t end) {
    // Simple random access iterator over values in ColumnString in specified range.
    class Iterator {
    public:
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-local-typedefs"
        using difference_type = std::ptrdiff_t;
        using value_type = StringRef;
        using iterator_category = std::random_access_iterator_tag;
        using pointer = StringRef*;
        using reference = StringRef&;
#pragma GCC diagnostic pop

        Iterator() = delete;
        Iterator(const ColumnString* data_, size_t index_) : data(data_), index(index_) {}
        Iterator(const Iterator& rhs) = default;
        Iterator& operator=(const Iterator& rhs) = default;
        inline Iterator& operator+=(difference_type rhs) {
            index += rhs;
            return *this;
        }
        inline StringRef operator*() const { return data->get_data_at(index); }

        inline Iterator& operator++() {
            ++index;
            return *this;
        }
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-member-function"
#endif
        inline Iterator& operator--() {
            --index;
            return *this;
        }
#ifdef __clang__
#pragma clang diagnostic pop
#endif
        inline difference_type operator-(const Iterator& rhs) const { return index - rhs.index; }

        const ColumnString* data;
        size_t index;
    };

    Iterator start_it(&sparse_data_paths, start);
    Iterator end_it(&sparse_data_paths, end);
    auto it = std::lower_bound(start_it, end_it, path);
    return it.index;
}

void ColumnVariant::fill_path_column_from_sparse_data(Subcolumn& subcolumn, NullMap* null_map,
                                                      StringRef path,
                                                      const ColumnPtr& sparse_data_column,
                                                      size_t start, size_t end) {
    const auto& sparse_data_map = assert_cast<const ColumnMap&>(*sparse_data_column);
    const auto& sparse_data_offsets = sparse_data_map.get_offsets();
    size_t first_offset = sparse_data_offsets[static_cast<ssize_t>(start) - 1];
    size_t last_offset = sparse_data_offsets[static_cast<ssize_t>(end) - 1];
    // Check if we have at least one row with data.
    if (first_offset == last_offset) {
        if (null_map) {
            null_map->resize_fill(end - start, 1);
        }
        subcolumn.insert_many_defaults(end - start);
        return;
    }

    const auto& sparse_data_paths = assert_cast<const ColumnString&>(sparse_data_map.get_keys());
    const auto& sparse_data_values = assert_cast<const ColumnString&>(sparse_data_map.get_values());
    for (size_t i = start; i != end; ++i) {
        size_t paths_start = sparse_data_offsets[static_cast<ssize_t>(i) - 1];
        size_t paths_end = sparse_data_offsets[static_cast<ssize_t>(i)];
        auto lower_bound_path_index = ColumnVariant::find_path_lower_bound_in_sparse_data(
                path, sparse_data_paths, paths_start, paths_end);
        bool is_null = false;
        if (lower_bound_path_index != paths_end &&
            sparse_data_paths.get_data_at(lower_bound_path_index) == path) {
            // auto value_data = sparse_data_values.get_data_at(lower_bound_path_index);
            // ReadBufferFromMemory buf(value_data.data, value_data.size);
            // dynamic_serialization->deserializeBinary(path_column, buf, getFormatSettings());
            const auto& data = ColumnVariant::deserialize_from_sparse_column(
                    &sparse_data_values, lower_bound_path_index);
            subcolumn.insert(data.first, data.second);
            is_null = false;
        } else {
            subcolumn.insert_default();
            is_null = true;
        }
        if (null_map) {
            null_map->push_back(is_null);
        }
    }
}

MutableColumnPtr ColumnVariant::clone() const {
    auto res = ColumnVariant::create(_max_subcolumns_count);
    Subcolumns new_subcolumns;
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = subcolumn->data;
        if (subcolumn->data.is_root) {
            new_subcolumns.create_root(std::move(new_subcolumn));
        } else if (!new_subcolumns.add(subcolumn->path, std::move(new_subcolumn))) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "add path {} is error in clone()",
                                   subcolumn->path.get_path());
        }
    }
    if (!new_subcolumns.get_root()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "root is nullptr in clone()");
    }
    res->subcolumns = std::move(new_subcolumns);
    auto&& column = serialized_sparse_column->get_ptr();
    auto sparse_column = std::move(*column).mutate();
    res->serialized_sparse_column = sparse_column->assume_mutable();
    res->set_num_rows(num_rows);
    ENABLE_CHECK_CONSISTENCY(res.get());
    return res;
}

#include "common/compile_check_end.h"

} // namespace doris::vectorized
