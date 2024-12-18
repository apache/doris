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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnObject.cpp
// and modified by Doris

#include "vec/columns/column_object.h"

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
#include "util/defer_op.h"
#include "util/simd/bits.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/convert_field_to_type.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_object.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/json/path_in_data.h"

#ifdef __AVX2__
#include "util/jsonb_parser_simd.h"
#else
#include "util/jsonb_parser.h"
#endif

namespace doris::vectorized {
namespace {

DataTypePtr create_array_of_type(TypeIndex type, size_t num_dimensions, bool is_nullable) {
    if (type == ColumnObject::MOST_COMMON_TYPE_ID) {
        // JSONB type MUST NOT wrapped in ARRAY column, it should be top level.
        // So we ignored num_dimensions.
        return is_nullable ? make_nullable(std::make_shared<ColumnObject::MostCommonType>())
                           : std::make_shared<ColumnObject::MostCommonType>();
    }
    DataTypePtr result = DataTypeFactory::instance().create_data_type(type, is_nullable);
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

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t> {
public:
    size_t operator()(const Array& x) const {
        const size_t size = x.size();
        size_t dimensions = 0;
        for (size_t i = 0; i < size; ++i) {
            size_t element_dimensions = apply_visitor(*this, x[i]);
            dimensions = std::max(dimensions, element_dimensions);
        }
        return 1 + dimensions;
    }
    template <typename T>
    size_t operator()(const T&) const {
        return 0;
    }
};

// Visitor that allows to get type of scalar field
// but exclude fields contain complex field.This is a faster version
// for FieldVisitorToScalarType which does not support complex field.
class SimpleFieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    size_t operator()(const Array& x) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Array type is not supported");
    }
    size_t operator()(const UInt64& x) {
        if (x <= std::numeric_limits<Int8>::max()) {
            type = TypeIndex::Int8;
        } else if (x <= std::numeric_limits<Int16>::max()) {
            type = TypeIndex::Int16;
        } else if (x <= std::numeric_limits<Int32>::max()) {
            type = TypeIndex::Int32;
        } else {
            type = TypeIndex::Int64;
        }
        return 1;
    }
    size_t operator()(const Int64& x) {
        if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
            type = TypeIndex::Int8;
        } else if (x <= std::numeric_limits<Int16>::max() &&
                   x >= std::numeric_limits<Int16>::min()) {
            type = TypeIndex::Int16;
        } else if (x <= std::numeric_limits<Int32>::max() &&
                   x >= std::numeric_limits<Int32>::min()) {
            type = TypeIndex::Int32;
        } else {
            type = TypeIndex::Int64;
        }
        return 1;
    }
    size_t operator()(const JsonbField& x) {
        type = TypeIndex::JSONB;
        return 1;
    }
    size_t operator()(const Null&) {
        have_nulls = true;
        return 1;
    }
    size_t operator()(const VariantMap&) {
        type = TypeIndex::VARIANT;
        return 1;
    }
    template <typename T>
    size_t operator()(const T&) {
        type = TypeId<NearestFieldType<T>>::value;
        return 1;
    }
    void get_scalar_type(TypeIndex* data_type) const { *data_type = type; }
    bool contain_nulls() const { return have_nulls; }

    bool need_convert_field() const { return false; }

private:
    TypeIndex type = TypeIndex::Nothing;
    bool have_nulls = false;
};

/// Visitor that allows to get type of scalar field
/// or least common type of scalars in array.
/// More optimized version of FieldToDataType.
class FieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    using FieldType = Field::Types::Which;
    size_t operator()(const Array& x) {
        size_t size = x.size();
        for (size_t i = 0; i < size; ++i) {
            apply_visitor(*this, x[i]);
        }
        return 0;
    }
    // TODO doris not support unsigned integers for now
    // treat as signed integers
    size_t operator()(const UInt64& x) {
        field_types.insert(FieldType::UInt64);
        if (x <= std::numeric_limits<Int8>::max()) {
            type_indexes.insert(TypeIndex::Int8);
        } else if (x <= std::numeric_limits<Int16>::max()) {
            type_indexes.insert(TypeIndex::Int16);
        } else if (x <= std::numeric_limits<Int32>::max()) {
            type_indexes.insert(TypeIndex::Int32);
        } else {
            type_indexes.insert(TypeIndex::Int64);
        }
        return 0;
    }
    size_t operator()(const Int64& x) {
        field_types.insert(FieldType::Int64);
        if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
            type_indexes.insert(TypeIndex::Int8);
        } else if (x <= std::numeric_limits<Int16>::max() &&
                   x >= std::numeric_limits<Int16>::min()) {
            type_indexes.insert(TypeIndex::Int16);
        } else if (x <= std::numeric_limits<Int32>::max() &&
                   x >= std::numeric_limits<Int32>::min()) {
            type_indexes.insert(TypeIndex::Int32);
        } else {
            type_indexes.insert(TypeIndex::Int64);
        }
        return 0;
    }
    size_t operator()(const JsonbField& x) {
        field_types.insert(FieldType::JSONB);
        type_indexes.insert(TypeIndex::JSONB);
        return 0;
    }
    size_t operator()(const VariantMap&) {
        field_types.insert(FieldType::VariantMap);
        type_indexes.insert(TypeIndex::VARIANT);
        return 0;
    }
    size_t operator()(const Null&) {
        have_nulls = true;
        return 0;
    }
    template <typename T>
    size_t operator()(const T&) {
        Field::EnumToType<Field::Types::Array>::Type a;
        field_types.insert(Field::TypeToEnum<NearestFieldType<T>>::value);
        type_indexes.insert(TypeId<NearestFieldType<T>>::value);
        return 0;
    }
    void get_scalar_type(TypeIndex* type) const {
        DataTypePtr data_type;
        get_least_supertype_jsonb(type_indexes, &data_type);
        *type = data_type->get_type_id();
    }
    bool contain_nulls() const { return have_nulls; }
    bool need_convert_field() const { return field_types.size() > 1; }

private:
    phmap::flat_hash_set<TypeIndex> type_indexes;
    phmap::flat_hash_set<FieldType> field_types;
    bool have_nulls = false;
};

/// Visitor that keeps @num_dimensions_to_keep dimensions in arrays
/// and replaces all scalars or nested arrays to @replacement at that level.
class FieldVisitorReplaceScalars : public StaticVisitor<Field> {
public:
    FieldVisitorReplaceScalars(const Field& replacement_, size_t num_dimensions_to_keep_)
            : replacement(replacement_), num_dimensions_to_keep(num_dimensions_to_keep_) {}

    Field operator()(const Array& x) const {
        if (num_dimensions_to_keep == 0) {
            return replacement;
        }

        const size_t size = x.size();
        Array res(size);
        for (size_t i = 0; i < size; ++i) {
            res[i] = apply_visitor(
                    FieldVisitorReplaceScalars(replacement, num_dimensions_to_keep - 1), x[i]);
        }
        return res;
    }

    template <typename T>
    Field operator()(const T&) const {
        return replacement;
    }

private:
    const Field& replacement;
    size_t num_dimensions_to_keep;
};

} // namespace

template <typename Visitor>
void get_field_info_impl(const Field& field, FieldInfo* info) {
    Visitor to_scalar_type_visitor;
    apply_visitor(to_scalar_type_visitor, field);
    TypeIndex type_id;
    to_scalar_type_visitor.get_scalar_type(&type_id);
    // array item's dimension may missmatch, eg. [1, 2, [1, 2, 3]]
    *info = {
            type_id,
            to_scalar_type_visitor.contain_nulls(),
            to_scalar_type_visitor.need_convert_field(),
            apply_visitor(FieldVisitorToNumberOfDimensions(), field),
    };
}

void get_field_info(const Field& field, FieldInfo* info) {
    if (field.is_complex_field()) {
        get_field_info_impl<FieldVisitorToScalarType>(field, info);
    } else {
        get_field_info_impl<SimpleFieldVisitorToScalarType>(field, info);
    }
}

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr&& data_, DataTypePtr type, bool is_nullable_,
                                   bool is_root_)
        : least_common_type(type), is_nullable(is_nullable_), is_root(is_root_) {
    data.push_back(std::move(data_));
    data_types.push_back(type);
    data_serdes.push_back(type->get_serde());
}

ColumnObject::Subcolumn::Subcolumn(size_t size_, bool is_nullable_, bool is_root_)
        : least_common_type(std::make_shared<DataTypeNothing>()),
          is_nullable(is_nullable_),
          num_of_defaults_in_prefix(size_),
          is_root(is_root_) {}

size_t ColumnObject::Subcolumn::Subcolumn::size() const {
    size_t res = num_of_defaults_in_prefix;
    for (const auto& part : data) {
        res += part->size();
    }
    return res;
}

size_t ColumnObject::Subcolumn::Subcolumn::byteSize() const {
    size_t res = 0;
    for (const auto& part : data) {
        res += part->byte_size();
    }
    return res;
}

size_t ColumnObject::Subcolumn::Subcolumn::allocatedBytes() const {
    size_t res = 0;
    for (const auto& part : data) {
        res += part->allocated_bytes();
    }
    return res;
}

void ColumnObject::Subcolumn::insert(Field field) {
    FieldInfo info;
    get_field_info(field, &info);
    insert(std::move(field), std::move(info));
}

void ColumnObject::Subcolumn::add_new_column_part(DataTypePtr type) {
    data.push_back(type->create_column());
    least_common_type = LeastCommonType {type};
    data_types.push_back(type);
    data_serdes.push_back(type->get_serde());
}

void ColumnObject::Subcolumn::insert(Field field, FieldInfo info) {
    auto base_type = WhichDataType(info.scalar_type_id);
    if (base_type.is_nothing() && info.num_dimensions == 0) {
        insert_default();
        return;
    }
    auto column_dim = least_common_type.get_dimensions();
    auto value_dim = info.num_dimensions;
    if (is_nothing(least_common_type.get_base())) {
        column_dim = value_dim;
    }
    if (base_type.is_nothing()) {
        value_dim = column_dim;
    }
    bool type_changed = false;
    if (value_dim != column_dim || info.num_dimensions >= 2) {
        // Deduce to JSONB
        VLOG_DEBUG << fmt::format(
                "Dimension of types mismatched between inserted value and column, "
                "expected:{}, but meet:{} for type:{}",
                column_dim, value_dim, least_common_type.get()->get_name());
        base_type = MOST_COMMON_TYPE_ID;
        value_dim = 0;
        type_changed = true;
    }
    if (data.empty()) {
        add_new_column_part(create_array_of_type(base_type.idx, value_dim, is_nullable));
    } else if (least_common_type.get_base_type_id() != base_type.idx && !base_type.is_nothing()) {
        if (schema_util::is_conversion_required_between_integers(
                    base_type.idx, least_common_type.get_base_type_id())) {
            VLOG_DEBUG << "Conversion between " << getTypeName(base_type.idx) << " and "
                       << getTypeName(least_common_type.get_type_id());
            DataTypePtr base_data_type;
            TypeIndex base_data_type_id;
            get_least_supertype_jsonb(
                    TypeIndexSet {base_type.idx, least_common_type.get_base_type_id()},
                    &base_data_type);
            type_changed = true;
            base_data_type_id = base_data_type->get_type_id();
            if (is_nullable) {
                base_data_type = make_nullable(base_data_type);
            }
            if (!least_common_type.get_base()->equals(*base_data_type)) {
                add_new_column_part(
                        create_array_of_type(base_data_type_id, value_dim, is_nullable));
            }
        }
    }

    if (type_changed || info.need_convert) {
        Field new_field;
        convert_field_to_type(field, *least_common_type.get(), &new_field);
        field = new_field;
    }

    data.back()->insert(field);
}

static DataTypePtr create_array(TypeIndex type, size_t num_dimensions) {
    DataTypePtr result_type = make_nullable(DataTypeFactory::instance().create_data_type(type));
    for (size_t i = 0; i < num_dimensions; ++i) {
        result_type = make_nullable(std::make_shared<DataTypeArray>(result_type));
    }
    return result_type;
}

Array create_empty_array_field(size_t num_dimensions) {
    if (num_dimensions == 0) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Cannot create array field with 0 dimensions");
    }

    Array array;
    Array* current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i) {
        current_array->push_back(Array());
        current_array = &current_array->back().get<Array&>();
    }

    return array;
}

// Recreates column with default scalar values and keeps sizes of arrays.
static ColumnPtr recreate_column_with_default_values(const ColumnPtr& column, TypeIndex scalar_type,
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

ColumnObject::Subcolumn ColumnObject::Subcolumn::clone_with_default_values(
        const FieldInfo& field_info) const {
    Subcolumn new_subcolumn(*this);
    new_subcolumn.least_common_type =
            LeastCommonType {create_array(field_info.scalar_type_id, field_info.num_dimensions)};

    for (int i = 0; i < new_subcolumn.data.size(); ++i) {
        new_subcolumn.data[i] = recreate_column_with_default_values(
                new_subcolumn.data[i], field_info.scalar_type_id, field_info.num_dimensions);
        new_subcolumn.data_types[i] = create_array_of_type(field_info.scalar_type_id,
                                                           field_info.num_dimensions, is_nullable);
    }

    return new_subcolumn;
}

Field ColumnObject::Subcolumn::get_last_field() const {
    if (data.empty()) {
        return Field();
    }

    const auto& last_part = data.back();
    assert(!last_part->empty());
    return (*last_part)[last_part->size() - 1];
}

void ColumnObject::Subcolumn::insert_range_from(const Subcolumn& src, size_t start, size_t length) {
    if (start + length > src.size()) {
        throw doris::Exception(
                ErrorCode::OUT_OF_BOUND,
                "Invalid range for insert_range_from: start={}, length={}, src.size={}", start,
                length, src.size());
    }
    size_t end = start + length;
    // num_rows += length;
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

bool ColumnObject::Subcolumn::is_finalized() const {
    return num_of_defaults_in_prefix == 0 && (data.empty() || (data.size() == 1));
}

template <typename Func>
MutableColumnPtr ColumnObject::apply_for_columns(Func&& func) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnObject&>(*finalized);
        return finalized_object.apply_for_columns(std::forward<Func>(func));
    }
    auto res = ColumnObject::create(is_nullable, false);
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = func(subcolumn->data.get_finalized_column_ptr());
        res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable(),
                            subcolumn->data.get_least_common_type());
    }
    auto sparse_column = func(serialized_sparse_column);
    res->serialized_sparse_column = sparse_column->assume_mutable();
    res->set_num_rows(serialized_sparse_column->size());
    check_consistency();
    return res;
}

void ColumnObject::resize(size_t n) {
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
}

bool ColumnObject::Subcolumn::check_if_sparse_column(size_t num_rows) {
    if (num_rows < config::variant_threshold_rows_to_estimate_sparse_column) {
        return false;
    }
    std::vector<double> defaults_ratio;
    for (size_t i = 0; i < data.size(); ++i) {
        defaults_ratio.push_back(data[i]->get_ratio_of_default_rows());
    }
    double default_ratio = std::accumulate(defaults_ratio.begin(), defaults_ratio.end(), 0.0) /
                           defaults_ratio.size();
    return default_ratio >= config::variant_ratio_of_defaults_as_sparse_column;
}

void ColumnObject::Subcolumn::finalize(FinalizeMode mode) {
    if (is_finalized()) {
        return;
    }
    if (data.size() == 1 && num_of_defaults_in_prefix == 0) {
        data[0] = data[0]->convert_to_full_column_if_const();
        return;
    }
    DataTypePtr to_type = least_common_type.get();
    if (mode == FinalizeMode::WRITE_MODE && is_root) {
        // Root always JSONB type in write mode
        to_type = is_nullable ? make_nullable(std::make_shared<MostCommonType>())
                              : std::make_shared<MostCommonType>();
        least_common_type = LeastCommonType {to_type};
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
    num_of_defaults_in_prefix = 0;
}

void ColumnObject::Subcolumn::insert_default() {
    if (data.empty()) {
        ++num_of_defaults_in_prefix;
    } else {
        data.back()->insert_default();
    }
}

void ColumnObject::Subcolumn::insert_many_defaults(size_t length) {
    if (data.empty()) {
        num_of_defaults_in_prefix += length;
    } else {
        data.back()->insert_many_defaults(length);
    }
}

void ColumnObject::Subcolumn::pop_back(size_t n) {
    if (n > size()) {
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Invalid number of elements to pop: {}, size: {}", n, size());
    }
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
    num_of_defaults_in_prefix -= n;
}

IColumn& ColumnObject::Subcolumn::get_finalized_column() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return *data[0];
}

const IColumn& ColumnObject::Subcolumn::get_finalized_column() const {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return *data[0];
}

const ColumnPtr& ColumnObject::Subcolumn::get_finalized_column_ptr() const {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return data[0];
}

ColumnPtr& ColumnObject::Subcolumn::get_finalized_column_ptr() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return data[0];
}

void ColumnObject::Subcolumn::remove_nullable() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    data[0] = doris::vectorized::remove_nullable(data[0]);
    least_common_type.remove_nullable();
}

ColumnObject::Subcolumn::LeastCommonType::LeastCommonType(DataTypePtr type_)
        : type(std::move(type_)),
          base_type(get_base_type_of_array(type)),
          num_dimensions(get_number_of_dimensions(*type)) {
    least_common_type_serder = type->get_serde();
    type_id = type->is_nullable() ? assert_cast<const DataTypeNullable*>(type.get())
                                            ->get_nested_type()
                                            ->get_type_id()
                                  : type->get_type_id();
    base_type_id = base_type->is_nullable() ? assert_cast<const DataTypeNullable*>(base_type.get())
                                                      ->get_nested_type()
                                                      ->get_type_id()
                                            : base_type->get_type_id();
}

ColumnObject::ColumnObject(bool is_nullable_, bool create_root_)
        : is_nullable(is_nullable_), num_rows(0) {
    if (create_root_) {
        subcolumns.create_root(Subcolumn(0, is_nullable, true /*root*/));
    }
}

ColumnObject::ColumnObject(MutableColumnPtr&& sparse_column)
        : is_nullable(true),
          num_rows(sparse_column->size()),
          serialized_sparse_column(std::move(sparse_column)) {}

ColumnObject::ColumnObject(bool is_nullable_, DataTypePtr type, MutableColumnPtr&& column)
        : is_nullable(is_nullable_), num_rows(0) {
    add_sub_column({}, std::move(column), type);
    serialized_sparse_column->insert_many_defaults(num_rows);
}

ColumnObject::ColumnObject(Subcolumns&& subcolumns_, bool is_nullable_)
        : is_nullable(is_nullable_),
          subcolumns(std::move(subcolumns_)),
          num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->data.size()) {
    check_consistency();
}

ColumnObject::ColumnObject(size_t size) : is_nullable(true), num_rows(0) {
    insert_many_defaults(size);
    check_consistency();
}

void ColumnObject::check_consistency() const {
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

size_t ColumnObject::size() const {
#ifndef NDEBUG
    check_consistency();
#endif
    return num_rows;
}

MutableColumnPtr ColumnObject::clone_resized(size_t new_size) const {
    if (new_size == 0) {
        return ColumnObject::create(is_nullable);
    }
    return apply_for_columns(
            [&](const ColumnPtr column) { return column->clone_resized(new_size); });
}

size_t ColumnObject::byte_size() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.byteSize();
    }
    res += serialized_sparse_column->byte_size();
    return res;
}

size_t ColumnObject::allocated_bytes() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.allocatedBytes();
    }
    res += serialized_sparse_column->allocated_bytes();
    return res;
}

void ColumnObject::for_each_subcolumn(ColumnCallback callback) {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            callback(part);
        }
    }
}

void ColumnObject::insert_from(const IColumn& src, size_t n) {
    const auto* src_v = check_and_get_column<ColumnObject>(src);
    // optimize when src and this column are scalar variant, since try_insert is inefficiency
    if (src_v != nullptr && src_v->is_scalar_variant() && is_scalar_variant() &&
        src_v->get_root_type()->equals(*get_root_type()) && src_v->is_finalized() &&
        is_finalized()) {
        assert_cast<ColumnNullable&, TypeCheckOnRelease::DISABLE>(*get_root())
                .insert_from(*src_v->get_root(), n);
        ++num_rows;
        return;
    }
    return try_insert(src[n]);
}

void ColumnObject::try_insert(const Field& field) {
    if (field.get_type() != Field::Types::VariantMap) {
        if (field.is_null()) {
            insert_default();
            return;
        }
        auto* root = get_subcolumn({});
        // Insert to an emtpy ColumnObject may result root null,
        // so create a root column of Variant is expected.
        if (root == nullptr) {
            bool succ = add_sub_column({}, num_rows);
            if (!succ) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to add root sub column {}");
            }
            root = get_subcolumn({});
        }
        root->insert(field);
    } else {
        const auto& object = field.get<const VariantMap&>();
        size_t old_size = size();
        for (const auto& [key_str, value] : object) {
            PathInData key;
            if (!key_str.empty()) {
                key = PathInData(key_str);
            }
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
            subcolumn->insert(value);
        }
        for (auto& entry : subcolumns) {
            if (old_size == entry->data.size()) {
                bool inserted = try_insert_default_from_nested(entry);
                if (!inserted) {
                    entry->data.insert_default();
                }
            }
        }
    }
    serialized_sparse_column->insert_default();
    ++num_rows;
}

void ColumnObject::insert_default() {
    for (auto& entry : subcolumns) {
        entry->data.insert_default();
    }
    serialized_sparse_column->insert_default();
    ++num_rows;
}

bool ColumnObject::Subcolumn::is_null_at(size_t n) const {
    if (least_common_type.get_base_type_id() == TypeIndex::Nothing) {
        return true;
    }
    size_t ind = n;
    if (ind < num_of_defaults_in_prefix) {
        return true;
    }

    ind -= num_of_defaults_in_prefix;
    for (const auto& part : data) {
        if (ind < part->size()) {
            return assert_cast<const ColumnNullable&>(*part).is_null_at(ind);
        }
        ind -= part->size();
    }

    throw doris::Exception(ErrorCode::OUT_OF_BOUND, "Index ({}) for getting field is out of range",
                           n);
}

void ColumnObject::Subcolumn::get(size_t n, Field& res) const {
    if (least_common_type.get_base_type_id() == TypeIndex::Nothing) {
        res = Null();
        return;
    }
    if (is_finalized()) {
        if (least_common_type.get_base_type_id() == TypeIndex::JSONB) {
            // JsonbFiled is special case
            res = JsonbField();
        }
        get_finalized_column().get(n, res);
        return;
    }

    size_t ind = n;
    if (ind < num_of_defaults_in_prefix) {
        res = least_common_type.get()->get_default();
        return;
    }

    ind -= num_of_defaults_in_prefix;
    for (size_t i = 0; i < data.size(); ++i) {
        const auto& part = data[i];
        const auto& part_type = data_types[i];
        if (ind < part->size()) {
            res = vectorized::remove_nullable(part_type)->get_default();
            part->get(ind, res);
            Field new_field;
            convert_field_to_type(res, *least_common_type.get(), &new_field);
            res = new_field;
            return;
        }

        ind -= part->size();
    }

    throw doris::Exception(ErrorCode::OUT_OF_BOUND, "Index ({}) for getting field is out of range",
                           n);
}

void ColumnObject::Subcolumn::serialize_to_sparse_column(ColumnString* key, std::string_view path,
                                                         ColumnString* value, size_t row,
                                                         bool& is_null) {
    // no need insert
    if (least_common_type.get_base_type_id() == TypeIndex::Nothing) {
        is_null = true;
        return;
    }

    // no need insert
    if (row < num_of_defaults_in_prefix) {
        is_null = true;
        return;
    }

    // remove default
    row -= num_of_defaults_in_prefix;
    for (size_t i = 0; i < data.size(); ++i) {
        const auto& part = data[i];
        if (row < part->size()) {
            // no need null in sparse column
            if (assert_cast<const ColumnNullable&>(*part).is_null_at(row)) {
                is_null = true;
            } else {
                is_null = false;
                // insert key
                key->insert_data(path.data(), path.size());

                // every subcolumn is always Nullable
                auto nullable_serde =
                        std::static_pointer_cast<DataTypeNullableSerDe>(data_types[i]->get_serde());
                auto& nullable_col = assert_cast<const ColumnNullable&>(*part);

                // insert value
                nullable_serde->get_nested_serde()->write_one_cell_to_binary(
                        nullable_col.get_nested_column(), value, row);
            }
            return;
        }

        row -= part->size();
    }

    throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                           "Index ({}) for serialize to sparse column is out of range", row);
}

const char* parse_binary_from_sparse_column(TypeIndex type, const char* data, Field& res,
                                            FieldInfo& info_res) {
    const char* end = data;
    switch (type) {
    case TypeIndex::String: {
        const size_t size = *reinterpret_cast<const size_t*>(data);
        data += sizeof(size_t);
        res = Field(String(data, size));
        end = data + size;
        break;
    }
    case TypeIndex::Int8: {
        res = *reinterpret_cast<const Int8*>(data);
        end = data + sizeof(Int8);
        break;
    }
    case TypeIndex::Int16: {
        res = *reinterpret_cast<const Int16*>(data);
        end = data + sizeof(Int16);
        break;
    }
    case TypeIndex::Int32: {
        res = *reinterpret_cast<const Int32*>(data);
        end = data + sizeof(Int32);
        break;
    }
    case TypeIndex::Int64: {
        res = *reinterpret_cast<const Int64*>(data);
        end = data + sizeof(Int64);
        break;
    }
    case TypeIndex::Float32: {
        res = *reinterpret_cast<const Float32*>(data);
        end = data + sizeof(Float32);
        break;
    }
    case TypeIndex::Float64: {
        res = *reinterpret_cast<const Float64*>(data);
        end = data + sizeof(Float64);
        break;
    }
    case TypeIndex::JSONB: {
        size_t size = *reinterpret_cast<const size_t*>(data);
        data += sizeof(size_t);
        res = JsonbField(data, size);
        end = data + size;
        break;
    }
    case TypeIndex::Array: {
        const size_t size = *reinterpret_cast<const size_t*>(data);
        data += sizeof(size_t);
        res = Array(size);
        auto& array = res.get<Array>();
        info_res.num_dimensions++;
        for (size_t i = 0; i < size; ++i) {
            Field nested_field;
            const auto nested_type =
                    assert_cast<const TypeIndex>(*reinterpret_cast<const uint8_t*>(data++));
            data = parse_binary_from_sparse_column(nested_type, data, nested_field, info_res);
            array.emplace_back(std::move(nested_field));
        }
        end = data;
        break;
    }
    default:
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Type ({}) for deserialize_from_sparse_column is invalid", type);
    }
    return end;
}

std::pair<Field, FieldInfo> ColumnObject::deserialize_from_sparse_column(const ColumnString* value,
                                                                         size_t row) {
    const auto& data_ref = value->get_data_at(row);
    const char* data = data_ref.data;
    DCHECK(data_ref.size > 1);
    const TypeIndex type = static_cast<const TypeIndex>(*reinterpret_cast<const uint8_t*>(data++));
    Field res;
    FieldInfo info_res = {
            .scalar_type_id = type,
            .have_nulls = false,
            .need_convert = false,
            .num_dimensions = 0,
    };
    const char* end = parse_binary_from_sparse_column(type, data, res, info_res);
    DCHECK_EQ(end - data_ref.data, data_ref.size);
    return {std::move(res), std::move(info_res)};
}

Field ColumnObject::operator[](size_t n) const {
    Field object;
    get(n, object);
    return object;
}

void ColumnObject::get(size_t n, Field& res) const {
    if (UNLIKELY(n >= size())) {
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Index ({}) for getting field is out of range for size {}", n,
                               size());
    }
    res = VariantMap();
    auto& object = res.get<VariantMap&>();

    for (const auto& entry : subcolumns) {
        Field field;
        entry->data.get(n, field);
        // Notice: we treat null as empty field, since we do not distinguish null and empty for Variant type.
        if (field.get_type() != Field::Types::Null) {
            object.try_emplace(entry->path.get_path(), field);
        }
    }

    const auto& [path, value] = get_sparse_data_paths_and_values();
    auto& sparse_column_offsets = serialized_sparse_column_offsets();
    size_t offset = sparse_column_offsets[n - 1];
    size_t end = sparse_column_offsets[n];
    // Iterator over [path, binary value]
    for (size_t i = offset; i != end; ++i) {
        const StringRef path_data = path->get_data_at(i);
        const auto& data = ColumnObject::deserialize_from_sparse_column(value, i);
        object.try_emplace(std::string(path_data.data, path_data.size), data.first);
    }

    if (object.empty()) {
        res = Null();
    }
}

void ColumnObject::add_nested_subcolumn(const PathInData& key, const FieldInfo& field_info,
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

bool ColumnObject::try_add_new_subcolumn(const PathInData& path) {
    if (subcolumns.size() == MAX_SUBCOLUMNS) return false;

    return add_sub_column(path, num_rows);
}

void ColumnObject::insert_range_from(const IColumn& src, size_t start, size_t length) {
#ifndef NDEBUG
    check_consistency();
#endif
    const auto& src_object = assert_cast<const ColumnObject&>(src);

    // First, insert src subcolumns
    // We can reach the limit of subcolumns, and in this case
    // the rest of subcolumns from src will be inserted into sparse column.
    std::map<std::string_view, Subcolumn> src_path_and_subcoumn_for_sparse_column;
    for (const auto& entry : src_object.subcolumns) {
        // Check if we already have such dense column path.
        if (auto* subcolumn = get_subcolumn(entry->path); subcolumn != nullptr) {
            subcolumn->insert_range_from(entry->data, start, length);
        } else if (try_add_new_subcolumn(entry->path)) {
            subcolumn = get_subcolumn(entry->path);
            DCHECK(subcolumn != nullptr);
            subcolumn->insert_range_from(entry->data, start, length);
        } else {
            src_path_and_subcoumn_for_sparse_column.emplace(entry->path.get_path(), entry->data);
        }
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
    finalize();
#ifndef NDEBUG
    check_consistency();
#endif
}

// std::map<std::string_view, Subcolumn>
void ColumnObject::insert_from_sparse_column_and_fill_remaing_dense_column(
        const ColumnObject& src,
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
            serialized_sparse_column->insert_many_defaults(length);
        } else {
            // Otherwise insert required src dense columns into sparse column.
            auto [sparse_column_keys, sparse_column_values] = get_sparse_data_paths_and_values();
            auto& sparse_column_offsets = serialized_sparse_column_offsets();
            for (size_t i = start; i != start + length; ++i) {
                int null_count = 0;
                // Paths in sorted_src_subcolumn_for_sparse_column are already sorted.
                for (auto& [path, subcolumn] : sorted_src_subcolumn_for_sparse_column) {
                    bool is_null = false;
                    subcolumn.serialize_to_sparse_column(sparse_column_keys, path,
                                                         sparse_column_values, i, is_null);
                    if (is_null) {
                        ++null_count;
                    }
                }

                // All the sparse columns in this row are null.
                if (null_count == sorted_src_subcolumn_for_sparse_column.size()) {
                    serialized_sparse_column->insert_default();
                } else {
                    DCHECK_EQ(sparse_column_keys->size(),
                              sparse_column_offsets[i - 1] +
                                      sorted_src_subcolumn_for_sparse_column.size() - null_count);
                    DCHECK_EQ(sparse_column_values->size(), sparse_column_keys->size());
                    sparse_column_offsets.push_back(sparse_column_keys->size());
                }
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
        int null_count = 0;

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
                        ColumnObject::deserialize_from_sparse_column(src_sparse_column_values, i);
                subcolumn->insert(data.first, data.second);
            } else {
                // Before inserting this path into sparse column check if we need to
                // insert suibcolumns from sorted_src_subcolumn_for_sparse_column before.
                while (sorted_src_subcolumn_for_sparse_column_idx <
                               sorted_src_subcolumn_for_sparse_column_size &&
                       sorted_src_subcolumn_for_sparse_column
                                       [sorted_src_subcolumn_for_sparse_column_idx]
                                               .first < src_sparse_path) {
                    auto& [src_path, src_subcolumn] = sorted_src_subcolumn_for_sparse_column
                            [sorted_src_subcolumn_for_sparse_column_idx++];
                    bool is_null = false;
                    src_subcolumn.serialize_to_sparse_column(sparse_column_path, src_path,
                                                             sparse_column_values, row, is_null);
                    if (is_null) {
                        ++null_count;
                    }
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
            bool is_null = false;
            src_subcolumn.serialize_to_sparse_column(sparse_column_path, src_path,
                                                     sparse_column_values, row, is_null);
            if (is_null) {
                ++null_count;
            }
        }

        // All the sparse columns in this row are null.
        if (null_count == sorted_src_subcolumn_for_sparse_column.size()) {
            serialized_sparse_column->insert_default();
        } else {
            sparse_column_offsets.push_back(sparse_column_path->size());
        }

        // Insert default values in all remaining dense columns.
        for (const auto& entry : subcolumns) {
            if (entry->data.size() == current_size) {
                entry->data.insert_default();
            }
        }
    }

    return;
}

ColumnPtr ColumnObject::permute(const Permutation& perm, size_t limit) const {
    return apply_for_columns([&](const ColumnPtr column) { return column->permute(perm, limit); });
}

void ColumnObject::pop_back(size_t length) {
    for (auto& entry : subcolumns) {
        entry->data.pop_back(length);
    }
    serialized_sparse_column->pop_back(length);
    num_rows -= length;
}

const ColumnObject::Subcolumn* ColumnObject::get_subcolumn(const PathInData& key) const {
    const auto* node = subcolumns.find_leaf(key);
    if (node == nullptr) {
        VLOG_DEBUG << "There is no subcolumn " << key.get_path();
        return nullptr;
    }
    return &node->data;
}

size_t ColumnObject::Subcolumn::serialize_text_json(size_t n, BufferWritable& output) const {
    if (least_common_type.get_base_type_id() == TypeIndex::Nothing) {
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
    DataTypeSerDe::FormatOptions opt;
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

const ColumnObject::Subcolumn* ColumnObject::get_subcolumn_with_cache(const PathInData& key,
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

ColumnObject::Subcolumn* ColumnObject::get_subcolumn(const PathInData& key, size_t key_index) {
    return const_cast<ColumnObject::Subcolumn*>(get_subcolumn_with_cache(key, key_index));
}

const ColumnObject::Subcolumn* ColumnObject::get_subcolumn(const PathInData& key,
                                                           size_t key_index) const {
    return get_subcolumn_with_cache(key, key_index);
}

ColumnObject::Subcolumn* ColumnObject::get_subcolumn(const PathInData& key) {
    const auto* node = subcolumns.find_leaf(key);
    if (node == nullptr) {
        VLOG_DEBUG << "There is no subcolumn " << key.get_path();
        return nullptr;
    }
    return &const_cast<Subcolumns::Node*>(node)->data;
}

bool ColumnObject::has_subcolumn(const PathInData& key) const {
    return subcolumns.find_leaf(key) != nullptr;
}

bool ColumnObject::add_sub_column(const PathInData& key, MutableColumnPtr&& subcolumn,
                                  DataTypePtr type) {
    size_t new_size = subcolumn->size();
    doc_structure = nullptr;
    if (key.empty() && subcolumns.empty()) {
        // create root
        subcolumns.create_root(Subcolumn(std::move(subcolumn), type, is_nullable, true));
        num_rows = new_size;
        return true;
    }
    if (key.empty() &&
        (!subcolumns.get_root()->is_scalar() ||
         (is_null_root() || is_nothing(subcolumns.get_root()->data.get_least_common_type())))) {
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
    return true;
}

bool ColumnObject::add_sub_column(const PathInData& key, size_t new_size) {
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
    doc_structure = nullptr;
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
    return true;
}

bool ColumnObject::is_finalized() const {
    return std::all_of(subcolumns.begin(), subcolumns.end(),
                       [](const auto& entry) { return entry->data.is_finalized(); });
}

void ColumnObject::Subcolumn::wrapp_array_nullable() {
    // Wrap array with nullable, treat empty array as null to elimate conflict at present
    auto& result_column = get_finalized_column_ptr();
    if (result_column->is_column_array() && !result_column->is_nullable()) {
        auto new_null_map = ColumnUInt8::create();
        new_null_map->reserve(result_column->size());
        auto& null_map_data = new_null_map->get_data();
        const auto* array = static_cast<const ColumnArray*>(result_column.get());
        for (size_t i = 0; i < array->size(); ++i) {
            null_map_data.push_back(array->is_default_at(i));
        }
        result_column = ColumnNullable::create(std::move(result_column), std::move(new_null_map));
        data_types[0] = make_nullable(data_types[0]);
        least_common_type = LeastCommonType {data_types[0]};
    }
}

rapidjson::Value* find_leaf_node_by_path(rapidjson::Value& json, const PathInData& path,
                                         int idx = 0) {
    if (idx >= path.get_parts().size()) {
        return &json;
    }

    std::string_view current_key = path.get_parts()[idx].key;
    if (!json.IsObject()) {
        return nullptr;
    }
    rapidjson::Value name(current_key.data(), current_key.size());
    auto it = json.FindMember(name);
    if (it == json.MemberEnd()) {
        return nullptr;
    }
    rapidjson::Value& current = it->value;
    // if (idx == path.get_parts().size() - 1) {
    //     return &current;
    // }
    return find_leaf_node_by_path(current, path, idx + 1);
}

// skip empty json:
// 1. null value as empty json, todo: think a better way to disinguish empty json and null json.
// 2. nested array with only nulls, eg. [null. null],todo: think a better way to deal distinguish array null value and real null value.
// 3. empty root jsonb value(not null)
// 4. type is nothing
bool skip_empty_json(const ColumnNullable* nullable, const DataTypePtr& type,
                     TypeIndex base_type_id, int row, const PathInData& path) {
    // skip nulls
    if (nullable && nullable->is_null_at(row)) {
        return true;
    }
    // check if it is empty nested json array, then skip
    if (base_type_id == TypeIndex::VARIANT && type->equals(*ColumnObject::NESTED_TYPE)) {
        Field field = (*nullable)[row];
        if (field.get_type() == Field::Types::Array) {
            const auto& array = field.get<Array>();
            bool only_nulls_inside = true;
            for (const auto& elem : array) {
                if (elem.get_type() != Field::Types::Null) {
                    only_nulls_inside = false;
                    break;
                }
            }
            // if only nulls then skip
            return only_nulls_inside;
        }
    }
    // skip empty jsonb value
    if ((path.empty() && nullable && nullable->get_data_at(row).empty())) {
        return true;
    }
    // skip nothing type
    if (base_type_id == TypeIndex::Nothing) {
        return true;
    }
    return false;
}

Status find_and_set_leave_value(const IColumn* column, const PathInData& path,
                                const DataTypeSerDeSPtr& type_serde, const DataTypePtr& type,
                                TypeIndex base_type_index, rapidjson::Value& root,
                                rapidjson::Document::AllocatorType& allocator, Arena& mem_pool,
                                int row) {
#ifndef NDEBUG
    // sanitize type and column
    if (column->get_name() != type->create_column()->get_name()) {
        return Status::InternalError(
                "failed to set value for path {}, expected type {}, but got {} at row {}",
                path.get_path(), type->get_name(), column->get_name(), row);
    }
#endif
    const auto* nullable = check_and_get_column<ColumnNullable>(column);
    if (skip_empty_json(nullable, type, base_type_index, row, path)) {
        return Status::OK();
    }
    // TODO could cache the result of leaf nodes with it's path info
    rapidjson::Value* target = find_leaf_node_by_path(root, path);
    if (UNLIKELY(!target)) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        root.Accept(writer);
        LOG(WARNING) << "could not find path " << path.get_path()
                     << ", root: " << std::string(buffer.GetString(), buffer.GetSize());
        return Status::NotFound("Not found path {}", path.get_path());
    }
    RETURN_IF_ERROR(type_serde->write_one_cell_to_json(*column, *target, allocator, mem_pool, row));
    return Status::OK();
}

// compact null values
// {"a" : {"b" : "d" {"n" : null}, "e" : null}, "c" : 10 }
// after compact -> {"a" : {"c"} : 10}
void compact_null_values(rapidjson::Value& json, rapidjson::Document::AllocatorType& allocator) {
    if (!json.IsObject() || json.IsNull()) {
        return;
    }

    rapidjson::Value::MemberIterator it = json.MemberBegin();
    while (it != json.MemberEnd()) {
        rapidjson::Value& value = it->value;
        if (value.IsNull()) {
            it = json.EraseMember(it);
            continue;
        }
        compact_null_values(value, allocator);
        if (value.IsObject() && value.ObjectEmpty()) {
            it = json.EraseMember(it);
            continue;
        }
        ++it;
    }
}

// Construct rapidjson value from Subcolumns
void get_json_by_column_tree(rapidjson::Value& root, rapidjson::Document::AllocatorType& allocator,
                             const ColumnObject::Subcolumns::Node* node_root) {
    if (node_root == nullptr || node_root->children.empty()) {
        root.SetNull();
        return;
    }
    root.SetObject();
    // sort to make output stable
    std::vector<StringRef> sorted_keys = node_root->get_sorted_chilren_keys();
    for (const StringRef& key : sorted_keys) {
        rapidjson::Value value(rapidjson::kObjectType);
        get_json_by_column_tree(value, allocator, node_root->get_child_node(key).get());
        root.AddMember(rapidjson::StringRef(key.data, key.size), value, allocator);
    }
}

Status ColumnObject::serialize_one_row_to_string(int64_t row, std::string* output) const {
    // if (!is_finalized()) {
    //     const_cast<ColumnObject*>(this)->finalize();
    // }
    if (is_scalar_variant() && is_finalized()) {
        auto type = get_root_type();
        *output = type->to_string(*get_root(), row);
        return Status::OK();
    }
    // TODO preallocate memory
    auto tmp_col = ColumnString::create();
    VectorBufferWriter write_buffer(*tmp_col.get());
    RETURN_IF_ERROR(serialize_one_row_to_json_format(row, write_buffer, nullptr));
    write_buffer.commit();
    auto str_ref = tmp_col->get_data_at(0);
    *output = std::string(str_ref.data, str_ref.size);
    return Status::OK();
}

Status ColumnObject::serialize_one_row_to_string(int64_t row, BufferWritable& output) const {
    // if (!is_finalized()) {
    //     const_cast<ColumnObject*>(this)->finalize();
    // }
    if (is_scalar_variant() && is_finalized()) {
        auto type = get_root_type();
        type->to_string(*get_root(), row, output);
        return Status::OK();
    }
    RETURN_IF_ERROR(serialize_one_row_to_json_format(row, output, nullptr));
    return Status::OK();
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
               elements[i].first == path_elements.elements[i])
            ++i;
        elements.resize(i);
    }

    /// Check is_first flag in current object.
    bool is_first_in_current_object() const {
        if (elements.empty()) return root_is_first_flag;
        return elements.back().second;
    }

    /// Set flag is_first = false in current object.
    void set_not_first_in_current_object() {
        if (elements.empty())
            root_is_first_flag = false;
        else
            elements.back().second = false;
    }

    size_t size() const { return elements.size(); }

    /// Elements of the prefix: (path element, is_first flag in this prefix).
    /// is_first flag indicates if we already serialized some key in the object with such prefix.
    std::vector<std::pair<std::string_view, bool>> elements;
    bool root_is_first_flag = true;
};

Status ColumnObject::serialize_one_row_to_json_format(int64_t row_num, BufferWritable& output,
                                                      bool* is_null) const {
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
    std::map<std::string, Subcolumn> subcolumn_path_map;
    sorted_paths.reserve(get_subcolumns().size() + (sparse_data_end - sparse_data_offset));
    for (const auto& subcolumn : get_subcolumns()) {
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

    writeChar('{', output);
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
                writeChar('}', output);
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
                    writeChar(',', output);
                } else {
                    current_prefix.set_not_first_in_current_object();
                }

                writeJSONString(path_elements.elements[i], output);
                writeCString(":{", output);

                // Update current prefix.
                current_prefix.elements.emplace_back(path_elements.elements[i], true);
            }
        }

        // Write comma before the key if it's not the first key in this prefix.
        if (!current_prefix.is_first_in_current_object()) {
            writeChar(',', output);
        } else {
            current_prefix.set_not_first_in_current_object();
        }

        writeJSONString(path_elements.elements.back(), output);
        writeCString(":", output);

        // Serialize value of current path.
        if (auto subcolumn_it = subcolumn_path_map.find(path);
            subcolumn_it != subcolumn_path_map.end()) {
            subcolumn_it->second.serialize_text_json(row_num, output);
        } else {
            // To serialize value stored in shared data we should first deserialize it from binary format.
            Subcolumn tmp_subcolumn(0, true);
            const auto& data = ColumnObject::deserialize_from_sparse_column(
                    sparse_data_values, index_in_sparse_data_values++);
            tmp_subcolumn.insert(data.first, data.second);
            tmp_subcolumn.serialize_text_json(0, output);
        }
    }

    // Close all remaining open objects.
    for (size_t i = 0; i != current_prefix.elements.size(); ++i) {
        writeChar('}', output);
    }
    writeChar('}', output);
#ifndef NDEBUG
    // check if it is a valid json
#endif
    return Status::OK();
}

size_t ColumnObject::Subcolumn::get_non_null_value_size() const {
    size_t res = 0;
    for (const auto& part : data) {
        const auto& null_data = assert_cast<const ColumnNullable&>(*part).get_null_map_data();
        res += simd::count_zero_num((int8_t*)null_data.data(), null_data.size());
    }
    return res;
}

Status ColumnObject::serialize_sparse_columns(
        std::map<std::string_view, Subcolumn>&& remaing_subcolumns) {
    CHECK(is_finalized());

    if (remaing_subcolumns.empty()) {
        serialized_sparse_column->insert_many_defaults(num_rows);
        return Status::OK();
    }
    serialized_sparse_column->reserve(num_rows);
    auto [sparse_column_keys, sparse_column_values] = get_sparse_data_paths_and_values();
    auto& sparse_column_offsets = serialized_sparse_column_offsets();

    // Fill the column map for each row
    for (size_t i = 0; i < num_rows; ++i) {
        int null_count = 0;

        for (auto& [path, subcolumn] : remaing_subcolumns) {
            bool is_null = false;
            subcolumn.serialize_to_sparse_column(sparse_column_keys, path, sparse_column_values, i,
                                                 is_null);
            if (is_null) {
                ++null_count;
            }
        }

        // All the sparse columns in this row are null.
        if (null_count == remaing_subcolumns.size()) {
            serialized_sparse_column->insert_default();
        } else {
            DCHECK_EQ(sparse_column_keys->size(),
                      sparse_column_offsets[i - 1] + remaing_subcolumns.size() - null_count);
            DCHECK_EQ(sparse_column_values->size(), sparse_column_keys->size());
            sparse_column_offsets.push_back(sparse_column_keys->size());
        }
    }
    CHECK_EQ(serialized_sparse_column->size(), num_rows);
    return Status::OK();
}

void ColumnObject::unnest(Subcolumns::NodePtr& entry, Subcolumns& subcolumns) const {
    entry->data.finalize();
    auto nested_column = entry->data.get_finalized_column_ptr()->assume_mutable();
    auto* nested_column_nullable = assert_cast<ColumnNullable*>(nested_column.get());
    auto* nested_column_array =
            assert_cast<ColumnArray*>(nested_column_nullable->get_nested_column_ptr().get());
    auto& offset = nested_column_array->get_offsets_ptr();

    auto* nested_object_nullable = assert_cast<ColumnNullable*>(
            nested_column_array->get_data_ptr()->assume_mutable().get());
    auto& nested_object_column =
            assert_cast<ColumnObject&>(nested_object_nullable->get_nested_column());
    PathInData nested_path = entry->path;
    for (auto& nested_entry : nested_object_column.subcolumns) {
        if (nested_entry->data.least_common_type.get_base_type_id() == TypeIndex::Nothing) {
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
                subnested_column, nested_column_nullable->get_null_map_column_ptr());
        auto type = make_nullable(
                std::make_shared<DataTypeArray>(nested_entry->data.least_common_type.get()));
        Subcolumn subcolumn(nullable_subnested_column->assume_mutable(), type, is_nullable);
        subcolumns.add(path_builder.build(), subcolumn);
    }
}

Status ColumnObject::finalize(FinalizeMode mode) {
    Subcolumns new_subcolumns;

    // finalize root first
    if (!is_null_root()) {
        new_subcolumns.create_root(subcolumns.get_root()->data);
        new_subcolumns.get_mutable_root()->data.finalize(mode);
    } else if (mode == FinalizeMode::WRITE_MODE) {
        new_subcolumns.create_root(Subcolumn(num_rows, is_nullable, true));
    }

    const bool need_pick_subcolumn_to_sparse_column =
            mode == FinalizeMode::WRITE_MODE && subcolumns.size() > MAX_SUBCOLUMNS;
    // finalize all subcolumns
    for (auto&& entry : subcolumns) {
        const auto& least_common_type = entry->data.get_least_common_type();
        /// Do not add subcolumns, which consists only from NULLs
        if (is_nothing(remove_nullable(get_base_type_of_array(least_common_type)))) {
            continue;
        }

        // unnest all nested columns, add them to new_subcolumns
        if (mode == FinalizeMode::WRITE_MODE &&
            least_common_type->equals(*ColumnObject::NESTED_TYPE)) {
            unnest(entry, new_subcolumns);
            continue;
        }

        if (entry->data.is_root) {
            continue;
        }
        entry->data.finalize(mode);
        entry->data.wrapp_array_nullable();

        if (!need_pick_subcolumn_to_sparse_column) {
            new_subcolumns.add(entry->path, entry->data);
        }
    }

    // caculate stats & merge and encode sparse column
    if (need_pick_subcolumn_to_sparse_column) {
        // pick sparse columns
        std::set<std::string_view> selected_path;
        // pick subcolumns sort by size of none null values
        std::unordered_map<std::string_view, size_t> none_null_value_sizes;
        // 1. get the none null value sizes
        for (auto&& entry : subcolumns) {
            if (entry->data.is_root) {
                continue;
            }
            size_t size = entry->data.get_non_null_value_size();
            none_null_value_sizes[entry->path.get_path()] = size;
        }
        // 2. sort by the size
        std::vector<std::pair<std::string_view, size_t>> sorted_by_size(
                none_null_value_sizes.begin(), none_null_value_sizes.end());
        std::sort(sorted_by_size.begin(), sorted_by_size.end(),
                  [](const auto& a, const auto& b) { return a.second > b.second; });

        // 3. pick MAX_SUBCOLUMNS selected subcolumns
        for (size_t i = 0; i < std::min(MAX_SUBCOLUMNS, sorted_by_size.size()); ++i) {
            selected_path.insert(sorted_by_size[i].first);
        }
        std::map<std::string_view, Subcolumn> remaing_subcolumns;
        // add selected subcolumns to new_subcolumns, otherwise add to remaining_subcolumns
        for (auto&& entry : subcolumns) {
            if (selected_path.find(entry->path.get_path()) != selected_path.end()) {
                new_subcolumns.add(entry->path, entry->data);
            } else {
                remaing_subcolumns.emplace(entry->path.get_path(), entry->data);
            }
        }
        serialized_sparse_column->clear();
        RETURN_IF_ERROR(serialize_sparse_columns(std::move(remaing_subcolumns)));
    }

    std::swap(subcolumns, new_subcolumns);
    doc_structure = nullptr;
    _prev_positions.clear();
    return Status::OK();
}

void ColumnObject::finalize() {
    static_cast<void>(finalize(FinalizeMode::READ_MODE));
}

void ColumnObject::ensure_root_node_type(const DataTypePtr& expected_root_type) {
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
        root.least_common_type = Subcolumn::LeastCommonType {expected_root_type};
    }
}

bool ColumnObject::empty() const {
    return subcolumns.empty() || subcolumns.begin()->get()->path.get_path() == COLUMN_NAME_DUMMY;
}

ColumnPtr get_base_column_of_array(const ColumnPtr& column) {
    if (const auto* column_array = check_and_get_column<ColumnArray>(column)) {
        return column_array->get_data_ptr();
    }
    return column;
}

ColumnPtr ColumnObject::filter(const Filter& filter, ssize_t count) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnObject&>(*finalized);
        return finalized_object.filter(filter, count);
    }
    if (subcolumns.empty()) {
        auto res = ColumnObject::create(count_bytes_in_filter(filter));
        return res;
    }
    auto new_column = ColumnObject::create(true, false);
    for (auto& entry : subcolumns) {
        auto subcolumn = entry->data.get_finalized_column().filter(filter, -1);
        new_column->add_sub_column(entry->path, subcolumn->assume_mutable(),
                                   entry->data.get_least_common_type());
    }

    return new_column;
}

ColumnPtr ColumnObject::replicate(const IColumn::Offsets& offsets) const {
    column_match_offsets_size(num_rows, offsets.size());
    return apply_for_columns([&](const ColumnPtr column) { return column->replicate(offsets); });
}

size_t ColumnObject::filter(const Filter& filter) {
    if (!is_finalized()) {
        finalize();
    }
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        clear();
    } else {
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
        const auto result_size = serialized_sparse_column->filter(filter);
        if (result_size != count) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "result_size not euqal with filter_size, result_size={}, "
                            "filter_size={}",
                            result_size, count);
        }
        CHECK_EQ(result_size, count);
    }
    num_rows = count;
#ifndef NDEBUG
    check_consistency();
#endif
    return count;
}

void ColumnObject::clear_column_data() {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            DCHECK_EQ(part->use_count(), 1);
            (*std::move(part)).clear();
        }
        entry->data.num_of_defaults_in_prefix = 0;
    }
    serialized_sparse_column->clear();
    num_rows = 0;
}

void ColumnObject::clear() {
    Subcolumns empty;
    std::swap(empty, subcolumns);
    serialized_sparse_column->clear();
    num_rows = 0;
    _prev_positions.clear();
}

void ColumnObject::create_root(const DataTypePtr& type, MutableColumnPtr&& column) {
    if (num_rows == 0) {
        num_rows = column->size();
    }
    add_sub_column({}, std::move(column), type);
}

const DataTypePtr& ColumnObject::get_most_common_type() {
    static auto type = make_nullable(std::make_shared<MostCommonType>());
    return type;
}

bool ColumnObject::is_null_root() const {
    auto* root = subcolumns.get_root();
    if (root == nullptr) {
        return true;
    }
    if (root->data.num_of_defaults_in_prefix == 0 &&
        (root->data.data.empty() || is_nothing(root->data.get_least_common_type()))) {
        return true;
    }
    return false;
}

bool ColumnObject::is_scalar_variant() const {
    // Only root itself
    return !is_null_root() && subcolumns.get_leaves().size() == 1 &&
           subcolumns.get_root()->is_scalar();
}

const DataTypePtr ColumnObject::NESTED_TYPE = std::make_shared<vectorized::DataTypeNullable>(
        std::make_shared<vectorized::DataTypeArray>(std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeObject>())));

const size_t ColumnObject::MAX_SUBCOLUMNS = 5;

DataTypePtr ColumnObject::get_root_type() const {
    return subcolumns.get_root()->data.get_least_common_type();
}

#define SANITIZE_ROOT()                                                                            \
    if (is_null_root()) {                                                                          \
        return Status::InternalError("No root column, path {}", path.get_path());                  \
    }                                                                                              \
    if (!WhichDataType(remove_nullable(subcolumns.get_root()->data.get_least_common_type()))       \
                 .is_json()) {                                                                     \
        return Status::InternalError(                                                              \
                "Root column is not jsonb type but {}, path {}",                                   \
                subcolumns.get_root()->data.get_least_common_type()->get_name(), path.get_path()); \
    }

Status ColumnObject::extract_root(const PathInData& path, MutableColumnPtr& dst) const {
    SANITIZE_ROOT();
    if (!path.empty()) {
        RETURN_IF_ERROR(schema_util::extract(subcolumns.get_root()->data.get_finalized_column_ptr(),
                                             path, dst));
    } else {
        if (!dst) {
            dst = subcolumns.get_root()->data.get_finalized_column_ptr()->clone_empty();
            dst->reserve(num_rows);
        }
        dst->insert_range_from(*subcolumns.get_root()->data.get_finalized_column_ptr(), 0,
                               num_rows);
    }
    return Status::OK();
}

void ColumnObject::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                       const uint32_t* indices_end) {
    for (const auto* x = indices_begin; x != indices_end; ++x) {
        ColumnObject::insert_from(src, *x);
    }
}

template <typename Func>
void ColumnObject::for_each_imutable_column(Func&& callback) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnObject&>(*finalized);
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

void ColumnObject::update_hash_with_value(size_t n, SipHash& hash) const {
    for_each_imutable_column(
            [&](const ColumnPtr column) { return column->update_hash_with_value(n, hash); });
}

void ColumnObject::update_hashes_with_value(uint64_t* __restrict hashes,
                                            const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_hashes_with_value(hashes, nullptr);
    });
}

void ColumnObject::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                            const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_xxHash_with_value(start, end, hash, nullptr);
    });
}

void ColumnObject::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type,
                                          uint32_t rows, uint32_t offset,
                                          const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_crcs_with_value(hash, type, rows, offset, nullptr);
    });
}

void ColumnObject::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                         const uint8_t* __restrict null_data) const {
    for_each_imutable_column([&](const ColumnPtr column) {
        return column->update_crc_with_value(start, end, hash, nullptr);
    });
}

std::string ColumnObject::debug_string() const {
    std::stringstream res;
    res << get_name() << "(num_row = " << num_rows;
    for (auto& entry : subcolumns) {
        if (entry->data.is_finalized()) {
            res << "[column:" << entry->data.data[0]->dump_structure()
                << ",type:" << entry->data.data_types[0]->get_name()
                << ",path:" << entry->path.get_path() << "],";
        }
    }
    res << ")";
    return res.str();
}

Status ColumnObject::sanitize() const {
    RETURN_IF_CATCH_EXCEPTION(check_consistency());
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_finalized()) {
            auto column = subcolumn->data.get_least_common_type()->create_column();
            std::string original = subcolumn->data.get_finalized_column().get_name();
            std::string expected = column->get_name();
            if (original != expected) {
                return Status::InternalError("Incompatible type between {} and {}, debug_info:",
                                             original, expected, debug_string());
            }
        }
    }

    VLOG_DEBUG << "sanitized " << debug_string();
    return Status::OK();
}

ColumnObject::Subcolumn ColumnObject::Subcolumn::cut(size_t start, size_t length) const {
    Subcolumn new_subcolumn(0, is_nullable);
    new_subcolumn.insert_range_from(*this, start, length);
    return new_subcolumn;
}

const ColumnObject::Subcolumns::Node* ColumnObject::get_leaf_of_the_same_nested(
        const Subcolumns::NodePtr& entry) const {
    const auto* leaf = subcolumns.get_leaf_of_the_same_nested(
            entry->path,
            [&](const Subcolumns::Node& node) { return node.data.size() > entry->data.size(); });
    if (leaf && is_nothing(leaf->data.get_least_common_typeBase())) {
        return nullptr;
    }
    return leaf;
}

bool ColumnObject::try_insert_many_defaults_from_nested(const Subcolumns::NodePtr& entry) const {
    const auto* leaf = get_leaf_of_the_same_nested(entry);
    if (!leaf) {
        return false;
    }

    size_t old_size = entry->data.size();
    FieldInfo field_info = {
            .scalar_type_id = entry->data.least_common_type.get_base_type_id(),
            .have_nulls = false,
            .need_convert = false,
            .num_dimensions = entry->data.get_dimensions(),
    };

    /// Cut the needed range from the found leaf
    /// and replace scalar values to the correct
    /// default values for given entry.
    auto new_subcolumn = leaf->data.cut(old_size, leaf->data.size() - old_size)
                                 .clone_with_default_values(field_info);

    entry->data.insert_range_from(new_subcolumn, 0, new_subcolumn.size());
    return true;
}

bool ColumnObject::try_insert_default_from_nested(const Subcolumns::NodePtr& entry) const {
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

    auto default_scalar =
            entry_num_dimensions > leaf_num_dimensions
                    ? create_empty_array_field(entry_num_dimensions - leaf_num_dimensions)
                    : entry->data.get_least_common_type()->get_default();

    auto default_field = apply_visitor(
            FieldVisitorReplaceScalars(default_scalar, leaf_num_dimensions), last_field);
    entry->data.insert(std::move(default_field));
    return true;
}

size_t ColumnObject::find_path_lower_bound_in_sparse_data(StringRef path,
                                                          const ColumnString& sparse_data_paths,
                                                          size_t start, size_t end) {
    // Simple random access iterator over values in ColumnString in specified range.
    class Iterator {
    public:
        using difference_type = size_t;
        using value_type = StringRef;
        using iterator_category = std::random_access_iterator_tag;
        using pointer = StringRef*;
        using reference = StringRef&;

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
        inline Iterator& operator--() {
            --index;
            return *this;
        }
        inline difference_type operator-(const Iterator& rhs) const { return index - rhs.index; }

        const ColumnString* data;
        size_t index;
    };

    Iterator start_it(&sparse_data_paths, start);
    Iterator end_it(&sparse_data_paths, end);
    auto it = std::lower_bound(start_it, end_it, path);
    return it.index;
}

void ColumnObject::fill_path_olumn_from_sparse_data(Subcolumn& subcolumn, StringRef path,
                                                    const ColumnPtr& sparse_data_column,
                                                    size_t start, size_t end) {
    const auto& sparse_data_map = assert_cast<const ColumnMap&>(*sparse_data_column);
    const auto& sparse_data_offsets = sparse_data_map.get_offsets();
    size_t first_offset = sparse_data_offsets[static_cast<ssize_t>(start) - 1];
    size_t last_offset = sparse_data_offsets[static_cast<ssize_t>(end) - 1];
    // Check if we have at least one row with data.
    if (first_offset == last_offset) {
        subcolumn.insert_many_defaults(end - start);
        return;
    }

    const auto& sparse_data_paths = assert_cast<const ColumnString&>(sparse_data_map.get_keys());
    const auto& sparse_data_values = assert_cast<const ColumnString&>(sparse_data_map.get_values());
    for (size_t i = start; i != end; ++i) {
        size_t paths_start = sparse_data_offsets[static_cast<ssize_t>(i) - 1];
        size_t paths_end = sparse_data_offsets[static_cast<ssize_t>(i)];
        auto lower_bound_path_index = ColumnObject::find_path_lower_bound_in_sparse_data(
                path, sparse_data_paths, paths_start, paths_end);
        if (lower_bound_path_index != paths_end &&
            sparse_data_paths.get_data_at(lower_bound_path_index) == path) {
            // auto value_data = sparse_data_values.get_data_at(lower_bound_path_index);
            // ReadBufferFromMemory buf(value_data.data, value_data.size);
            // dynamic_serialization->deserializeBinary(path_column, buf, getFormatSettings());
            const auto& data = ColumnObject::deserialize_from_sparse_column(&sparse_data_values,
                                                                            lower_bound_path_index);
            subcolumn.insert(data.first, data.second);
        } else {
            subcolumn.insert_default();
        }
    }
}

} // namespace doris::vectorized
