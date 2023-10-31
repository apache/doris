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
#include <fmt/format.h>
#include <parallel_hashmap/phmap.h>

#include <functional>
#include <limits>
#include <map>
#include <optional>

#include "common/exception.h"
#include "common/status.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/columns_number.h"
#include "vec/common/field_visitors.h"
#include "vec/common/schema_util.h"
#include "vec/core/field.h"
#include "vec/data_types/convert_field_to_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/get_least_supertype.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {
namespace {

DataTypePtr create_array_of_type(DataTypePtr type, size_t num_dimensions) {
    for (size_t i = 0; i < num_dimensions; ++i) {
        type = std::make_shared<DataTypeArray>(std::move(type));
    }
    return type;
}

DataTypePtr getBaseTypeOfArray(const DataTypePtr& type) {
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray* last_array = nullptr;
    const auto* current_type = type.get();
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
    }
    return last_array ? last_array->get_nested_type() : type;
}

size_t getNumberOfDimensions(const IDataType& type) {
    if (const auto* type_array = typeid_cast<const DataTypeArray*>(&type)) {
        return type_array->get_number_of_dimensions();
    }
    return 0;
}

DataTypePtr get_data_type_by_column(const IColumn& column) {
    // Removed in the future PR
    // assert(false);
    return nullptr;
}

/// Recreates column with default scalar values and keeps sizes of arrays.
ColumnPtr recreate_column_with_default_value(const ColumnPtr& column,
                                             const DataTypePtr& scalar_type,
                                             size_t num_dimensions) {
    const auto* column_array = check_and_get_column<ColumnArray>(column.get());
    if (column_array && num_dimensions) {
        return ColumnArray::create(
                recreate_column_with_default_value(column_array->get_data_ptr(), scalar_type,
                                                   num_dimensions - 1),
                IColumn::mutate(column_array->get_offsets_ptr()));
    }
    return create_array_of_type(scalar_type, num_dimensions)
            ->create_column()
            ->clone_resized(column->size());
}

Array create_empty_array_field(size_t num_dimensions) {
    assert(num_dimensions != 0);
    Array array;
    Array* current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i) {
        current_array->push_back(Array());
        current_array = &current_array->back().get<Array&>();
    }
    return array;
}

/// Replaces NULL fields to given field or empty array.
class FieldVisitorReplaceNull : public StaticVisitor<Field> {
public:
    explicit FieldVisitorReplaceNull(const Field& replacement_, size_t num_dimensions_)
            : replacement(replacement_), num_dimensions(num_dimensions_) {}
    Field operator()(const Null&) const {
        return num_dimensions ? create_empty_array_field(num_dimensions) : replacement;
    }
    Field operator()(const Array& x) const {
        assert(num_dimensions > 0);
        const size_t size = x.size();
        Array res(size);
        for (size_t i = 0; i < size; ++i) {
            res[i] = apply_visitor(FieldVisitorReplaceNull(replacement, num_dimensions - 1), x[i]);
        }
        return res;
    }
    template <typename T>
    Field operator()(const T& x) const {
        return x;
    }

private:
    const Field& replacement;
    size_t num_dimensions;
};

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t> {
public:
    size_t operator()(const Array& x) const {
        const size_t size = x.size();
        std::optional<size_t> dimensions;
        for (size_t i = 0; i < size; ++i) {
            /// Do not count Nulls, because they will be replaced by default
            /// values with proper number of dimensions.
            if (x[i].is_null()) {
                continue;
            }
            size_t current_dimensions = apply_visitor(*this, x[i]);
            if (!dimensions) {
                dimensions = current_dimensions;
            } else if (current_dimensions != *dimensions) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Number of dimensions mismatched among array elements");
                return 0;
            }
        }
        return 1 + dimensions.value_or(0);
    }
    template <typename T>
    size_t operator()(const T&) const {
        return 0;
    }
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
        // // Only Int64 | Int32 at present
        // field_types.insert(FieldType::Int64);
        // type_indexes.insert(TypeIndex::Int64);
        // return 0;
        field_types.insert(FieldType::Int64);
        if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min()) {
            type_indexes.insert(TypeIndex::Int32);
        } else {
            type_indexes.insert(TypeIndex::Int64);
        }
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
    void get_scalar_type(DataTypePtr* type) const {
        get_least_supertype(type_indexes, type, true /*compatible with string type*/);
    }
    bool contain_nulls() const { return have_nulls; }
    bool need_convert_field() const { return field_types.size() > 1; }

private:
    phmap::flat_hash_set<TypeIndex> type_indexes;
    phmap::flat_hash_set<FieldType> field_types;
    bool have_nulls = false;
};

} // namespace
void get_field_info(const Field& field, FieldInfo* info) {
    FieldVisitorToScalarType to_scalar_type_visitor;
    apply_visitor(to_scalar_type_visitor, field);
    DataTypePtr type = nullptr;
    to_scalar_type_visitor.get_scalar_type(&type);
    // array item's dimension may missmatch, eg. [1, 2, [1, 2, 3]]
    *info = {
            type,
            to_scalar_type_visitor.contain_nulls(),
            to_scalar_type_visitor.need_convert_field(),
            apply_visitor(FieldVisitorToNumberOfDimensions(), field),
    };
}

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr&& data_, bool is_nullable_)
        : least_common_type(get_data_type_by_column(*data_)), is_nullable(is_nullable_) {
    data.push_back(std::move(data_));
}

ColumnObject::Subcolumn::Subcolumn(size_t size_, bool is_nullable_)
        : least_common_type(std::make_shared<DataTypeNothing>()),
          is_nullable(is_nullable_),
          num_of_defaults_in_prefix(size_) {}

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
    least_common_type = LeastCommonType {std::move(type)};
}

void ColumnObject::Subcolumn::insert(Field field, FieldInfo info) {
    auto base_type = std::move(info.scalar_type);
    if (is_nothing(base_type)) {
        insertDefault();
        return;
    }
    auto column_dim = least_common_type.get_dimensions();
    auto value_dim = info.num_dimensions;
    if (is_nothing(least_common_type.getBase())) {
        column_dim = value_dim;
    }
    if (is_nothing(base_type)) {
        value_dim = column_dim;
    }
    if (value_dim != column_dim) {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Dimension of types mismatched between inserted value and column, "
                               "expected:{}, but meet:{} for type:{}",
                               column_dim, value_dim, least_common_type.get()->get_name());
    }
    if (is_nullable && !is_nothing(base_type)) {
        base_type = make_nullable(base_type);
    }
    // alawys nullable at present
    if (!is_nullable && info.have_nulls) {
        field = apply_visitor(FieldVisitorReplaceNull(base_type->get_default(), value_dim),
                              std::move(field));
    }
    // need replace muli dimensions array which contains null. eg. [[1, 2, 3], null] -> [[1, 2, 3], []]
    // since column array doesnt known null's dimension
    if (info.num_dimensions >= 2 && info.have_nulls) {
        field = apply_visitor(FieldVisitorReplaceNull(base_type->get_default(), value_dim),
                              std::move(field));
    }

    bool type_changed = false;
    const auto& least_common_base_type = least_common_type.getBase();
    if (data.empty()) {
        add_new_column_part(create_array_of_type(std::move(base_type), value_dim));
    } else if (!least_common_base_type->equals(*base_type) && !is_nothing(base_type)) {
        if (!schema_util::is_conversion_required_between_integers(*base_type,
                                                                  *least_common_base_type)) {
            get_least_supertype(DataTypes {std::move(base_type), least_common_base_type},
                                &base_type, true /*compatible with string type*/);
            type_changed = true;
            if (!least_common_base_type->equals(*base_type)) {
                add_new_column_part(create_array_of_type(std::move(base_type), value_dim));
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

void ColumnObject::Subcolumn::insertRangeFrom(const Subcolumn& src, size_t start, size_t length) {
    assert(src.is_finalized());
    const auto& src_column = src.data.back();
    const auto& src_type = src.least_common_type.get();
    if (data.empty()) {
        add_new_column_part(src.least_common_type.get());
        data.back()->insert_range_from(*src_column, start, length);
    } else if (least_common_type.get()->equals(*src_type)) {
        data.back()->insert_range_from(*src_column, start, length);
    } else {
        DataTypePtr new_least_common_type = nullptr;
        get_least_supertype(DataTypes {least_common_type.get(), src_type}, &new_least_common_type,
                            true /*compatible with string type*/);
        ColumnPtr casted_column;
        Status st = schema_util::cast_column({src_column, src_type, ""}, new_least_common_type,
                                             &casted_column);
        if (!st.ok()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string() + ", real_code:{}",
                                   st.code());
        }
        if (!least_common_type.get()->equals(*new_least_common_type)) {
            add_new_column_part(std::move(new_least_common_type));
        }
        data.back()->insert_range_from(*casted_column, start, length);
    }
}

bool ColumnObject::Subcolumn::is_finalized() const {
    return data.empty() || (data.size() == 1 && num_of_defaults_in_prefix == 0);
}

template <typename Func>
ColumnPtr ColumnObject::apply_for_subcolumns(Func&& func, std::string_view func_name) const {
    if (!is_finalized()) {
        // LOG(FATAL) << "Cannot " << func_name << " non-finalized ColumnObject";
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Cannot {} non-finalized ColumnObject", func_name);
    }
    auto res = ColumnObject::create(is_nullable);
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = func(subcolumn->data.get_finalized_column());
        res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable());
    }
    return res;
}
ColumnPtr ColumnObject::index(const IColumn& indexes, size_t limit) const {
    return apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.index(indexes, limit); }, "index");
}

void ColumnObject::Subcolumn::finalize() {
    if (is_finalized()) {
        return;
    }
    if (data.size() == 1 && num_of_defaults_in_prefix == 0) {
        data[0] = data[0]->convert_to_full_column_if_const();
        return;
    }
    const auto& to_type = least_common_type.get();
    auto result_column = to_type->create_column();
    if (num_of_defaults_in_prefix) {
        result_column->insert_many_defaults(num_of_defaults_in_prefix);
    }
    for (auto& part : data) {
        part = part->convert_to_full_column_if_const();
        auto from_type = get_data_type_by_column(*part);
        size_t part_size = part->size();
        if (!from_type->equals(*to_type)) {
            auto offsets = ColumnUInt64::create();
            auto& offsets_data = offsets->get_data();
            /// We need to convert only non-default values and then recreate column
            /// with default value of new type, because default values (which represents misses in data)
            /// may be inconsistent between types (e.g "0" in UInt64 and empty string in String).
            part->get_indices_of_non_default_rows(offsets_data, 0, part_size);
            if (offsets->size() == part_size) {
                ColumnPtr ptr;
                static_cast<void>(schema_util::cast_column({part, from_type, ""}, to_type, &ptr));
                part = ptr;
            } else {
                auto values = part->index(*offsets, offsets->size());
                static_cast<void>(
                        schema_util::cast_column({values, from_type, ""}, to_type, &values));
                part = values->create_with_offsets(offsets_data, to_type->get_default(), part_size,
                                                   /*shift=*/0);
            }
        }
        result_column->insert_range_from(*part, 0, part_size);
    }
    data = {std::move(result_column)};
    num_of_defaults_in_prefix = 0;
}

void ColumnObject::Subcolumn::insertDefault() {
    if (data.empty()) {
        ++num_of_defaults_in_prefix;
    } else {
        data.back()->insert_default();
    }
}

void ColumnObject::Subcolumn::insertManyDefaults(size_t length) {
    if (data.empty()) {
        num_of_defaults_in_prefix += length;
    } else {
        data.back()->insert_many_defaults(length);
    }
}

void ColumnObject::Subcolumn::pop_back(size_t n) {
    assert(n <= size());
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
    data.resize(data.size() - num_removed);
    num_of_defaults_in_prefix -= n;
}

Field ColumnObject::Subcolumn::get_last_field() const {
    if (data.empty()) {
        return Field();
    }
    const auto& last_part = data.back();
    assert(!last_part->empty());
    return (*last_part)[last_part->size() - 1];
}

ColumnObject::Subcolumn ColumnObject::Subcolumn::recreate_with_default_values(
        const FieldInfo& field_info) const {
    auto scalar_type = field_info.scalar_type;
    if (is_nullable) {
        scalar_type = make_nullable(scalar_type);
    }
    Subcolumn new_subcolumn;
    new_subcolumn.least_common_type =
            LeastCommonType {create_array_of_type(scalar_type, field_info.num_dimensions)};
    new_subcolumn.is_nullable = is_nullable;
    new_subcolumn.num_of_defaults_in_prefix = num_of_defaults_in_prefix;
    new_subcolumn.data.reserve(data.size());
    for (const auto& part : data) {
        new_subcolumn.data.push_back(
                recreate_column_with_default_value(part, scalar_type, field_info.num_dimensions));
    }
    return new_subcolumn;
}

IColumn& ColumnObject::Subcolumn::get_finalized_column() {
    assert(is_finalized());
    return *data[0];
}

const IColumn& ColumnObject::Subcolumn::get_finalized_column() const {
    assert(is_finalized());
    return *data[0];
}

const ColumnPtr& ColumnObject::Subcolumn::get_finalized_column_ptr() const {
    assert(is_finalized());
    return data[0];
}

void ColumnObject::Subcolumn::remove_nullable() {
    assert(is_finalized());
    data[0] = doris::vectorized::remove_nullable(data[0]);
    least_common_type.remove_nullable();
}

ColumnObject::Subcolumn::LeastCommonType::LeastCommonType(DataTypePtr type_)
        : type(std::move(type_)),
          base_type(getBaseTypeOfArray(type)),
          num_dimensions(getNumberOfDimensions(*type)) {}

ColumnObject::ColumnObject(bool is_nullable_) : is_nullable(is_nullable_), num_rows(0) {}

ColumnObject::ColumnObject(Subcolumns&& subcolumns_, bool is_nullable_)
        : is_nullable(is_nullable_),
          subcolumns(std::move(subcolumns_)),
          num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->data.size()) {
    check_consistency();
}

void ColumnObject::check_consistency() const {
    if (subcolumns.empty()) {
        return;
    }
    for (const auto& leaf : subcolumns) {
        if (num_rows != leaf->data.size()) {
            // LOG(FATAL) << "unmatched column:" << leaf->path.get_path()
            //            << ", expeted rows:" << num_rows << ", but meet:" << leaf->data.size();
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "unmatched column: {}, expeted rows: {}, but meet: {}",
                                   leaf->path.get_path(), num_rows, leaf->data.size());
        }
    }
}

size_t ColumnObject::size() const {
#ifndef NDEBUG
    check_consistency();
#endif
    return num_rows;
}

MutableColumnPtr ColumnObject::clone_resized(size_t new_size) const {
    /// cloneResized with new_size == 0 is used for cloneEmpty().
    if (new_size != 0) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "ColumnObject doesn't support resize to non-zero length");
    }
    return ColumnObject::create(is_nullable);
}

size_t ColumnObject::byte_size() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.byteSize();
    }
    return res;
}

size_t ColumnObject::allocated_bytes() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.allocatedBytes();
    }
    return res;
}

void ColumnObject::for_each_subcolumn(ColumnCallback callback) {
    if (!is_finalized()) {
        assert(false);
    }
    for (auto& entry : subcolumns) {
        callback(entry->data.data.back());
    }
}

void ColumnObject::try_insert_from(const IColumn& src, size_t n) {
    return try_insert(src[n]);
}

void ColumnObject::try_insert(const Field& field) {
    const auto& object = field.get<const VariantMap&>();
    phmap::flat_hash_set<std::string> inserted;
    size_t old_size = size();
    for (const auto& [key_str, value] : object) {
        PathInData key(key_str);
        inserted.insert(key_str);
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
        if (!inserted.contains(entry->path.get_path())) {
            entry->data.insertDefault();
        }
    }
    ++num_rows;
}

void ColumnObject::insert_default() {
    for (auto& entry : subcolumns) {
        entry->data.insertDefault();
    }
    ++num_rows;
}

Field ColumnObject::operator[](size_t n) const {
    if (!is_finalized()) {
        assert(false);
    }
    VariantMap map;
    for (const auto& entry : subcolumns) {
        map[entry->path.get_path()] = (*entry->data.data.back())[n];
    }
    return map;
}

void ColumnObject::get(size_t n, Field& res) const {
    if (!is_finalized()) {
        assert(false);
    }
    auto& map = res.get<VariantMap&>();
    for (const auto& entry : subcolumns) {
        auto it = map.try_emplace(entry->path.get_path()).first;
        entry->data.data.back()->get(n, it->second);
    }
}

Status ColumnObject::try_insert_indices_from(const IColumn& src, const int* indices_begin,
                                             const int* indices_end) {
    for (auto x = indices_begin; x != indices_end; ++x) {
        if (*x == -1) {
            ColumnObject::insert_default();
        } else {
            ColumnObject::try_insert_from(src, *x);
        }
    }
    finalize();
    return Status::OK();
}

void ColumnObject::try_insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& src_object = assert_cast<const ColumnObject&>(src);
    if (UNLIKELY(src_object.empty())) {
        return;
    }
    for (auto& entry : subcolumns) {
        if (src_object.has_subcolumn(entry->path)) {
            auto* subcolumn = src_object.get_subcolumn(entry->path);
            if (!subcolumn) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to find sub column {}", entry->path.get_path());
            }
            entry->data.insertRangeFrom(*subcolumn, start, length);
        } else {
            entry->data.insertManyDefaults(length);
        }
    }
    for (const auto& entry : src_object.subcolumns) {
        if (!has_subcolumn(entry->path)) {
            bool succ = false;
            if (entry->path.has_nested_part()) {
                const auto& base_type = entry->data.get_least_common_typeBase();
                FieldInfo field_info {
                        .scalar_type = base_type,
                        .have_nulls = base_type->is_nullable(),
                        .need_convert = false,
                        .num_dimensions = entry->data.get_dimensions(),
                };
                succ = add_nested_subcolumn(entry->path, field_info, num_rows);
            } else {
                succ = add_sub_column(entry->path, num_rows);
            }
            if (!succ) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to add column {}", entry->path.get_path());
            }
            auto* subcolumn = get_subcolumn(entry->path);
            if (!subcolumn) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to find sub column {}", entry->path.get_path());
            }
            subcolumn->insertRangeFrom(entry->data, start, length);
        }
    }
    num_rows += length;
    finalize();
}

void ColumnObject::pop_back(size_t length) {
    for (auto& entry : subcolumns) {
        entry->data.pop_back(length);
    }
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

bool ColumnObject::add_sub_column(const PathInData& key, MutableColumnPtr&& subcolumn) {
    size_t new_size = subcolumn->size();
    bool inserted = subcolumns.add(key, Subcolumn(std::move(subcolumn), is_nullable));
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

bool ColumnObject::add_nested_subcolumn(const PathInData& key, const FieldInfo& field_info,
                                        size_t new_size) {
    assert(key.has_nested_part());
    bool inserted = false;
    /// We find node that represents the same Nested type as @key.
    const auto* nested_node = subcolumns.find_best_match(key);
    if (nested_node) {
        /// Find any leaf of Nested subcolumn.
        const auto* leaf = doris::vectorized::ColumnObject::Subcolumns::find_leaf(
                nested_node, [&](const auto&) { return true; });
        assert(leaf);
        /// Recreate subcolumn with default values and the same sizes of arrays.
        auto new_subcolumn = leaf->data.recreate_with_default_values(field_info);
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
        VLOG_DEBUG << "Subcolumn already exists";
        return false;
    }
    if (num_rows == 0) {
        num_rows = new_size;
    }
    return true;
}

PathsInData ColumnObject::getKeys() const {
    PathsInData keys;
    keys.reserve(subcolumns.size());
    for (const auto& entry : subcolumns) {
        keys.emplace_back(entry->path);
    }
    return keys;
}

void ColumnObject::remove_subcolumns(const std::unordered_set<std::string>& keys) {
    Subcolumns new_subcolumns;
    for (auto& entry : subcolumns) {
        if (keys.count(entry->path.get_path()) == 0) {
            new_subcolumns.add(entry->path, entry->data);
        }
    }
    std::swap(subcolumns, new_subcolumns);
}

bool ColumnObject::is_finalized() const {
    return std::all_of(subcolumns.begin(), subcolumns.end(),
                       [](const auto& entry) { return entry->data.is_finalized(); });
}

void ColumnObject::finalize() {
    Subcolumns new_subcolumns;
    for (auto&& entry : subcolumns) {
        const auto& least_common_type = entry->data.get_least_common_type();
        /// Do not add subcolumns, which consists only from NULLs.
        if (is_nothing(getBaseTypeOfArray(least_common_type))) {
            continue;
        }
        if (!entry->data.data.empty()) {
            entry->data.finalize();
            new_subcolumns.add(entry->path, entry->data);
        }
    }
    /// If all subcolumns were skipped add a dummy subcolumn,
    /// because Tuple type must have at least one element.
    // if (new_subcolumns.empty()) {
    //     new_subcolumns.add(
    //             PathInData {COLUMN_NAME_DUMMY},
    //             Subcolumn {static_cast<MutableColumnPtr&&>(ColumnUInt8::create(old_size, 0)),
    //                        is_nullable});
    // }
    std::swap(subcolumns, new_subcolumns);
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

void ColumnObject::strip_outer_array() {
    assert(is_finalized());
    Subcolumns new_subcolumns;
    for (auto&& entry : subcolumns) {
        auto base_column = get_base_column_of_array(entry->data.get_finalized_column_ptr());
        new_subcolumns.add(entry->path, Subcolumn {base_column->assume_mutable(), is_nullable});
        num_rows = base_column->size();
    }
    /// If all subcolumns were skipped add a dummy subcolumn,
    /// because Tuple type must have at least one element.
    // if (new_subcolumns.empty()) {
    //     new_subcolumns.add(
    //             PathInData {COLUMN_NAME_DUMMY},
    //             Subcolumn {static_cast<MutableColumnPtr&&>(ColumnUInt8::create(old_size, 0)),
    //                        is_nullable});
    // }
    std::swap(subcolumns, new_subcolumns);
}

ColumnPtr ColumnObject::filter(const Filter& filter, ssize_t count) const {
    DCHECK(is_finalized());
    auto new_column = ColumnObject::create(true);
    for (auto& entry : subcolumns) {
        auto subcolumn = entry->data.get_finalized_column().filter(filter, count);
        new_column->add_sub_column(entry->path, std::move(subcolumn));
    }
    return new_column;
}

size_t ColumnObject::filter(const Filter& filter) {
    DCHECK(is_finalized());
    for (auto& entry : subcolumns) {
        num_rows = entry->data.get_finalized_column().filter(filter);
    }
    return num_rows;
}

void ColumnObject::revise_to(int target_num_rows) {
    for (auto&& entry : subcolumns) {
        if (entry->data.size() > target_num_rows) {
            entry->data.pop_back(entry->data.size() - target_num_rows);
        }
    }
    num_rows = target_num_rows;
}

template <typename ColumnInserterFn>
void align_variant_by_name_and_type(ColumnObject& dst, const ColumnObject& src, size_t row_cnt,
                                    ColumnInserterFn inserter) {
    CHECK(dst.is_finalized() && src.is_finalized());
    // Use rows() here instead of size(), since size() will check_consistency
    // but we could not check_consistency since num_rows will be upgraded even
    // if src and dst is empty, we just increase the num_rows of dst and fill
    // num_rows of default values when meet new data
    size_t num_rows = dst.rows();
    for (auto& entry : dst.get_subcolumns()) {
        const auto* src_subcol = src.get_subcolumn(entry->path);
        if (src_subcol == nullptr) {
            entry->data.get_finalized_column().insert_many_defaults(row_cnt);
        } else {
            // TODO handle type confict here， like ColumnObject before
            CHECK(entry->data.get_least_common_type()->equals(
                    *src_subcol->get_least_common_type()));
            const auto& src_column = src_subcol->get_finalized_column();
            inserter(src_column, &entry->data.get_finalized_column());
        }
        dst.set_num_rows(entry->data.get_finalized_column().size());
    }
    for (const auto& entry : src.get_subcolumns()) {
        // encounter a new column
        const auto* dst_subcol = dst.get_subcolumn(entry->path);
        if (dst_subcol == nullptr) {
            auto type = entry->data.get_least_common_type();
            auto new_column = type->create_column();
            new_column->insert_many_defaults(num_rows);
            inserter(entry->data.get_finalized_column(), new_column.get());
            dst.set_num_rows(new_column->size());
            dst.add_sub_column(entry->path, std::move(new_column));
        }
    }
    num_rows += row_cnt;
    if (dst.empty()) {
        dst.incr_num_rows(row_cnt);
    }
#ifndef NDEBUG
    // Check all columns rows matched
    for (const auto& entry : dst.get_subcolumns()) {
        DCHECK_EQ(entry->data.get_finalized_column().size(), num_rows);
    }
#endif
}

void ColumnObject::insert_range_from(const IColumn& src, size_t start, size_t length) {
    // insert_range_from with alignment
    const ColumnObject& src_column = *check_and_get_column<ColumnObject>(src);
    align_variant_by_name_and_type(*this, src_column, length,
                                   [start, length](const IColumn& src, IColumn* dst) {
                                       dst->insert_range_from(src, start, length);
                                   });
}

void ColumnObject::append_data_by_selector(MutableColumnPtr& res,
                                           const IColumn::Selector& selector) const {
    // append by selector with alignment
    ColumnObject& dst_column = *assert_cast<ColumnObject*>(res.get());
    align_variant_by_name_and_type(dst_column, *this, selector.size(),
                                   [&selector](const IColumn& src, IColumn* dst) {
                                       auto mutable_dst = dst->assume_mutable();
                                       src.append_data_by_selector(mutable_dst, selector);
                                   });
}

void ColumnObject::insert_indices_from(const IColumn& src, const int* indices_begin,
                                       const int* indices_end) {
    // insert_indices_from with alignment
    const ColumnObject& src_column = *check_and_get_column<ColumnObject>(src);
    align_variant_by_name_and_type(*this, src_column, indices_end - indices_begin,
                                   [indices_begin, indices_end](const IColumn& src, IColumn* dst) {
                                       dst->insert_indices_from(src, indices_begin, indices_end);
                                   });
}

void ColumnObject::insert_indices_from_join(const IColumn& src, const uint32_t* indices_begin,
                                            const uint32_t* indices_end) {
    // insert_indices_from with alignment
    const ColumnObject& src_column = *check_and_get_column<ColumnObject>(src);
    align_variant_by_name_and_type(*this, src_column, indices_end - indices_begin,
                                   [indices_begin, indices_end](const IColumn& src, IColumn* dst) {
                                       dst->insert_indices_from_join(src, indices_begin,
                                                                     indices_end);
                                   });
}

} // namespace doris::vectorized
