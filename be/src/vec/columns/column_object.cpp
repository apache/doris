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
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>

#include "common/exception.h"
#include "common/status.h"
#include "olap/olap_common.h"
#include "util/simd/bits.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/columns_number.h"
#include "vec/common/field_visitors.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/convert_field_to_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/get_least_supertype.h"

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/logging.h"
#include "exprs/json_functions.h"
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

DataTypePtr create_array_of_type(DataTypePtr type, size_t num_dimensions, bool is_nullable) {
    const DataTypeNullable* nullable = typeid_cast<const DataTypeNullable*>(type.get());
    if ((nullable &&
         typeid_cast<const ColumnObject::MostCommonType*>(nullable->get_nested_type().get())) ||
        typeid_cast<const ColumnObject::MostCommonType*>(type.get())) {
        // JSONB type MUST NOT wrapped in ARRAY column, it should be top level.
        // So we ignored num_dimensions.
        return type;
    }
    for (size_t i = 0; i < num_dimensions; ++i) {
        type = std::make_shared<DataTypeArray>(std::move(type));
        if (is_nullable) {
            // wrap array with nullable
            type = make_nullable(type);
        }
    }
    return type;
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
        get_least_supertype<LeastSupertypeOnError::Jsonb>(type_indexes, type);
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

ColumnObject::Subcolumn::Subcolumn(MutableColumnPtr&& data_, DataTypePtr type, bool is_nullable_,
                                   bool is_root_)
        : least_common_type(type), is_nullable(is_nullable_), is_root(is_root_) {
    data.push_back(std::move(data_));
    data_types.push_back(type);
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
}

void ColumnObject::Subcolumn::insert(Field field, FieldInfo info) {
    auto base_type = std::move(info.scalar_type);
    if (is_nothing(base_type)) {
        insertDefault();
        return;
    }
    auto column_dim = least_common_type.get_dimensions();
    auto value_dim = info.num_dimensions;
    if (is_nothing(least_common_type.get_base())) {
        column_dim = value_dim;
    }
    if (is_nothing(base_type)) {
        value_dim = column_dim;
    }
    bool type_changed = false;
    if (value_dim != column_dim || info.num_dimensions >= 2) {
        // Deduce to JSONB
        LOG(INFO) << fmt::format(
                "Dimension of types mismatched between inserted value and column, "
                "expected:{}, but meet:{} for type:{}",
                column_dim, value_dim, least_common_type.get()->get_name());
        base_type = std::make_shared<MostCommonType>();
        value_dim = 0;
        type_changed = true;
    }
    if (is_nullable && !is_nothing(base_type)) {
        base_type = make_nullable(base_type);
    }

    const auto& least_common_base_type = least_common_type.get_base();
    if (data.empty()) {
        add_new_column_part(create_array_of_type(std::move(base_type), value_dim, is_nullable));
    } else if (!least_common_base_type->equals(*base_type) && !is_nothing(base_type)) {
        if (!schema_util::is_conversion_required_between_integers(*base_type,
                                                                  *least_common_base_type)) {
            get_least_supertype<LeastSupertypeOnError::Jsonb>(
                    DataTypes {std::move(base_type), least_common_base_type}, &base_type);
            type_changed = true;
            if (is_nullable) {
                base_type = make_nullable(base_type);
            }
            if (!least_common_base_type->equals(*base_type)) {
                add_new_column_part(
                        create_array_of_type(std::move(base_type), value_dim, is_nullable));
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
    assert(start + length <= src.size());
    size_t end = start + length;
    // num_rows += length;
    if (data.empty()) {
        add_new_column_part(src.get_least_common_type());
    } else if (!least_common_type.get()->equals(*src.get_least_common_type())) {
        DataTypePtr new_least_common_type;
        get_least_supertype<LeastSupertypeOnError::Jsonb>(
                DataTypes {least_common_type.get(), src.get_least_common_type()},
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
        assert(from + n <= column->size());
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
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       st.to_string() + ", real_code:{}", st.code());
            }
            data.back()->insert_range_from(*casted_column, from, n);
            return;
        }
        auto casted_column = column->cut(from, n);
        Status st = schema_util::cast_column({casted_column, column_type, ""},
                                             least_common_type.get(), &casted_column);
        if (!st.ok()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string() + ", real_code:{}",
                                   st.code());
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
MutableColumnPtr ColumnObject::apply_for_subcolumns(Func&& func) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnObject&>(*finalized);
        return finalized_object.apply_for_subcolumns(std::forward<Func>(func));
    }
    auto res = ColumnObject::create(is_nullable, false);
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = func(subcolumn->data.get_finalized_column());
        res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable(),
                            subcolumn->data.get_least_common_type());
    }
    return res;
}
ColumnPtr ColumnObject::index(const IColumn& indexes, size_t limit) const {
    return apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.index(indexes, limit); });
}

bool ColumnObject::Subcolumn::check_if_sparse_column(size_t num_rows) {
    constexpr static size_t s_threshold_rows_estimate_sparse_column = 1000;
    if (num_rows < s_threshold_rows_estimate_sparse_column) {
        return false;
    }
    std::vector<double> defaults_ratio;
    for (size_t i = 0; i < data.size(); ++i) {
        defaults_ratio.push_back(data[i]->get_ratio_of_default_rows());
    }
    double default_ratio = std::accumulate(defaults_ratio.begin(), defaults_ratio.end(), 0.0) /
                           defaults_ratio.size();
    return default_ratio >= config::ratio_of_defaults_as_sparse_column;
}

void ColumnObject::Subcolumn::finalize() {
    if (is_finalized()) {
        return;
    }
    if (data.size() == 1 && num_of_defaults_in_prefix == 0) {
        data[0] = data[0]->convert_to_full_column_if_const();
        return;
    }
    DataTypePtr to_type = least_common_type.get();
    if (is_root) {
        // Root always JSONB type
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
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                                       st.to_string() + ", real_code:{}", st.code());
            }
            part = ptr;
        }
        result_column->insert_range_from(*part, 0, part_size);
    }
    data = {std::move(result_column)};
    data_types = {std::move(to_type)};
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
    size_t sz = data.size() - num_removed;
    data.resize(sz);
    data_types.resize(sz);
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

ColumnPtr& ColumnObject::Subcolumn::get_finalized_column_ptr() {
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
          base_type(get_base_type_of_array(type)),
          num_dimensions(get_number_of_dimensions(*type)) {
    if (!WhichDataType(type).is_nothing()) {
        least_common_type_serder = type->get_serde();
    }
}

ColumnObject::ColumnObject(bool is_nullable_, bool create_root_)
        : is_nullable(is_nullable_), num_rows(0) {
    if (create_root_) {
        subcolumns.create_root(Subcolumn(0, is_nullable, true /*root*/));
    }
}

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
    if (new_size == 0) {
        return ColumnObject::create(is_nullable);
    }
    return apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.clone_resized(new_size); });
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
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            callback(part);
        }
    }
}

void ColumnObject::try_insert_from(const IColumn& src, size_t n) {
    return try_insert(src[n]);
}

void ColumnObject::try_insert(const Field& field) {
    if (field.get_type() != Field::Types::VariantMap) {
        auto* root = get_subcolumn({});
        if (!root) {
            doris::Exception(doris::ErrorCode::INVALID_ARGUMENT, "Failed to find root column_path");
        }
        root->insert(field);
        ++num_rows;
        return;
    }
    const auto& object = field.get<const VariantMap&>();
    phmap::flat_hash_set<StringRef, StringRefHash> inserted;
    size_t old_size = size();
    for (const auto& [key_str, value] : object) {
        PathInData key;
        if (!key_str.empty()) {
            key = PathInData(key_str);
        }
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
        if (WhichDataType(remove_nullable(entry->data.data_types.back())).is_json()) {
            // JsonbFiled is special case
            Field f = JsonbField();
            (*entry->data.data.back()).get(n, f);
            map[entry->path.get_path()] = std::move(f);
            continue;
        }
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
        if (WhichDataType(remove_nullable(entry->data.data_types.back())).is_json()) {
            // JsonbFiled is special case
            it->second = JsonbField();
        }
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

FieldInfo ColumnObject::Subcolumn::get_subcolumn_field_info() const {
    const auto& base_type = least_common_type.get_base();
    return FieldInfo {
            .scalar_type = base_type,
            .have_nulls = base_type->is_nullable(),
            .need_convert = false,
            .num_dimensions = least_common_type.get_dimensions(),
    };
}

void ColumnObject::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& src_object = assert_cast<const ColumnObject&>(src);
    for (const auto& entry : src_object.subcolumns) {
        if (!has_subcolumn(entry->path)) {
            add_sub_column(entry->path, num_rows);
        }
        auto* subcolumn = get_subcolumn(entry->path);
        subcolumn->insertRangeFrom(entry->data, start, length);
    }
    for (auto& entry : subcolumns) {
        if (!src_object.has_subcolumn(entry->path)) {
            entry->data.insertManyDefaults(length);
        }
    }
    num_rows += length;
    finalize();
#ifndef NDEBUG
    check_consistency();
#endif
}

ColumnPtr ColumnObject::replicate(const Offsets& offsets) const {
    return apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.replicate(offsets); });
}

ColumnPtr ColumnObject::permute(const Permutation& perm, size_t limit) const {
    return apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.permute(perm, limit); });
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
    if (key.empty() && is_nothing(subcolumns.get_root()->data.get_least_common_type())) {
        // update root
        subcolumns.get_mutable_root()->data =
                Subcolumn(std::move(subcolumn), type, is_nullable, true);
        num_rows = new_size;
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

static bool check_if_valid_column_name(const PathInData& path) {
    static const std::regex COLUMN_NAME_REGEX("^[_a-zA-Z@0-9][.a-zA-Z0-9_+-/><?@#$%^&*]{0,255}$");
    return std::regex_match(path.get_path(), COLUMN_NAME_REGEX);
}

void ColumnObject::Subcolumn::wrapp_array_nullable() {
    // Wrap array with nullable, treat empty array as null to elimate conflict at present
    auto& result_column = get_finalized_column_ptr();
    if (result_column->is_column_array() && !result_column->is_nullable()) {
        auto new_null_map = ColumnUInt8::create();
        new_null_map->reserve(result_column->size());
        auto& null_map_data = new_null_map->get_data();
        auto array = static_cast<const ColumnArray*>(result_column.get());
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

void find_and_set_leave_value(const IColumn* column, const PathInData& path,
                              const DataTypeSerDeSPtr& type, rapidjson::Value& root,
                              rapidjson::Document::AllocatorType& allocator, int row) {
    const auto* nullable = assert_cast<const ColumnNullable*>(column);
    if (nullable->is_null_at(row)) {
        return;
    }
    // TODO could cache the result of leaf nodes with it's path info
    rapidjson::Value* target = find_leaf_node_by_path(root, path);
    if (UNLIKELY(!target)) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        root.Accept(writer);
        LOG(FATAL) << "could not find path " << path.get_path()
                   << ", root: " << std::string(buffer.GetString(), buffer.GetSize());
    }
    type->write_one_cell_to_json(*column, *target, allocator, row);
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
    for (auto it = node_root->children.begin(); it != node_root->children.end(); ++it) {
        auto child = it->get_second();
        rapidjson::Value value(rapidjson::kObjectType);
        get_json_by_column_tree(value, allocator, child.get());
        root.AddMember(rapidjson::StringRef(it->get_first().data, it->get_first().size), value,
                       allocator);
    }
}

bool ColumnObject::serialize_one_row_to_string(int row, std::string* output) const {
    CHECK(is_finalized());
    rapidjson::StringBuffer buf;
    bool res = serialize_one_row_to_json_format(row, &buf, nullptr);
    if (res) {
        // TODO avoid copy
        *output = std::string(buf.GetString(), buf.GetSize());
    }
    return res;
}

bool ColumnObject::serialize_one_row_to_string(int row, BufferWritable& output) const {
    CHECK(is_finalized());
    rapidjson::StringBuffer buf;
    bool res = serialize_one_row_to_json_format(row, &buf, nullptr);
    if (res) {
        output.write(buf.GetString(), buf.GetLength());
    }
    return res;
}

bool ColumnObject::serialize_one_row_to_json_format(int row, rapidjson::StringBuffer* output,
                                                    bool* is_null) const {
    CHECK(is_finalized());
    CHECK(size() > row);
    rapidjson::StringBuffer buffer;
    rapidjson::Value root(rapidjson::kNullType);
    if (doc_structure == nullptr) {
        doc_structure = std::make_shared<rapidjson::Document>();
        rapidjson::Document::AllocatorType& allocator = doc_structure->GetAllocator();
        get_json_by_column_tree(*doc_structure, allocator, subcolumns.get_root());
    }
    if (!doc_structure->IsNull()) {
        root.CopyFrom(*doc_structure, doc_structure->GetAllocator());
    }
#ifndef NDEBUG
    VLOG_DEBUG << "dump structure " << JsonFunctions::print_json_value(*doc_structure);
#endif
    for (const auto& subcolumn : subcolumns) {
        find_and_set_leave_value(subcolumn->data.get_finalized_column_ptr(), subcolumn->path,
                                 subcolumn->data.get_least_common_type_serde(), root,
                                 doc_structure->GetAllocator(), row);
    }
    compact_null_values(root, doc_structure->GetAllocator());
    if (root.IsNull() && is_null != nullptr) {
        // Fast path
        *is_null = true;
    } else {
        output->Clear();
        rapidjson::Writer<rapidjson::StringBuffer> writer(*output);
        return root.Accept(writer);
    }
    return true;
}

void ColumnObject::merge_sparse_to_root_column() {
    CHECK(is_finalized());
    if (sparse_columns.empty()) {
        return;
    }
    ColumnPtr src = subcolumns.get_mutable_root()->data.get_finalized_column_ptr();
    MutableColumnPtr mresult = src->clone_empty();
    const ColumnNullable* src_null = assert_cast<const ColumnNullable*>(src.get());
    const ColumnString* src_column_ptr =
            assert_cast<const ColumnString*>(&src_null->get_nested_column());
    rapidjson::StringBuffer buffer;
    doc_structure = std::make_shared<rapidjson::Document>();
    rapidjson::Document::AllocatorType& allocator = doc_structure->GetAllocator();
    get_json_by_column_tree(*doc_structure, allocator, sparse_columns.get_root());

#ifndef NDEBUG
    VLOG_DEBUG << "dump structure " << JsonFunctions::print_json_value(*doc_structure);
#endif

    ColumnNullable* result_column_nullable =
            assert_cast<ColumnNullable*>(mresult->assume_mutable().get());
    ColumnString* result_column_ptr =
            assert_cast<ColumnString*>(&result_column_nullable->get_nested_column());
    result_column_nullable->reserve(num_rows);
    // parse each row to jsonb
    for (size_t i = 0; i < num_rows; ++i) {
        // root is not null, store original value, eg. the root is scalar type like '[1]'
        if (!src_null->empty() && !src_null->is_null_at(i)) {
            result_column_ptr->insert_data(src_column_ptr->get_data_at(i).data,
                                           src_column_ptr->get_data_at(i).size);
            result_column_nullable->get_null_map_data().push_back(0);
            continue;
        }

        // parse and encode sparse columns
        buffer.Clear();
        rapidjson::Value root(rapidjson::kNullType);
        if (!doc_structure->IsNull()) {
            root.CopyFrom(*doc_structure, doc_structure->GetAllocator());
        }
        size_t null_count = 0;
        for (const auto& subcolumn : sparse_columns) {
            auto& column = subcolumn->data.get_finalized_column_ptr();
            if (assert_cast<const ColumnNullable&>(*column).is_null_at(i)) {
                ++null_count;
                continue;
            }
            find_and_set_leave_value(column, subcolumn->path,
                                     subcolumn->data.get_least_common_type_serde(), root,
                                     doc_structure->GetAllocator(), i);
        }

        // all null values, store null to sparse root
        if (null_count == sparse_columns.size()) {
            result_column_ptr->insert_default();
            result_column_nullable->get_null_map_data().push_back(1);
            continue;
        }

        // encode sparse columns into jsonb format
        compact_null_values(root, doc_structure->GetAllocator());
        // parse as jsonb value and put back to rootnode
        // TODO, we could convert to jsonb directly from rapidjson::Value for better performance, instead of parsing
        JsonbParser parser;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        root.Accept(writer);
        bool res = parser.parse(buffer.GetString(), buffer.GetSize());
        CHECK(res) << "buffer:" << std::string(buffer.GetString(), buffer.GetSize())
                   << ", row_num:" << i;
        result_column_ptr->insert_data(parser.getWriter().getOutput()->getBuffer(),
                                       parser.getWriter().getOutput()->getSize());
        result_column_nullable->get_null_map_data().push_back(0);
    }

    // assign merged column
    subcolumns.get_mutable_root()->data.get_finalized_column_ptr() = mresult->get_ptr();
}

void ColumnObject::finalize(bool ignore_sparse) {
    Subcolumns new_subcolumns;
    // finalize root first
    if (!ignore_sparse || !is_null_root()) {
        new_subcolumns.create_root(subcolumns.get_root()->data);
        new_subcolumns.get_mutable_root()->data.finalize();
    }
    for (auto&& entry : subcolumns) {
        if (entry->data.is_root) {
            continue;
        }
        const auto& least_common_type = entry->data.get_least_common_type();
        /// Do not add subcolumns, which consists only from NULLs
        if (is_nothing(get_base_type_of_array(least_common_type))) {
            continue;
        }
        entry->data.finalize();
        entry->data.wrapp_array_nullable();

        // Check and spilit sparse subcolumns
        if (!ignore_sparse && (entry->data.check_if_sparse_column(num_rows) ||
                               !check_if_valid_column_name(entry->path))) {
            // TODO seperate ambiguous path
            sparse_columns.add(entry->path, entry->data);
            continue;
        }

        new_subcolumns.add(entry->path, entry->data);
    }
    std::swap(subcolumns, new_subcolumns);
    doc_structure = nullptr;
}

void ColumnObject::finalize() {
    finalize(true);
}

void ColumnObject::ensure_root_node_type(const DataTypePtr& expected_root_type) {
    auto& root = subcolumns.get_mutable_root()->data;
    if (!root.get_least_common_type()->equals(*expected_root_type)) {
        // make sure the root type is alawys as expected
        ColumnPtr casted_column;
        schema_util::cast_column(ColumnWithTypeAndName {root.get_finalized_column_ptr(),
                                                        root.get_least_common_type(), ""},
                                 expected_root_type, &casted_column);
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

void ColumnObject::strip_outer_array() {
    assert(is_finalized());
    Subcolumns new_subcolumns;
    for (auto&& entry : subcolumns) {
        auto base_column = get_base_column_of_array(entry->data.get_finalized_column_ptr());
        new_subcolumns.add(entry->path, Subcolumn {base_column->assume_mutable(), is_nullable});
        num_rows = base_column->size();
    }
    std::swap(subcolumns, new_subcolumns);
}

ColumnPtr ColumnObject::filter(const Filter& filter, ssize_t count) const {
    DCHECK(is_finalized());
    auto new_column = ColumnObject::create(true, false);
    for (auto& entry : subcolumns) {
        auto subcolumn = entry->data.get_finalized_column().filter(filter, count);
        new_column->add_sub_column(entry->path, subcolumn->assume_mutable(),
                                   entry->data.get_least_common_type());
    }
    return new_column;
}

Status ColumnObject::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    if (!is_finalized()) {
        finalize();
    }
    auto* res = assert_cast<ColumnObject*>(col_ptr);
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = subcolumn->data.get_least_common_type()->create_column();
        RETURN_IF_ERROR(subcolumn->data.get_finalized_column().filter_by_selector(
                sel, sel_size, new_subcolumn.get()));
        res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable(),
                            subcolumn->data.get_least_common_type());
    }
    return Status::OK();
}

size_t ColumnObject::filter(const Filter& filter) {
    DCHECK(is_finalized());
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        for_each_subcolumn([](auto& part) { part->clear(); });
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
    }
    num_rows = count;
#ifndef NDEBUG
    check_consistency();
#endif
    return count;
}

void ColumnObject::clear() {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            part->clear();
        }
        entry->data.num_of_defaults_in_prefix = 0;
    }
    num_rows = 0;
}

void ColumnObject::revise_to(int target_num_rows) {
    for (auto&& entry : subcolumns) {
        if (entry->data.size() > target_num_rows) {
            entry->data.pop_back(entry->data.size() - target_num_rows);
        }
    }
    num_rows = target_num_rows;
}

void ColumnObject::create_root() {
    auto type = is_nullable ? make_nullable(std::make_shared<MostCommonType>())
                            : std::make_shared<MostCommonType>();
    add_sub_column({}, type->create_column(), type);
}

void ColumnObject::create_root(const DataTypePtr& type, MutableColumnPtr&& column) {
    add_sub_column({}, std::move(column), type);
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
    return !is_null_root() && subcolumns.get_leaves().size() == 1;
}

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

Status ColumnObject::extract_root(const PathInData& path) {
    SANITIZE_ROOT();
    if (!path.empty()) {
        MutableColumnPtr extracted;
        RETURN_IF_ERROR(schema_util::extract(subcolumns.get_root()->data.get_finalized_column_ptr(),
                                             path, extracted));
        subcolumns.get_mutable_root()->data.data[0] = extracted->get_ptr();
    }
    return Status::OK();
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
            // It's the first time alignment, so that we should build it
            if (entry->data.get_least_common_type()->get_type_id() == TypeIndex::Nothing) {
                entry->data.add_new_column_part(src_subcol->get_least_common_type());
            }
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

} // namespace doris::vectorized
