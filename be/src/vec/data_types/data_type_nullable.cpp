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

#include "vec/data_types/data_type_nullable.h"

#include "common/logging.h"
#include "gen_cpp/data.pb.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_nothing.h"

namespace doris::vectorized {

DataTypeNullable::DataTypeNullable(const DataTypePtr& nested_data_type_)
        : nested_data_type{nested_data_type_} {
    if (!nested_data_type->can_be_inside_nullable()) {
        LOG(FATAL) << fmt::format("Nested type {} cannot be inside Nullable type",
                                  nested_data_type->get_name());
    }
}

bool DataTypeNullable::only_null() const {
    return typeid_cast<const DataTypeNothing*>(nested_data_type.get());
}

std::string DataTypeNullable::to_string(const IColumn& column, size_t row_num) const {
    const ColumnNullable& col =
            assert_cast<const ColumnNullable&>(*column.convert_to_full_column_if_const().get());

    if (col.is_null_at(row_num)) {
        return "\\N";
    } else {
        return nested_data_type->to_string(col.get_nested_column(), row_num);
    }
}

size_t DataTypeNullable::serialize(const IColumn& column, PColumn* pcolumn) const {
    auto ptr = column.convert_to_full_column_if_const();
    const ColumnNullable& col = assert_cast<const ColumnNullable&>(*ptr.get());
    pcolumn->mutable_is_null()->Reserve(column.size());

    for (size_t i = 0; i < column.size(); ++i) {
        bool is_null = col.is_null_at(i);
        pcolumn->add_is_null(is_null);
    }

    return nested_data_type->serialize(col.get_nested_column(), pcolumn) +
           sizeof(bool) * column.size();
}

void DataTypeNullable::deserialize(const PColumn& pcolumn, IColumn* column) const {
    ColumnNullable* col = assert_cast<ColumnNullable*>(column);
    col->get_null_map_data().reserve(pcolumn.is_null_size());

    for (int i = 0; i < pcolumn.is_null_size(); ++i) {
        if (pcolumn.is_null(i)) {
            col->get_null_map_data().push_back(1);
        } else {
            col->get_null_map_data().push_back(0);
        }
    }
    IColumn& nested = col->get_nested_column();
    nested_data_type->deserialize(pcolumn, &nested);
}

MutableColumnPtr DataTypeNullable::create_column() const {
    return ColumnNullable::create(nested_data_type->create_column(), ColumnUInt8::create());
}

Field DataTypeNullable::get_default() const {
    return Null();
}

size_t DataTypeNullable::get_size_of_value_in_memory() const {
    LOG(FATAL) << fmt::format("Value of type {} in memory is not of fixed size.", get_name());
    return 0;
}

bool DataTypeNullable::equals(const IDataType& rhs) const {
    return rhs.is_nullable() &&
           nested_data_type->equals(*static_cast<const DataTypeNullable&>(rhs).nested_data_type);
}

DataTypePtr make_nullable(const DataTypePtr& type) {
    if (type->is_nullable()) return type;
    return std::make_shared<DataTypeNullable>(type);
}

DataTypePtr remove_nullable(const DataTypePtr& type) {
    if (type->is_nullable()) return static_cast<const DataTypeNullable&>(*type).get_nested_type();
    return type;
}

} // namespace doris::vectorized
