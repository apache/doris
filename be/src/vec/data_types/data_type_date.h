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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDate.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>

#include <boost/iterator/iterator_facade.hpp>
#include <cstddef>
#include <string>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_date_or_datetime_serde.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class BufferWritable;
class IColumn;

class DataTypeDate final : public DataTypeNumberBase<PrimitiveType::TYPE_DATE> {
public:
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_DATE; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATE;
    }
    const std::string get_family_name() const override { return "Date"; }
    std::string do_get_name() const override { return "Date"; }

    bool equals(const IDataType& rhs) const override;
    std::string to_string(const IColumn& column, size_t row_num) const override;
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    void to_string_batch(const IColumn& column, ColumnString& column_to) const final {
        DataTypeNumberBase<PrimitiveType::TYPE_DATE>::template to_string_batch_impl<DataTypeDate>(
                column, column_to);
    }

    size_t number_length() const;
    void push_number(ColumnString::Chars& chars, const Int64& num) const;
    std::string to_string(Int64 int_val) const {
        doris::VecDateTimeValue value = binary_cast<Int64, doris::VecDateTimeValue>(int_val);
        char buf[64];
        value.to_string(buf);
        return buf;
    }
    static void cast_to_date(Int64& x);
    Field get_field(const TExprNode& node) const override {
        VecDateTimeValue value;
        if (value.from_date_str(node.date_literal.value.c_str(), node.date_literal.value.size())) {
            value.cast_to_date();
            return Field::create_field<TYPE_DATE>(Int64(*reinterpret_cast<__int64_t*>(&value)));
        } else {
            throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                   "Invalid value: {} for type Date", node.date_literal.value);
        }
    }

    MutableColumnPtr create_column() const override;

    using SerDeType = DataTypeDateSerDe<TYPE_DATE>;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
