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
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_number_base.h"
#include "core/data_type/define_primitive_type.h"
#include "core/data_type_serde/data_type_date_or_datetime_serde.h"
#include "core/types.h"

namespace doris {
#include "common/compile_check_begin.h"
class BufferWritable;
class IColumn;

class DataTypeDate final : public DataTypeNumberBase<PrimitiveType::TYPE_DATE> {
public:
    static constexpr PrimitiveType PType = TYPE_DATE;
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_DATE; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_DATE;
    }
    const std::string get_family_name() const override { return "Date"; }
    std::string do_get_name() const override { return "Date"; }

    bool equals(const IDataType& rhs) const override;
/// TODO: remove this in the future
#ifdef BE_TEST
    using IDataType::to_string;
    std::string to_string(VecDateTimeValue value) const {
        char buf[64];
        value.to_string(buf);
        return buf;
    }
#endif
    static void cast_to_date(VecDateTimeValue& x);
    Field get_field(const TExprNode& node) const override;

    MutableColumnPtr create_column() const override;

    using SerDeType = DataTypeDateSerDe<TYPE_DATE>;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }
};
#include "common/compile_check_end.h"
} // namespace doris
