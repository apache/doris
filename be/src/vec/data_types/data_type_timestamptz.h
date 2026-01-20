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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/DataTypeDateTime.h
// and modified by Doris

#pragma once

#include <gen_cpp/Types_types.h>

#include <boost/iterator/iterator_facade.hpp>
#include <memory>
#include <string>

#include "runtime/define_primitive_type.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number_base.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/data_types/serde/data_type_timestamptz_serde.h"

namespace doris::vectorized {

class DataTypeTimeStampTz final : public DataTypeNumberBase<PrimitiveType::TYPE_TIMESTAMPTZ> {
public:
    DataTypeTimeStampTz() = default;
    DataTypeTimeStampTz(UInt32 scale) : _scale(scale) {}
    bool equals(const IDataType& rhs) const override {
        return rhs.get_primitive_type() == PrimitiveType::TYPE_TIMESTAMPTZ &&
               _scale == rhs.get_scale();
    }
    bool equals_ignore_precision(const IDataType& rhs) const override {
        return rhs.get_primitive_type() == PrimitiveType::TYPE_TIMESTAMPTZ;
    }
    MutableColumnPtr create_column() const override { return ColumnType::create(); }

    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeTimeStampTzSerDe>(_scale, nesting_level);
    };
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_TIMESTAMPTZ; }
    const std::string get_family_name() const override { return "TimeStampTz"; }
    std::string do_get_name() const override {
        return "TimeStampTz(" + std::to_string(_scale) + ")";
    }

    void to_pb_column_meta(PColumnMeta* col_meta) const override {
        DataTypeNumberBase<PrimitiveType::TYPE_TIMESTAMPTZ>::to_pb_column_meta(col_meta);
        col_meta->mutable_decimal_param()->set_scale(_scale);
    }

    UInt32 get_scale() const override { return _scale; }

    Field get_field(const TExprNode& node) const override;

private:
    const UInt32 _scale = 6; // Default precision is 6
};

} // namespace doris::vectorized
