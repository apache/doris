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

#pragma once

#include <memory>
#include <string>

#include "core/data_type/data_type_number_base.h"
#include "core/data_type_serde/data_type_timestamp_ns_serde.h"

namespace doris {

class DataTypeTimeStampNs final : public DataTypeNumberBase<PrimitiveType::TYPE_TIMESTAMP_NS> {
public:
    static constexpr PrimitiveType PType = TYPE_TIMESTAMP_NS;
    static constexpr bool is_parametric = false;
    static constexpr UInt32 SCALE = 9;

    PrimitiveType get_primitive_type() const override { return TYPE_TIMESTAMP_NS; }
    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS;
    }
    const std::string get_family_name() const override { return "TimeStampNs"; }
    std::string do_get_name() const override { return "TimeStampNs"; }

    bool equals(const IDataType& rhs) const override;
    bool equals_ignore_precision(const IDataType& rhs) const override {
        return rhs.get_primitive_type() == TYPE_TIMESTAMP_NS;
    }

    using SerDeType = DataTypeTimeStampNsSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }

    Field get_field(const TExprNode& node) const override;
    UInt32 get_scale() const override { return SCALE; }
    void to_pb_column_meta(PColumnMeta* col_meta) const override;
    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override;
};

} // namespace doris
