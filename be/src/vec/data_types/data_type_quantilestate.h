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

#include <glog/logging.h>

#include <memory>
#include <ostream>
#include <string>
#include <typeinfo>

#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "serde/data_type_quantilestate_serde.h"
#include "util/quantile_state.h"
#include "vec/columns/column_complex.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {
class BufferReadable;
class BufferWritable;
class IColumn;

class DataTypeQuantileState : public IDataType {
public:
    DataTypeQuantileState() = default;
    ~DataTypeQuantileState() override = default;
    using ColumnType = ColumnQuantileState;
    using FieldType = QuantileState;
    static constexpr PrimitiveType PType = TYPE_QUANTILE_STATE;

    std::string do_get_name() const override { return get_family_name(); }
    const std::string get_family_name() const override { return "QuantileState"; }
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_QUANTILE_STATE; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE;
    }
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    std::string to_string(const IColumn& column, size_t row_num) const override {
        return "QuantileState()";
    }
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    Field get_default() const override {
        return Field::create_field<TYPE_QUANTILE_STATE>(QuantileState());
    }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Unimplemented get_field for quantile state");
    }

    static void serialize_as_stream(const QuantileState& value, BufferWritable& buf);

    static void deserialize_as_stream(QuantileState& value, BufferReadable& buf);
    using SerDeType = DataTypeQuantileStateSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    };
};
} // namespace doris::vectorized
