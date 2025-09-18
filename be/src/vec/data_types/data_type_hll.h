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
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <ostream>
#include <string>
#include <typeinfo>

#include "olap/hll.h"
#include "runtime/define_primitive_type.h"
#include "serde/data_type_hll_serde.h"
#include "vec/columns/column_complex.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {
class BufferReadable;
class BufferWritable;
class IColumn;

class DataTypeHLL : public IDataType {
public:
    DataTypeHLL() = default;
    ~DataTypeHLL() override = default;

    using ColumnType = ColumnHLL;
    using FieldType = doris::HyperLogLog;
    static constexpr PrimitiveType PType = TYPE_HLL;

    std::string do_get_name() const override { return get_family_name(); }
    const std::string get_family_name() const override { return "HLL"; }

    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_HLL; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_HLL;
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    std::string to_string(const IColumn& column, size_t row_num) const override { return "HLL()"; }
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    Field get_default() const override {
        return Field::create_field<TYPE_HLL>(HyperLogLog::empty());
    }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR, "Unimplemented get_field for HLL");
    }

    static void serialize_as_stream(const HyperLogLog& value, BufferWritable& buf);

    static void deserialize_as_stream(HyperLogLog& value, BufferReadable& buf);

    using SerDeType = DataTypeHLLSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    };
};

} // namespace doris::vectorized
