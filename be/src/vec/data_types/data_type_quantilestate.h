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

#include "runtime/define_primitive_type.h"
#include "serde/data_type_quantilestate_serde.h"
#include "util/quantile_state.h"
#include "vec/columns/column_complex.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris {
namespace vectorized {
class BufferReadable;
class BufferWritable;
class IColumn;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
class DataTypeQuantileState : public IDataType {
public:
    DataTypeQuantileState() = default;
    ~DataTypeQuantileState() override = default;
    using ColumnType = ColumnQuantileState;
    using FieldType = QuantileState;

    std::string do_get_name() const override { return get_family_name(); }
    const char* get_family_name() const override { return "QuantileState"; }

    TypeIndex get_type_id() const override { return TypeIndex::QuantileState; }
    TypeDescriptor get_type_as_type_descriptor() const override {
        return TypeDescriptor(TYPE_QUANTILE_STATE);
    }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE;
    }
    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, IColumn* column, int be_exec_version) const override;

    MutableColumnPtr create_column() const override;

    bool get_is_parametric() const override { return false; }
    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return false; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return false; }
    bool is_value_represented_by_number() const override { return false; }
    bool is_value_represented_by_integer() const override { return false; }
    bool is_value_represented_by_unsigned_integer() const override { return false; }
    // TODO:
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }
    bool have_maximum_size_of_value() const override { return false; }

    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    bool can_be_inside_low_cardinality() const override { return false; }

    std::string to_string(const IColumn& column, size_t row_num) const override {
        return "QuantileState()";
    }
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;

    Field get_default() const override { return QuantileState(); }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        LOG(FATAL) << "Unimplemented get_field for quantilestate";
        __builtin_unreachable();
    }

    static void serialize_as_stream(const QuantileState& value, BufferWritable& buf);

    static void deserialize_as_stream(QuantileState& value, BufferReadable& buf);
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<DataTypeQuantileStateSerDe>(nesting_level);
    };
};
} // namespace doris::vectorized
