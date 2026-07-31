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

#include "core/data_type/data_type_timestamp_ns.h"

#include <gen_cpp/data.pb.h>

#include <cstdint>
#include <typeinfo>
#include <utility>

#include "common/exception.h"
#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "core/string_ref.h"
#include "core/value/timestamp_ns_value.h"

namespace doris {

Field DataTypeTimeStampNs::get_field(const TExprNode& node) const {
    int64_t value = 0;
    const StringRef string_value(node.date_literal.value.data(), node.date_literal.value.size());
    const auto status = parse_timestamp_ns(string_value, &value);
    if (!status.ok()) {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "Invalid value: {} for type TimeStampNs: {}",
                               node.date_literal.value, status.to_string());
    }
    return Field::create_field<TYPE_TIMESTAMP_NS>(TimeStampNsValue(value));
}

bool DataTypeTimeStampNs::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

void DataTypeTimeStampNs::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
}

FieldWithDataType DataTypeTimeStampNs::get_field_with_data_type(const IColumn& column,
                                                                size_t row_num) const {
    const auto& column_data =
            assert_cast<const ColumnTimeStampNs&, TypeCheckOnRelease::DISABLE>(column);
    Field field;
    column_data.get(row_num, field);
    return FieldWithDataType {.field = std::move(field),
                              .base_scalar_type_id = get_primitive_type(),
                              .precision = -1,
                              .scale = static_cast<int>(get_scale())};
}

} // namespace doris
