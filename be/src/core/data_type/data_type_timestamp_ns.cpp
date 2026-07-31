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

#include <cstdint>
#include <typeinfo>

#include "common/exception.h"
#include "core/string_ref.h"

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

} // namespace doris
