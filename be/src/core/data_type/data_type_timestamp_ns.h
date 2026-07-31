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
#include "core/value/timestamp_ns_value.h"

namespace doris {

class DataTypeTimeStampNs final : public DataTypeNumberBase<PrimitiveType::TYPE_TIMESTAMP_NS> {
public:
    const std::string get_family_name() const override { return "TimeStampNs"; }

    bool equals(const IDataType& rhs) const override;

    using SerDeType = DataTypeTimeStampNsSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    }

    Field get_field(const TExprNode& node) const override;
    UInt32 get_scale() const override { return TimeStampNsValue::FRACTIONAL_DIGITS; }
};

} // namespace doris
