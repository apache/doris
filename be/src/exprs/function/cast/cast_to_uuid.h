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

#include "core/column/column_nullable.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_uuid.h"
#include "exprs/function/cast/cast_base.h"

namespace doris {

struct CastToUUID {
    static bool from_string(const StringRef& from, UUIDValueType& to, CastParameters&) {
        return UUIDValue::from_string(to, from.data, from.size);
    }
};

template <CastModeType Mode>
class CastToImpl<Mode, DataTypeString, DataTypeUUID> : public CastToBase {
public:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count,
                        const NullMap::value_type* null_map = nullptr) const override {
        const auto& column_from =
                assert_cast<const ColumnString&>(*block.get_by_position(arguments[0]).column);
        const auto nested_to_type = remove_nullable(block.get_by_position(result).type);
        const auto serde = nested_to_type->get_serde();

        if constexpr (Mode == CastModeType::NonStrictMode) {
            auto column_to = create_empty_nullable_column(nested_to_type);
            RETURN_IF_ERROR(serde->from_string_batch(column_from, *column_to, {}));
            block.get_by_position(result).column = std::move(column_to);
        } else {
            auto column_to = nested_to_type->create_column();
            RETURN_IF_ERROR(
                    serde->from_string_strict_mode_batch(column_from, *column_to, {}, null_map));
            block.get_by_position(result).column = std::move(column_to);
        }
        return Status::OK();
    }
};

} // namespace doris
