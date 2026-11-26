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

#include "exprs/function/cast/cast_to_uuid.h"

namespace doris::CastWrapper {

WrapperType create_uuid_wrapper(const CastOptions& options, const DataTypePtr& from_type) {
    if (!check_and_get_data_type<DataTypeString>(from_type.get())) {
        return create_unsupport_wrapper(from_type->get_name(), "UUID");
    }

    std::shared_ptr<CastToBase> cast;
    if (options.strict_mode) {
        cast = std::make_shared<
                CastToImpl<CastModeType::StrictMode, DataTypeString, DataTypeUUID>>();
    } else {
        cast = std::make_shared<
                CastToImpl<CastModeType::NonStrictMode, DataTypeString, DataTypeUUID>>();
    }
    return [cast = std::move(cast)](RuntimeState* state, const ColumnWithTypeAndName& source_column,
                                    ColumnWithTypeAndName& target_column, size_t input_rows_count,
                                    const NullMap::value_type* null_map = nullptr) {
        return cast->execute_impl(state, source_column, target_column, input_rows_count, null_map);
    };
}

} // namespace doris::CastWrapper
