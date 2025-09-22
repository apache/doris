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

#include "vec/aggregate_functions/aggregate_function_min_max_by.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
std::unique_ptr<MaxMinValueBase> create_max_min_value(const DataTypePtr& type) {
    switch (type->get_primitive_type()) {
    case PrimitiveType::TYPE_BOOLEAN:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_BOOLEAN>>>();
    case PrimitiveType::TYPE_TINYINT:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_TINYINT>>>();
    case PrimitiveType::TYPE_SMALLINT:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_SMALLINT>>>();
    case PrimitiveType::TYPE_INT:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_INT>>>();
    case PrimitiveType::TYPE_BIGINT:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_BIGINT>>>();
    case PrimitiveType::TYPE_LARGEINT:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_LARGEINT>>>();
    case PrimitiveType::TYPE_FLOAT:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_FLOAT>>>();
    case PrimitiveType::TYPE_DOUBLE:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_DOUBLE>>>();
    case PrimitiveType::TYPE_DECIMAL32:
        return std::make_unique<MaxMinValue<SingleValueDataDecimal<TYPE_DECIMAL32>>>();
    case PrimitiveType::TYPE_DECIMAL64:
        return std::make_unique<MaxMinValue<SingleValueDataDecimal<TYPE_DECIMAL64>>>();
    case PrimitiveType::TYPE_DECIMAL128I:
        return std::make_unique<MaxMinValue<SingleValueDataDecimal<TYPE_DECIMAL128I>>>();
    case PrimitiveType::TYPE_DECIMALV2:
        return std::make_unique<MaxMinValue<SingleValueDataDecimal<TYPE_DECIMALV2>>>();
    case PrimitiveType::TYPE_DECIMAL256:
        return std::make_unique<MaxMinValue<SingleValueDataDecimal<TYPE_DECIMAL256>>>();
    case PrimitiveType::TYPE_STRING:
    case PrimitiveType::TYPE_CHAR:
    case PrimitiveType::TYPE_VARCHAR:
        return std::make_unique<MaxMinValue<SingleValueDataString>>();
    case PrimitiveType::TYPE_DATE:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_DATE>>>();
    case PrimitiveType::TYPE_DATETIME:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_DATETIME>>>();
    case PrimitiveType::TYPE_DATEV2:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_DATEV2>>>();
    case PrimitiveType::TYPE_DATETIMEV2:
        return std::make_unique<MaxMinValue<SingleValueDataFixed<TYPE_DATETIMEV2>>>();
    case PrimitiveType::TYPE_BITMAP:
        return std::make_unique<MaxMinValue<BitmapValueData>>();
    default:
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Illegal type {} of argument of aggregate function min/max_by",
                               type->get_name());
        return nullptr;
    }
}

} // namespace doris::vectorized

#include "common/compile_check_end.h"
