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

#include "util/array_parser.h"

namespace doris {
std::unordered_map<FunctionContext::Type, PrimitiveType> ArrayParser::_types_mapping = {
        {FunctionContext::INVALID_TYPE, PrimitiveType::INVALID_TYPE},
        {FunctionContext::TYPE_NULL, PrimitiveType::TYPE_NULL},
        {FunctionContext::TYPE_BOOLEAN, PrimitiveType::TYPE_BOOLEAN},
        {FunctionContext::TYPE_TINYINT, PrimitiveType::TYPE_TINYINT},
        {FunctionContext::TYPE_SMALLINT, PrimitiveType::TYPE_SMALLINT},
        {FunctionContext::TYPE_INT, PrimitiveType::TYPE_INT},
        {FunctionContext::TYPE_BIGINT, PrimitiveType::TYPE_BIGINT},
        {FunctionContext::TYPE_LARGEINT, PrimitiveType::TYPE_LARGEINT},
        {FunctionContext::TYPE_FLOAT, PrimitiveType::TYPE_FLOAT},
        {FunctionContext::TYPE_DOUBLE, PrimitiveType::TYPE_DOUBLE},
        {FunctionContext::TYPE_DATE, PrimitiveType::TYPE_DATE},
        {FunctionContext::TYPE_DATETIME, PrimitiveType::TYPE_DATETIME},
        {FunctionContext::TYPE_CHAR, PrimitiveType::TYPE_CHAR},
        {FunctionContext::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR},
        {FunctionContext::TYPE_HLL, PrimitiveType::TYPE_HLL},
        {FunctionContext::TYPE_STRING, PrimitiveType::TYPE_STRING},
        {FunctionContext::TYPE_DECIMALV2, PrimitiveType::TYPE_DECIMALV2},
        {FunctionContext::TYPE_OBJECT, PrimitiveType::TYPE_OBJECT},
        {FunctionContext::TYPE_ARRAY, PrimitiveType::TYPE_ARRAY},
};

}