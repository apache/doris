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

#include "exprs/function/cast/cast_base.h"

namespace doris::CastWrapper {

// Implemented in function_cast_int.cpp
WrapperType create_int_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                               PrimitiveType to_type);

// Implemented in function_cast_float.cpp
WrapperType create_float_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                 PrimitiveType to_type);

// Implemented in function_cast_decimal.cpp
WrapperType create_decimal_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                   PrimitiveType to_type);

// Implemented in function_cast_date.cpp (handles DATE / DATETIME / DATEv2 / DATETIMEv2 / TIMEv2)
WrapperType create_datelike_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                                    PrimitiveType to_type);

// Implemented in function_cast_date.cpp
WrapperType create_timestamptz_wrapper(FunctionContext* context, const DataTypePtr& from_type);

// Implemented in function_cast_ip.cpp
WrapperType create_ip_wrapper(FunctionContext* context, const DataTypePtr& from_type,
                              PrimitiveType to_type);

// Implemented in function_cast_bool.cpp
WrapperType create_boolean_wrapper(FunctionContext* context, const DataTypePtr& from_type);

// Implemented in function_cast_string.cpp
WrapperType create_string_wrapper(const DataTypePtr& from_type);

} // namespace doris::CastWrapper
