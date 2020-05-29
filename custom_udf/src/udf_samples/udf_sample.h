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

#include "udf.h"

namespace doris_udf {

IntVal AddUdf(FunctionContext* context, const IntVal& arg1, const IntVal& arg2);

/// --- Prepare / Close Functions ---
/// ---------------------------------

/// The UDF can optionally include a prepare function. The prepare function is called
/// before any calls to the UDF to evaluate values.
void AddUdfPrepare(FunctionContext* context, FunctionContext::FunctionStateScope scope);

/// The UDF can also optionally include a close function. The close function is called 
/// after all calls to the UDF have completed.
void AddUdfClose(FunctionContext* context, FunctionContext::FunctionStateScope scope);

}
