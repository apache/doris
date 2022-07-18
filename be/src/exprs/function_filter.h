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

#include "udf/udf.h"
#include "udf/udf_internal.h"

namespace doris {

class FunctionFilter {
public:
    FunctionFilter(bool opposite, const std::string& col_name, doris_udf::FunctionContext* fn_ctx,
                   doris_udf::StringVal string_param)
            : _opposite(opposite),
              _col_name(col_name),
              _fn_ctx(fn_ctx),
              _string_param(string_param) {}

    bool _opposite;
    std::string _col_name;
    // these pointer's life time controlled by scan node
    doris_udf::FunctionContext* _fn_ctx;
    doris_udf::StringVal
            _string_param; // only one param from conjunct, because now only support like predicate
};

} // namespace doris
