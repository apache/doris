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

#include "vec/functions/function_rpc.h"

#include <fmt/format.h>

#include <memory>

#include "exprs/rpc_fn.h"

namespace doris::vectorized {
FunctionRPC::FunctionRPC(const TFunction& fn, const DataTypes& argument_types,
                         const DataTypePtr& return_type)
        : _argument_types(argument_types), _return_type(return_type), _tfn(fn) {}

Status FunctionRPC::prepare(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    _fn = std::make_unique<RPCFn>(_tfn, false);

    if (!_fn->avliable()) {
        return Status::InternalError("rpc env init error");
    }
    return Status::OK();
}

Status FunctionRPC::execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                            size_t result, size_t input_rows_count, bool dry_run) {
    return _fn->vec_call(context, block, arguments, result, input_rows_count);
}
} // namespace doris::vectorized
