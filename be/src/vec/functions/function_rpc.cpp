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

#include <brpc/controller.h>
#include <fmt/format.h>
#include <gen_cpp/function_service.pb.h>
#include <gen_cpp/types.pb.h>

#include <algorithm>
#include <memory>
#include <utility>

#include "runtime/exec_env.h"
#include "util/brpc_client_cache.h"
#include "vec/columns/column.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {

RPCFnImpl::RPCFnImpl(const TFunction& fn) : _fn(fn) {
    _function_name = _fn.scalar_fn.symbol;
    _server_addr = _fn.hdfs_location;
    _client = ExecEnv::GetInstance()->brpc_function_client_cache()->get_client(_server_addr);
    _signature = fmt::format("{}: [{}/{}]", _fn.name.function_name, _fn.hdfs_location,
                             _fn.scalar_fn.symbol);
}

Status RPCFnImpl::vec_call(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           size_t result, size_t input_rows_count) {
    PFunctionCallRequest request;
    PFunctionCallResponse response;
    request.set_function_name(_function_name);
    _convert_block_to_proto(block, arguments, input_rows_count, &request);
    brpc::Controller cntl;
    _client->fn_call(&cntl, &request, &response, nullptr);
    if (cntl.Failed()) {
        return Status::InternalError("call to rpc function {} failed: {}", _signature,
                                     cntl.ErrorText());
    }
    if (!response.has_status() || response.result_size() == 0) {
        return Status::InternalError("call rpc function {} failed: status or result is not set.",
                                     _signature);
    }
    if (response.status().status_code() != 0) {
        return Status::InternalError("call to rpc function {} failed: {}", _signature,
                                     response.status().DebugString());
    }
    _convert_to_block(block, response.result(0), result);
    return Status::OK();
}

void RPCFnImpl::_convert_block_to_proto(Block& block, const ColumnNumbers& arguments,
                                        size_t input_rows_count, PFunctionCallRequest* request) {
    size_t row_count = std::min(block.rows(), input_rows_count);
    for (size_t col_idx : arguments) {
        PValues* arg = request->add_args();
        ColumnWithTypeAndName& column = block.get_by_position(col_idx);
        arg->set_has_null(column.column->has_null(row_count));
        auto col = column.column->convert_to_full_column_if_const();
        static_cast<void>(column.type->get_serde()->write_column_to_pb(*col, *arg, 0, row_count));
    }
}

void RPCFnImpl::_convert_to_block(Block& block, const PValues& result, size_t pos) {
    auto data_type = block.get_data_type(pos);
    auto col = data_type->create_column();
    auto serde = data_type->get_serde();
    static_cast<void>(serde->read_column_from_pb(*col, result));
    block.replace_by_position(pos, std::move(col));
}

FunctionRPC::FunctionRPC(const TFunction& fn, const DataTypes& argument_types,
                         const DataTypePtr& return_type)
        : _argument_types(argument_types), _return_type(return_type), _tfn(fn) {}

Status FunctionRPC::open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FRAGMENT_LOCAL) {
        std::shared_ptr<RPCFnImpl> fn = std::make_shared<RPCFnImpl>(_tfn);
        if (!fn->available()) {
            return Status::InternalError("rpc env init error");
        }
        context->set_function_state(FunctionContext::FRAGMENT_LOCAL, fn);
    }
    return Status::OK();
}

Status FunctionRPC::execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                            size_t result, size_t input_rows_count, bool dry_run) {
    RPCFnImpl* fn = reinterpret_cast<RPCFnImpl*>(
            context->get_function_state(FunctionContext::FRAGMENT_LOCAL));
    return fn->vec_call(context, block, arguments, result, input_rows_count);
}
} // namespace doris::vectorized
