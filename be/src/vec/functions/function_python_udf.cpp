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

#include "vec/functions/function_python_udf.h"

#include <arrow/record_batch.h>
#include <arrow/type_fwd.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <ctime>
#include <memory>

#include "common/status.h"
#include "runtime/user_function_cache.h"
#include "udf/python/python_server.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udf_meta.h"
#include "util/arrow/block_convertor.h"
#include "util/arrow/row_batch.h"
#include "util/timezone_utils.h"
#include "vec/core/block.h"
#include "vec/exec/jni_connector.h"

namespace doris::vectorized {

PythonFunctionCall::PythonFunctionCall(const TFunction& fn, const DataTypes& argument_types,
                                       const DataTypePtr& return_type)
        : _fn(fn), _argument_types(argument_types), _return_type(return_type) {}

Status PythonFunctionCall::open(FunctionContext* context,
                                FunctionContext::FunctionStateScope scope) {
    if (scope == FunctionContext::FunctionStateScope::FRAGMENT_LOCAL) {
        LOG(INFO) << "Open python UDF fragment local";
        return Status::OK();
    }

    PythonVersion version;
    PythonUDFMeta func_meta;
    func_meta.id = _fn.id;
    func_meta.name = _fn.name.function_name;
    func_meta.symbol = _fn.scalar_fn.symbol;
    if (!_fn.function_code.empty()) {
        func_meta.type = PythonUDFLoadType::INLINE;
        func_meta.location = "inline";
        func_meta.inline_code = _fn.function_code;
    } else if (!_fn.hdfs_location.empty()) {
        func_meta.type = PythonUDFLoadType::MODULE;
        func_meta.location = _fn.hdfs_location;
        func_meta.checksum = _fn.checksum;
    } else {
        func_meta.type = PythonUDFLoadType::UNKNOWN;
        func_meta.location = "unknown";
    }

    func_meta.input_types = _argument_types;
    func_meta.return_type = _return_type;
    func_meta.client_type = PythonClientType::UDF;

    if (_fn.__isset.runtime_version && !_fn.runtime_version.empty()) {
        RETURN_IF_ERROR(
                PythonVersionManager::instance().get_version(_fn.runtime_version, &version));
    } else {
        return Status::InvalidArgument("Python UDF runtime version is not set");
    }

    func_meta.runtime_version = version.full_version;
    RETURN_IF_ERROR(func_meta.check());
    func_meta.always_nullable = _return_type->is_nullable();
    LOG(INFO) << fmt::format("runtime_version: {}, func_meta: {}", version.to_string(),
                             func_meta.to_string());

    if (func_meta.type == PythonUDFLoadType::MODULE) {
        RETURN_IF_ERROR(UserFunctionCache::instance()->get_pypath(
                func_meta.id, func_meta.location, func_meta.checksum, &func_meta.location));
    }

    PythonUDFClientPtr client = nullptr;
    RETURN_IF_ERROR(PythonServerManager::instance().get_client(func_meta, version, &client));

    if (!client) {
        return Status::InternalError("Python UDF client is null");
    }

    context->set_function_state(FunctionContext::THREAD_LOCAL, client);
    LOG(INFO) << fmt::format("Successfully get python UDF client, process: {}",
                             client->print_process());
    return Status::OK();
}

Status PythonFunctionCall::execute_impl(FunctionContext* context, Block& block,
                                        const ColumnNumbers& arguments, uint32_t result,
                                        size_t num_rows) const {
    auto client = reinterpret_cast<PythonUDFClient*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (!client) {
        LOG(WARNING) << "Python UDF client is null";
        return Status::InternalError("Python UDF client is null");
    }

    int64_t input_rows = block.rows();
    uint32_t input_columns = block.columns();
    DCHECK(input_columns > 0 && result < input_columns &&
           _argument_types.size() == arguments.size());
    vectorized::Block input_block;
    vectorized::Block output_block;

    if (!_return_type->equals(*block.get_by_position(result).type)) {
        return Status::InternalError(fmt::format("Python UDF output type {} not equal to {}",
                                                 block.get_by_position(result).type->get_name(),
                                                 _return_type->get_name()));
    }

    for (uint32_t i = 0; i < arguments.size(); ++i) {
        if (!_argument_types[i]->equals(*block.get_by_position(arguments[i]).type)) {
            return Status::InternalError(
                    fmt::format("Python UDF input type {} not equal to {}",
                                block.get_by_position(arguments[i]).type->get_name(),
                                _argument_types[i]->get_name()));
        }
        input_block.insert(block.get_by_position(arguments[i]));
    }

    std::shared_ptr<arrow::Schema> schema;
    RETURN_IF_ERROR(
            get_arrow_schema_from_block(input_block, &schema, TimezoneUtils::default_time_zone));
    std::shared_ptr<arrow::RecordBatch> input_batch;
    std::shared_ptr<arrow::RecordBatch> output_batch;
    cctz::time_zone _timezone_obj; // default UTC
    RETURN_IF_ERROR(convert_to_arrow_batch(input_block, schema, arrow::default_memory_pool(),
                                           &input_batch, _timezone_obj));
    RETURN_IF_ERROR(client->evaluate(*input_batch, &output_batch));
    int64_t output_rows = output_batch->num_rows();

    if (output_batch->num_columns() != 1) {
        return Status::InternalError(fmt::format("Python UDF output columns {} not equal to 1",
                                                 output_batch->num_columns()));
    }

    if (input_rows != output_rows) {
        return Status::InternalError(fmt::format(
                "Python UDF output rows {} not equal to input rows {}", output_rows, input_rows));
    }

    RETURN_IF_ERROR(
            convert_from_arrow_batch(output_batch, {_return_type}, &output_block, _timezone_obj));
    DCHECK_EQ(output_block.columns(), 1);
    block.replace_by_position(result, std::move(output_block.get_by_position(0).column));
    return Status::OK();
}

Status PythonFunctionCall::close(FunctionContext* context,
                                 FunctionContext::FunctionStateScope scope) {
    auto client = reinterpret_cast<PythonUDFClient*>(
            context->get_function_state(FunctionContext::THREAD_LOCAL));
    if (!client) {
        LOG(WARNING) << "Python UDF client is null";
        return Status::InternalError("Python UDF client is null");
    }
    RETURN_IF_ERROR(client->close());
    return Status::OK();
}

} // namespace doris::vectorized
