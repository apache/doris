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

#include "udf/python/python_udtf_client.h"

#include "arrow/array/array_nested.h"
#include "arrow/array/array_primitive.h"
#include "arrow/record_batch.h"
#include "arrow/type.h"
#include "common/status.h"

namespace doris {

Status PythonUDTFClient::create(const PythonUDFMeta& func_meta, ProcessPtr process,
                                PythonUDTFClientPtr* client) {
    PythonUDTFClientPtr python_udtf_client = std::make_shared<PythonUDTFClient>();
    RETURN_IF_ERROR(python_udtf_client->init(func_meta, std::move(process)));
    *client = std::move(python_udtf_client);
    return Status::OK();
}

Status PythonUDTFClient::evaluate(const arrow::RecordBatch& input,
                                  std::shared_ptr<arrow::ListArray>* list_array) {
    RETURN_IF_ERROR(begin_stream(input.schema()));
    RETURN_IF_ERROR(write_batch(input));

    // Read the response (ListArray-based)
    std::shared_ptr<arrow::RecordBatch> response_batch;
    RETURN_IF_ERROR(read_batch(&response_batch));

    // Validate response structure: should have a single ListArray column
    if (response_batch->num_columns() != 1) {
        return Status::InternalError(
                fmt::format("Invalid UDTF response: expected 1 column (ListArray), got {}",
                            response_batch->num_columns()));
    }

    auto list_array_ptr = response_batch->column(0);
    if (list_array_ptr->type_id() != arrow::Type::LIST) {
        return Status::InternalError(
                fmt::format("Invalid UDTF response: expected ListArray, got type {}",
                            list_array_ptr->type()->ToString()));
    }

    *list_array = std::static_pointer_cast<arrow::ListArray>(list_array_ptr);
    return Status::OK();
}

} // namespace doris
