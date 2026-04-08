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

#include "udf/python/python_udf_client.h"

#include "common/status.h"

namespace doris {

Status PythonUDFClient::create(const PythonUDFMeta& func_meta, ProcessPtr process,
                               PythonUDFClientPtr* client) {
    PythonUDFClientPtr python_udf_client = std::make_shared<PythonUDFClient>();
    RETURN_IF_ERROR(python_udf_client->init(func_meta, std::move(process)));
    *client = std::move(python_udf_client);
    return Status::OK();
}

Status PythonUDFClient::evaluate(const arrow::RecordBatch& input,
                                 std::shared_ptr<arrow::RecordBatch>* output) {
    RETURN_IF_ERROR(begin_stream(input.schema()));
    RETURN_IF_ERROR(write_batch(input));
    RETURN_IF_ERROR(read_batch(output));
    return Status::OK();
}

} // namespace doris