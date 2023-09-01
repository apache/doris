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

#include "service/arrow_flight/flight_sql_statement_batch_reader.h"

#include <arrow/status.h>

#include "arrow/builder.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"

namespace doris {
namespace flight {

std::shared_ptr<arrow::Schema> DorisStatementBatchReader::schema() const {
    return schema_;
}

DorisStatementBatchReader::DorisStatementBatchReader(std::shared_ptr<DorisStatement> statement,
                                                     std::shared_ptr<arrow::Schema> schema)
        : statement_(std::move(statement)), schema_(std::move(schema)) {}

arrow::Result<std::shared_ptr<DorisStatementBatchReader>> DorisStatementBatchReader::Create(
        const std::shared_ptr<DorisStatement>& statement_) {
    // 要确保fe先向be发fragment并创建完 buffer control block 后，再返回给client，才能正确找到 row_descriptor 和 control block
    RowDescriptor row_desc =
            ExecEnv::GetInstance()->result_mgr()->find_row_descriptor(statement_->query_id);
    if (row_desc.equals(RowDescriptor())) {
        ARROW_RETURN_IF_ERROR(Status::InternalError(fmt::format(
                "Schema RowDescriptor Not Found, queryid: {}", print_id(statement_->query_id))));
    }
    std::shared_ptr<arrow::Schema> schema;
    ARROW_RETURN_IF_ERROR(convert_to_arrow_schema(row_desc, &schema));
    std::shared_ptr<DorisStatementBatchReader> result(
            new DorisStatementBatchReader(statement_, schema));
    return result;
}

arrow::Status DorisStatementBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    CHECK(*out == nullptr);
    ARROW_RETURN_IF_ERROR(
            ExecEnv::GetInstance()->result_mgr()->fetch_data(statement_->query_id, out));
    return arrow::Status::OK();
}

} // namespace flight
} // namespace doris
