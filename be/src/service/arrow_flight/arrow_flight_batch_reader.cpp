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

#include "service/arrow_flight/arrow_flight_batch_reader.h"

#include <arrow/status.h>

#include "arrow/builder.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"

namespace doris {
namespace flight {

std::shared_ptr<arrow::Schema> ArrowFlightBatchReader::schema() const {
    return schema_;
}

ArrowFlightBatchReader::ArrowFlightBatchReader(std::shared_ptr<QueryStatement> statement,
                                               std::shared_ptr<arrow::Schema> schema)
        : statement_(std::move(statement)), schema_(std::move(schema)) {}

arrow::Result<std::shared_ptr<ArrowFlightBatchReader>> ArrowFlightBatchReader::Create(
        const std::shared_ptr<QueryStatement>& statement_) {
    // Make sure that FE send the fragment to BE and creates the BufferControlBlock before returning ticket
    // to the ADBC client, so that the row_descriptor and control block can be found.
    RowDescriptor row_desc =
            ExecEnv::GetInstance()->result_mgr()->find_row_descriptor(statement_->query_id);
    if (row_desc.equals(RowDescriptor())) {
        ARROW_RETURN_NOT_OK(arrow::Status::Invalid(fmt::format(
                "Schema RowDescriptor Not Found, queryid: {}", print_id(statement_->query_id))));
    }
    std::shared_ptr<arrow::Schema> schema;
    auto st = convert_to_arrow_schema(row_desc, &schema);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << st.to_string();
        ARROW_RETURN_NOT_OK(to_arrow_status(st));
    }
    std::shared_ptr<ArrowFlightBatchReader> result(new ArrowFlightBatchReader(statement_, schema));
    return result;
}

arrow::Status ArrowFlightBatchReader::ReadNext(std::shared_ptr<arrow::RecordBatch>* out) {
    // *out not nullptr
    *out = nullptr;
    auto st = ExecEnv::GetInstance()->result_mgr()->fetch_arrow_data(statement_->query_id, out);
    if (UNLIKELY(!st.ok())) {
        LOG(WARNING) << "ArrowFlightBatchReader fetch arrow data failed: " + st.to_string();
        ARROW_RETURN_NOT_OK(to_arrow_status(st));
    }
    if (*out != nullptr) {
        VLOG_NOTICE << "ArrowFlightBatchReader read next: " << (*out)->num_rows() << ", "
                    << (*out)->num_columns();
    }
    return arrow::Status::OK();
}

} // namespace flight
} // namespace doris
