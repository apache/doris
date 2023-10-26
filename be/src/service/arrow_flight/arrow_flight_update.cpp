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

#include "service/arrow_flight/arrow_flight_update.h"

#include <arrow/status.h>

#include "arrow/builder.h"
#include "runtime/exec_env.h"
#include "runtime/result_buffer_mgr.h"
#include "util/arrow/row_batch.h"
#include "util/arrow/utils.h"

namespace doris {
namespace flight {


arrow::Result<std::shared_ptr<ArrowFlightUpdate>> ArrowFlightUpdate::Create(const std::string& sql) {


        std::cout << "sql is " << sql << std::endl;

  std::shared_ptr<ArrowFlightUpdate> result(new ArrowFlightUpdate());
  return result;
}

arrow::Result<int64_t> ArrowFlightUpdate::ExecuteUpdate() {
     std::cout << "ExecuteUpdate" << std::endl;
     return 1;
//   return sqlite3_changes(db_);
}

arrow::Status ArrowFlightUpdate::SetParameters(
    std::vector<std::shared_ptr<arrow::RecordBatch>> parameters) {
  parameters_ = std::move(parameters);
  auto end = std::remove_if(
      parameters_.begin(), parameters_.end(),
      [](const std::shared_ptr<arrow::RecordBatch>& batch) { return batch->num_rows() == 0; });
  parameters_.erase(end, parameters_.end());
  return arrow::Status::OK();
}


} // namespace flight
} // namespace doris
