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

#include <gen_cpp/Types_types.h>

#include <memory>

#include "arrow/record_batch.h"

namespace doris {
namespace flight {

struct UpdateStatement {
public:
    TUniqueId query_id;
    std::string sql;

    UpdateStatement(const TUniqueId& query_id_, const std::string& sql_)
            : query_id(query_id_), sql(sql_) {}
};

class ArrowFlightUpdate {
public:
    static arrow::Result<std::shared_ptr<ArrowFlightUpdate>> Create(const std::string& sql);

  /// \brief Steps on underlying sqlite3_stmt.
  /// \return          The resulting return code from SQLite.
  arrow::Result<int> Step();

  /// \brief Executes an UPDATE, INSERT or DELETE statement.
  /// \return              The number of rows changed by execution.
  arrow::Result<int64_t> ExecuteUpdate();

  arrow::Status SetParameters(
    std::vector<std::shared_ptr<arrow::RecordBatch>> parameters);


// private:
//     std::shared_ptr<QueryStatement> statement_;
//     std::shared_ptr<arrow::Schema> schema_;

//     ArrowFlightBatchReader(std::shared_ptr<QueryStatement> statement,
//                            std::shared_ptr<arrow::Schema> schema);

    private:
        std::vector<std::shared_ptr<arrow::RecordBatch>> parameters_;
};

} // namespace flight
} // namespace doris
