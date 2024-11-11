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

#include <cctz/time_zone.h>
#include <gen_cpp/Types_types.h>

#include <memory>
#include <utility>

#include "arrow/record_batch.h"
#include "runtime/exec_env.h"

namespace doris {

namespace vectorized {
class Block;
} // namespace vectorized

namespace flight {

struct QueryStatement {
public:
    TUniqueId query_id;
    TNetworkAddress result_addr; // BE brpc ip & port
    std::string sql;

    QueryStatement(TUniqueId query_id_, TNetworkAddress result_addr_, std::string sql_)
            : query_id(std::move(query_id_)),
              result_addr(std::move(result_addr_)),
              sql(std::move(sql_)) {}
};

class ArrowFlightBatchReaderBase : public arrow::RecordBatchReader {
public:
    // RecordBatchReader force override
    [[nodiscard]] std::shared_ptr<arrow::Schema> schema() const override;

protected:
    ArrowFlightBatchReaderBase(const std::shared_ptr<QueryStatement>& statement);
    ~ArrowFlightBatchReaderBase() override;
    arrow::Status _return_invalid_status(const std::string& msg);

    std::shared_ptr<QueryStatement> _statement;
    std::shared_ptr<arrow::Schema> _schema;
    cctz::time_zone _timezone_obj;
    std::atomic<int64_t> _packet_seq = 0;

    std::atomic<int64_t> _convert_arrow_batch_timer = 0;
    std::atomic<int64_t> _deserialize_block_timer = 0;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
};

class ArrowFlightBatchLocalReader : public ArrowFlightBatchReaderBase {
public:
    static arrow::Result<std::shared_ptr<ArrowFlightBatchLocalReader>> Create(
            const std::shared_ptr<QueryStatement>& statement);

    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
    ArrowFlightBatchLocalReader(const std::shared_ptr<QueryStatement>& statement,
                                const std::shared_ptr<arrow::Schema>& schema,
                                const std::shared_ptr<MemTrackerLimiter>& mem_tracker);
};

class ArrowFlightBatchRemoteReader : public ArrowFlightBatchReaderBase {
public:
    static arrow::Result<std::shared_ptr<ArrowFlightBatchRemoteReader>> Create(
            const std::shared_ptr<QueryStatement>& statement);

    // create arrow RecordBatchReader must initialize the schema.
    // so when creating arrow RecordBatchReader, fetch result data once,
    // which will return Block and some necessary information, and extract arrow schema from Block.
    arrow::Status init_schema();
    arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* out) override;

private:
    ArrowFlightBatchRemoteReader(const std::shared_ptr<QueryStatement>& statement,
                                 const std::shared_ptr<PBackendService_Stub>& stub);

    arrow::Status _fetch_schema();
    arrow::Status _fetch_data();

    std::shared_ptr<PBackendService_Stub> _brpc_stub = nullptr;
    std::once_flag _timezone_once_flag;
    std::shared_ptr<vectorized::Block> _block;
};

} // namespace flight

} // namespace doris
