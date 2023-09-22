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
#include <fmt/format.h>
#include <sqltypes.h>
#include <stdint.h>
#include <stdlib.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/table_connector.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class TupleDescriptor;

struct ODBCConnectorParam {
    std::string connect_string;

    // only use in query
    std::string query_string;

    // only use in insert
    std::string table_name;
    bool use_transaction = false;
    const TupleDescriptor* tuple_desc;
};

// Because the DataBinding have the mem alloc, so
// this class should not be copyable
struct DataBinding {
    SQLSMALLINT target_type;
    SQLINTEGER buffer_length;
    SQLLEN strlen_or_ind;
    SQLPOINTER target_value_ptr;

    DataBinding() = default;

    ~DataBinding() { free(target_value_ptr); }
    DataBinding(const DataBinding&) = delete;
    DataBinding& operator=(const DataBinding&) = delete;
};

// ODBC Connector for scan data from ODBC
class ODBCConnector : public TableConnector {
public:
    explicit ODBCConnector(const ODBCConnectorParam& param);

    Status open(RuntimeState* state, bool read = false);
    // query for ODBC table
    Status query() override;

    Status get_next_row(bool* eos);

    Status exec_write_sql(const std::u16string& insert_stmt,
                          const fmt::memory_buffer& insert_stmt_buffer) override;

    Status exec_stmt_write(vectorized::Block* block,
                           const vectorized::VExprContextSPtrs& _output_vexpr_ctxs,
                           uint32_t* num_rows_sent) override {
        return Status::OK();
    }

    // use in ODBC transaction
    Status begin_trans() override; // should be call after connect and before query or init_to_write
    Status abort_trans() override; // should be call after transaction abort
    Status finish_trans() override; // should be call after transaction commit

    Status append(vectorized::Block* block, const vectorized::VExprContextSPtrs& _output_vexpr_ctxs,
                  uint32_t start_send_row, uint32_t* num_rows_sent,
                  TOdbcTableType::type table_type = TOdbcTableType::MYSQL) override;

    const DataBinding& get_column_data(int i) const { return *_columns_data.at(i).get(); }
    Status init_to_write(RuntimeProfile* profile) override;

    // Now we only treat HLL, CHAR, VARCHAR as big column
    uint32_t big_column_size_buffer = config::big_column_size_buffer;
    uint32_t small_column_size_buffer = config::small_column_size_buffer;

    Status close(Status) override;

private:
    static Status error_status(const std::string& prefix, const std::string& error_msg);

    static std::string handle_diagnostic_record(SQLHANDLE hHandle, SQLSMALLINT hType,
                                                RETCODE RetCode);

    std::string _connect_string;
    SQLSMALLINT _field_num;
    SQLHENV _env;
    SQLHDBC _dbc;
    SQLHSTMT _stmt;
    std::vector<std::unique_ptr<DataBinding>> _columns_data;
};

} // namespace doris
