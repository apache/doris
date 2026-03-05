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
#include <stdint.h>

#include <map>
#include <memory>
#include <string>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "util/runtime_profile.h"
#include "vec/exec/scan/scanner.h"

namespace doris {
class RuntimeState;
class TupleDescriptor;

namespace vectorized {
class Block;
class VExprContext;
class JdbcJniReader;

/**
 * JdbcScanner is the pipeline-level scanner for JDBC data sources.
 *
 * In the transitional phase (Phase 3), it delegates to JdbcJniReader internally,
 * which routes through the unified JniConnector → JdbcJniScanner (Java) path.
 *
 * This keeps FE unchanged (still uses TJdbcScanNode), while the actual
 * data reading is unified with the JniReader/GenericReader framework.
 *
 * In Phase 4, this class will be eliminated and JDBC will flow through
 * FileScanner directly.
 */
class JdbcScanner : public Scanner {
    ENABLE_FACTORY_CREATOR(JdbcScanner);

public:
    friend class JdbcJniReader;

    JdbcScanner(RuntimeState* state, doris::pipeline::JDBCScanLocalState* parent, int64_t limit,
                const TupleId& tuple_id, const std::string& query_string,
                TOdbcTableType::type table_type, bool is_tvf, RuntimeProfile* profile);
    Status _open_impl(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status init(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

private:
    // Build JDBC params from TupleDescriptor for JdbcJniReader
    std::map<std::string, std::string> _build_jdbc_params(const TupleDescriptor* tuple_desc);

    // Convert TOdbcTableType enum to string for JdbcTypeHandlerFactory
    static std::string _odbc_table_type_to_string(TOdbcTableType::type type) {
        switch (type) {
        case TOdbcTableType::MYSQL:
            return "MYSQL";
        case TOdbcTableType::ORACLE:
            return "ORACLE";
        case TOdbcTableType::POSTGRESQL:
            return "POSTGRESQL";
        case TOdbcTableType::SQLSERVER:
            return "SQLSERVER";
        case TOdbcTableType::CLICKHOUSE:
            return "CLICKHOUSE";
        case TOdbcTableType::SAP_HANA:
            return "SAP_HANA";
        case TOdbcTableType::TRINO:
            return "TRINO";
        case TOdbcTableType::PRESTO:
            return "PRESTO";
        case TOdbcTableType::OCEANBASE:
            return "OCEANBASE";
        case TOdbcTableType::OCEANBASE_ORACLE:
            return "OCEANBASE_ORACLE";
        case TOdbcTableType::DB2:
            return "DB2";
        case TOdbcTableType::GBASE:
            return "GBASE";
        default:
            return "MYSQL";
        }
    }

    bool _jdbc_eos;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    // SQL
    std::string _query_string;
    // Descriptor of tuples read from JDBC table.
    const TupleDescriptor* _tuple_desc = nullptr;
    // the sql query database type: like mysql, PG..
    TOdbcTableType::type _table_type;
    bool _is_tvf;
    // Unified JNI reader
    std::unique_ptr<JdbcJniReader> _jni_reader;
};
} // namespace vectorized
} // namespace doris
