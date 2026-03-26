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
#include "exec/operator/jdbc_scan_operator.h"
#include "exec/scan/scanner.h"
#include "format/table/jdbc_jni_reader.h"
#include "runtime/runtime_profile.h"

namespace doris {
class RuntimeState;
class TupleDescriptor;

class Block;
class VExprContext;

/**
 * DEPRECATED: This class is transitional and should be removed once JDBC scanning
 * is fully integrated into the FileScanner path.
 *
 * JdbcScanner is the pipeline-level scanner for JDBC data sources.
 * It delegates to JdbcJniReader internally, which uses the unified
 * JniReader → JdbcJniScanner (Java) path for data reading.
 *
 * Prerequisites before deletion:
 * 1. FE: Change JdbcScanNode to generate FileScanNode plan with TFileFormatType::FORMAT_JDBC,
 *    so JDBC scans flow through FileScanner instead of JDBCScanLocalState → JdbcScanner.
 * 2. BE: Add FORMAT_JDBC case in FileScanner::_create_reader() to create JdbcJniReader
 *    (similar to Paimon/Hudi/MaxCompute/TrinoConnector).
 * 3. BE: Remove JDBCScanLocalState / jdbc_scan_operator.h/cpp which depend on this class.
 * 4. After the above, this file (jdbc_scanner.h/cpp) can be safely deleted.
 */
class JdbcScanner : public Scanner {
    ENABLE_FACTORY_CREATOR(JdbcScanner);

public:
    friend class JdbcJniReader;

    JdbcScanner(RuntimeState* state, doris::JDBCScanLocalState* parent, int64_t limit,
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
} // namespace doris
