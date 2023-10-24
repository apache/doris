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

#include <stdint.h>

#include <memory>
#include <string>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "exec/odbc_connector.h"
#include "vec/exec/scan/vscanner.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class TOdbcScanNode;
class TupleDescriptor;

namespace vectorized {
class Block;
class NewOdbcScanNode;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
class NewOdbcScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(NewOdbcScanner);

public:
    NewOdbcScanner(RuntimeState* state, NewOdbcScanNode* parent, int64_t limit,
                   const TOdbcScanNode& odbc_scan_node, RuntimeProfile* profile);

    Status open(RuntimeState* state) override;

    // Close the odbc_scanner, and report errors.
    Status close(RuntimeState* state) override;

public:
    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

private:
    bool _is_init;

    // Indicates whether there are more rows to process. Set in _odbc_connector.next().
    bool _odbc_eof;

    std::string _table_name;

    std::string _connect_string;

    std::string _query_string;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of tuples read from ODBC table.
    const TupleDescriptor* _tuple_desc;

    // Scanner of ODBC.
    std::unique_ptr<ODBCConnector> _odbc_connector;
    ODBCConnectorParam _odbc_param;
    // Helper class for converting text to other types;
    DataTypeSerDeSPtrs _text_serdes;
    DataTypeSerDe::FormatOptions _text_formatOptions;
};
} // namespace doris::vectorized
