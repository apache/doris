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
#ifdef LIBJVM

#include "runtime/runtime_state.h"
#include "vec/exec/scan/new_jdbc_scan_node.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/exec/vjdbc_connector.h"
namespace doris {
namespace vectorized {
class NewJdbcScanner : public VScanner {
public:
    NewJdbcScanner(RuntimeState* state, NewJdbcScanNode* parent, int64_t limit, TupleId tuple_id,
                   std::string query_string);

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

public:
    Status prepare(RuntimeState* state);

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

private:
    bool _is_init;

    bool _jdbc_eos;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    // SQL
    std::string _query_string;
    // Descriptor of tuples read from JDBC table.
    const TupleDescriptor* _tuple_desc;
    // Scanner of JDBC.
    std::unique_ptr<JdbcConnector> _jdbc_connector;
    JdbcConnectorParam _jdbc_param;
};
} // namespace vectorized
} // namespace doris
#endif
