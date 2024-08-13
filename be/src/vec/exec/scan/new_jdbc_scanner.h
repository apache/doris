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

#include <memory>
#include <string>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "pipeline/exec/jdbc_scan_operator.h"
#include "util/runtime_profile.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/exec/vjdbc_connector.h"

namespace doris {
class RuntimeState;
class TupleDescriptor;

namespace vectorized {
class Block;
class VExprContext;

class NewJdbcScanner : public VScanner {
    ENABLE_FACTORY_CREATOR(NewJdbcScanner);

public:
    friend class JdbcConnector;

    NewJdbcScanner(RuntimeState* state, doris::pipeline::JDBCScanLocalState* parent, int64_t limit,
                   const TupleId& tuple_id, const std::string& query_string,
                   TOdbcTableType::type table_type, RuntimeProfile* profile);
    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;

    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eos) override;

    RuntimeProfile::Counter* _load_jar_timer = nullptr;
    RuntimeProfile::Counter* _init_connector_timer = nullptr;
    RuntimeProfile::Counter* _get_data_timer = nullptr;
    RuntimeProfile::Counter* _get_block_address_timer = nullptr;
    RuntimeProfile::Counter* _fill_block_timer = nullptr;
    RuntimeProfile::Counter* _check_type_timer = nullptr;
    RuntimeProfile::Counter* _execte_read_timer = nullptr;
    RuntimeProfile::Counter* _connector_close_timer = nullptr;

private:
    void _init_profile(const std::shared_ptr<RuntimeProfile>& profile);
    void _update_profile();

    bool _jdbc_eos;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    // SQL
    std::string _query_string;
    // Descriptor of tuples read from JDBC table.
    const TupleDescriptor* _tuple_desc = nullptr;
    // the sql query database type: like mysql, PG...
    TOdbcTableType::type _table_type;
    // Scanner of JDBC.
    std::unique_ptr<JdbcConnector> _jdbc_connector;
    JdbcConnectorParam _jdbc_param;
};
} // namespace vectorized
} // namespace doris
