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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/global_types.h"
#include "common/status.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/scan/scanner.h"
#include "vec/exec/vjdbc_connector.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class TupleDescriptor;

struct DorisArrowConnectorParam {
    // use -1 as default value to find error earlier.
    int64_t catalog_id = -1;
    std::string user;
    std::string password;
    std::string location_uri;
    std::string ticket;
    const std::vector<std::string>* fe_arrow_nodes;

    const TupleDescriptor* tuple_desc = nullptr;
};

namespace vectorized {
class Block;
class VExprContext;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class NewDorisAdbcScanner : public Scanner {
    ENABLE_FACTORY_CREATOR(NewDorisAdbcScanner);

public:
    friend class JdbcConnector;

    NewDorisAdbcScanner(RuntimeState* state, pipeline::ScanLocalStateBase* local_state, int64_t limit,
                 TupleId tuple_id, const std::string& location_uri, const std::string& ticket,
                 RuntimeProfile* profile);

    Status open(RuntimeState* state) override;
    Status close(RuntimeState* state) override;
    Status prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) override;

protected:
    Status _get_block_impl(RuntimeState* state, Block* block, bool* eof) override;

private:
    bool _doris_eof;

    std::string _location_uri;
    std::string _ticket;

    TupleId _tuple_id;
    const TupleDescriptor* _tuple_desc = nullptr;

    DorisArrowConnectorParam _doris_arrow_param;

    // Scanner of JDBC(ADBC).
    std::unique_ptr<JdbcConnector> _jdbc_connector;
    JdbcConnectorParam _jdbc_param;
};
} // namespace doris::vectorized
