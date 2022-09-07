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

#include "exec/exec_node.h"
#include "exec/scan_node.h"
#include "vec/exec/vjdbc_connector.h"
namespace doris {

namespace vectorized {
class VJdbcScanNode final : public ScanNode {
public:
    VJdbcScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

    ~VJdbcScanNode() override = default;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override {
        return Status::NotSupported("Not Implemented JdbcScanNode::get_next.");
    }

    Status get_next(RuntimeState* state, vectorized::Block* block, bool* eos) override;

    Status close(RuntimeState* state) override;

    // No use
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    std::string get_query_stmt(const std::string& table, const std::vector<std::string>& fields,
                               const std::vector<std::string>& filters, int64_t limit);

    bool _is_init;
    std::string _table_name;

    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;
    //SQL
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