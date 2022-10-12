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

#include "vec/exec/scan/new_odbc_scan_node.h"

#include "vec/exec/scan/new_odbc_scanner.h"

static const std::string NEW_SCAN_NODE_TYPE = "NewOdbcScanNode";

namespace doris::vectorized {

NewOdbcScanNode::NewOdbcScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs),
          _table_name(tnode.jdbc_scan_node.table_name),
          _odbc_scan_node(tnode.odbc_scan_node) {
    _output_tuple_id = tnode.odbc_scan_node.tuple_id;
}

std::string NewOdbcScanNode::get_name() {
    return fmt::format("VNewOdbcScanNode({0})", _table_name);
}

Status NewOdbcScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << NEW_SCAN_NODE_TYPE << "::prepare";
    RETURN_IF_ERROR(VScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());
    return Status::OK();
}

Status NewOdbcScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    return Status::OK();
}

Status NewOdbcScanNode::_init_scanners(std::list<VScanner*>* scanners) {
    if (_eos == true) {
        return Status::OK();
    }
    NewOdbcScanner* scanner = new NewOdbcScanner(_state, this, _limit_per_scanner, _odbc_scan_node);
    _scanner_pool.add(scanner);
    RETURN_IF_ERROR(scanner->prepare(_state));
    scanners->push_back(static_cast<VScanner*>(scanner));
    return Status::OK();
}
} // namespace doris::vectorized
