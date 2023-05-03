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

#include "vec/exec/scan/new_jdbc_scan_node.h"

#include <fmt/format.h>
#include <gen_cpp/PlanNodes_types.h>

#include <memory>
#include <ostream>

#include "common/logging.h"
#include "common/object_pool.h"
#include "runtime/runtime_state.h"
#include "vec/exec/scan/new_jdbc_scanner.h"

namespace doris {
class DescriptorTbl;
namespace vectorized {
class VScanner;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
NewJdbcScanNode::NewJdbcScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs),
          _table_name(tnode.jdbc_scan_node.table_name),
          _tuple_id(tnode.jdbc_scan_node.tuple_id),
          _query_string(tnode.jdbc_scan_node.query_string),
          _table_type(tnode.jdbc_scan_node.table_type) {
    _output_tuple_id = tnode.jdbc_scan_node.tuple_id;
}

std::string NewJdbcScanNode::get_name() {
    return fmt::format("VNewJdbcScanNode({0})", _table_name);
}

Status NewJdbcScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VNewJdbcScanNode::Prepare";
    RETURN_IF_ERROR(VScanNode::prepare(state));
    return Status::OK();
}

Status NewJdbcScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    return Status::OK();
}

Status NewJdbcScanNode::_init_scanners(std::list<VScannerSPtr>* scanners) {
    if (_eos == true) {
        return Status::OK();
    }
    std::unique_ptr<NewJdbcScanner> scanner =
            NewJdbcScanner::create_unique(_state, this, _limit_per_scanner, _tuple_id,
                                          _query_string, _table_type, _state->runtime_profile());
    RETURN_IF_ERROR(scanner->prepare(_state, _vconjunct_ctx_ptr.get()));
    scanners->push_back(std::move(scanner));
    return Status::OK();
}
} // namespace doris::vectorized
