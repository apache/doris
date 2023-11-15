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

#include "vmeta_scan_node.h"

#include <memory>

#include "common/object_pool.h"
#include "vmeta_scanner.h"

namespace doris {
class DescriptorTbl;
class RuntimeState;
namespace vectorized {
class VScanner;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

VMetaScanNode::VMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs),
          _tuple_id(tnode.meta_scan_node.tuple_id),
          _scan_params(tnode.meta_scan_node) {
    _output_tuple_id = _tuple_id;
    if (_scan_params.__isset.current_user_ident) {
        _user_identity = _scan_params.current_user_ident;
    }
}

Status VMetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::init(tnode, state));
    return Status::OK();
}

Status VMetaScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::prepare(state));
    return Status::OK();
}

void VMetaScanNode::set_scan_ranges(RuntimeState* state,
                                    const std::vector<TScanRangeParams>& scan_ranges) {
    _scan_ranges = scan_ranges;
}

Status VMetaScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    return Status::OK();
}

Status VMetaScanNode::_init_scanners(std::list<VScannerSPtr>* scanners) {
    if (_eos == true) {
        return Status::OK();
    }

    for (auto& scan_range : _scan_ranges) {
        std::shared_ptr<VMetaScanner> scanner =
                VMetaScanner::create_shared(_state, this, _tuple_id, scan_range, _limit_per_scanner,
                                            runtime_profile(), _user_identity);
        RETURN_IF_ERROR(scanner->prepare(_state, _conjuncts));
        scanners->push_back(scanner);
    }
    return Status::OK();
}

Status VMetaScanNode::_process_conjuncts() {
    return Status::OK();
}

} // namespace doris::vectorized
