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
#include "meta_scan_node.h"
namespace doris::vectorized {

VMetaScanNode::VMetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs),
          _tuple_id(tnode.meta_scan_node.tuple_id) {}

//std::string VMetaScanNode::get_name() {
//    return fmt::format("VMetaScanNode({0})", _table_name);
//}

Status VMetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::init(tnode, state));
    return Status::OK();
}

Status VMetaScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::prepare(state));
    return Status::OK();
}

void VMetaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {

}

Status VMetaScanNode::_init_profile() {
    return Status::OK();
}

Status VMetaScanNode::_init_scanners(std::list<VScanner*>* scanners) {
    return Status::OK();
}

} // namespace doris::vectorized

