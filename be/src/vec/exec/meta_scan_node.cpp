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

#include "vec/exec/meta_scan_node.h"
#include "vec/exec/volap_scan_node.h"
namespace doris::vectorized {

MetaScanNode::MetaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _tuple_id(tnode.meta_scan_node.tuple_id),
          _meta_scan_node(tnode.meta_scan_node),
          _tuple_desc(nullptr),
          _slot_to_dict(tnode.meta_scan_node.slot_to_dict) {}

Status MetaScanNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "MetaScanNode::Open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));

    Status status = init_tablets();
    if (!status.ok() || _tablets.empty()) {
        return status;
    }

    assert(_tuple_desc->slots().size() == 1);
    auto slot = _tuple_desc->slots()[0];
    _return_column_id = _tablets[0]->field_index(slot->col_name());

    return Status::OK();
}

//for first call get_next(), return meta and eos=false;
//for later call return empty block and eos=true
Status MetaScanNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    if (get_next_done) {
        *eos = true;
        return Status::OK();
    }

    std::set<string> dict_words;
    //get local dict from tablets
    for (auto tablet : _tablets) {
        Status st = tablet->get_dict_data(dict_words, _return_column_id);
        if (!st.ok()) {
            return st;
        }
    }
    //convert local dict to string column
    const int default_dict_size = 256;
    assert(_tuple_desc->slots().size() == 1);
    //slot[0] is string column
    auto tmp_block = new Block(_tuple_desc->slots(), default_dict_size);
    auto col0 = std::move(*tmp_block->get_by_position(0).column).mutate(); //string column
    for (auto word : dict_words) {
        col0->insert_data(word.data(), word.size());
    }
    block->swap(*tmp_block);
    *eos = false;
    get_next_done = true;
    return Status::OK();
}

Status MetaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    return Status::OK();
}

Status MetaScanNode::init_tablets() {
    std::string err;
    for (int tablet_id : _tablet_ids) {
        auto tablet =
                StorageEngine::instance()->tablet_manager()->get_tablet(tablet_id, true, &err);
        if (tablet.get() == nullptr) {
            std::stringstream ss;
            ss << "failed to get tablet. tablet_id=" << tablet_id << err;
            LOG(WARNING) << ss.str();
            return Status::InternalError(ss.str());
        }

        _tablets.emplace_back(tablet);
    }
    return Status::OK();
}

Status MetaScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_SWITCH_TASK_THREAD_LOCAL_MEM_TRACKER(mem_tracker());

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    return Status::OK();
}

Status MetaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& scan_range : scan_ranges) {
        DCHECK(scan_range.scan_range.__isset.palo_scan_range);
        _tablet_ids.push_back(scan_range.scan_range.palo_scan_range.tablet_id);
    }
    return Status::OK();
}

Status MetaScanNode::close(RuntimeState* state) {
    return Status::OK();
}

} // namespace doris::vectorized