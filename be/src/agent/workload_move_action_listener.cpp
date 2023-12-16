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

#include "agent/workload_move_action_listener.h"

namespace doris {

void WorkloadMoveActionListener::handle_topic_info(const std::vector<TopicInfo>& topic_info_list) {
    for (const TopicInfo& topic_info : topic_info_list) {
        if (!topic_info.__isset.move_action) {
            continue;
        }
        FragmentMgr* fmgr = _exec_env->fragment_mgr();

        TUniqueId query_id = topic_info.move_action.query_id;
        uint64_t dst_group_id = topic_info.move_action.workload_group_id;

        std::shared_ptr<QueryContext> query_ctx_ptr = nullptr;
        fmgr->get_query_ctx_by_query_id(query_id, &query_ctx_ptr);
        if (!query_ctx_ptr) {
            continue;
        }

        if (query_ctx_ptr->is_cancelled()) {
            continue;
        }

        // 1 move memory tracker
        std::shared_ptr<taskgroup::TaskGroup> current_group =
                query_ctx_ptr->get_task_group_shared_ptr();
        if (!current_group) {
            continue;
        }
        std::shared_ptr<taskgroup::TaskGroup> dst_group_ptr = nullptr;
        bool move_mem_tracker_ret =
                _exec_env->task_group_manager()->migrate_memory_tracker_to_group(
                        query_ctx_ptr->query_mem_tracker, current_group->id(), dst_group_id,
                        &dst_group_ptr);
        if (move_mem_tracker_ret) {
            query_ctx_ptr->set_task_group(dst_group_ptr);
        }

        // 2 move exec/scan task
        bool move_task_ret = _exec_env->task_group_manager()->set_cg_task_sche_for_query_ctx(
                dst_group_id, query_ctx_ptr.get());

        LOG(INFO) << "try move query " << print_id(query_id) << " to group " << dst_group_id
                  << " , move memory result=" << ((int)move_mem_tracker_ret)
                  << ", move cpu result=" << ((int)move_task_ret);
    }
}

} // namespace doris