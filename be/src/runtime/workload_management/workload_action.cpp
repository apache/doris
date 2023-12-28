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

#include "runtime/workload_management/workload_action.h"

#include "runtime/fragment_mgr.h"
#include "runtime/task_group/task_group_manager.h"

namespace doris {

void WorkloadActionCancelQuery::exec(WorkloadQueryInfo* query_info) {
    LOG(INFO) << "[workload_schedule]workload scheduler cancel query " << query_info->query_id;
    ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
            query_info->tquery_id, PPlanFragmentCancelReason::INTERNAL_ERROR,
            std::string("query canceled by workload scheduler"));
}

void WorkloadActionMoveQuery::exec(WorkloadQueryInfo* query_info) {
    QueryContext* query_ctx_ptr = query_info->_query_ctx_share_ptr.get();

    taskgroup::TaskGroup* cur_task_group = query_ctx_ptr->get_task_group();
    // todo(wb) maybe we can support move a query with no group to a group
    if (!cur_task_group) {
        LOG(INFO) << "[workload_schedule] no workload group for current query "
                  << query_info->query_id << " skip move action";
        return;
    }
    uint64_t cur_group_id = cur_task_group->id();
    std::shared_ptr<taskgroup::TaskGroup> dst_group = nullptr;
    bool move_mem_ret =
            ExecEnv::GetInstance()->task_group_manager()->migrate_memory_tracker_to_group(
                    query_ctx_ptr->query_mem_tracker, cur_group_id, _wg_id, &dst_group);

    bool move_cpu_ret =
            ExecEnv::GetInstance()->task_group_manager()->set_cg_task_sche_for_query_ctx(
                    _wg_id, query_ctx_ptr);

    if (move_mem_ret && move_cpu_ret) {
        query_ctx_ptr->set_task_group(dst_group);
        LOG(INFO) << "[workload_schedule]move query " << query_info->query_id << " from "
                  << cur_group_id << " to " << _wg_id << " success";
    } else {
        LOG(INFO) << "[workload_schedule]move query " << query_info->query_id << " from "
                  << cur_group_id << " to " << _wg_id
                  << " failed, move memory ret=" << ((int)move_mem_ret)
                  << ", move cpu ret=" << ((int)move_cpu_ret);
    }
};

} // namespace doris