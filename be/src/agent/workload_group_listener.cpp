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

#include "agent/workload_group_listener.h"

#include "runtime/task_group/task_group.h"
#include "runtime/task_group/task_group_manager.h"
#include "util/mem_info.h"
#include "util/parse_util.h"

namespace doris {

void WorkloadGroupListener::handle_topic_info(const TPublishTopicRequest& topic_request) {
    std::set<uint64_t> current_wg_ids;
    for (const TopicInfo& topic_info : topic_request.topic_list) {
        if (topic_info.topic_type != doris::TTopicInfoType::type::WORKLOAD_GROUP) {
            continue;
        }

        int wg_id = 0;
        auto iter2 = topic_info.info_map.find("id");
        std::from_chars(iter2->second.c_str(), iter2->second.c_str() + iter2->second.size(), wg_id);

        current_wg_ids.insert(wg_id);
    }

    _exec_env->task_group_manager()->delete_task_group_by_ids(current_wg_ids);
    LOG(INFO) << "finish update workload group info, size=" << topic_request.topic_list.size();
}
} // namespace doris