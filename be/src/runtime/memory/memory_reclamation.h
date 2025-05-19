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

#include <gen_cpp/PaloInternalService_types.h>

#include "runtime/workload_management/memory_context.h"
#include "runtime/workload_management/resource_context.h"
#include "runtime/workload_management/task_controller.h"

namespace doris {
#include "common/compile_check_begin.h"

constexpr size_t SMALL_MEMORY_TASK = 32 * 1024 * 1024; // 32M

class MemoryReclamation {
public:
    enum class PriorityCmpFunc { TOP_MEMORY = 0, TOP_OVERCOMMITED_MEMORY = 1 };
    enum class FilterFunc {
        EXCLUDE_IS_SMALL = 0,
        EXCLUDE_IS_OVERCOMMITED = 1,
        IS_QUERY = 2,
        IS_LOAD = 3,
        IS_COMPACTION = 4
    };
    enum class ActionFunc { CANCEL = 0 };

    inline static std::unordered_map<PriorityCmpFunc,
                                     const std::function<int64_t(ResourceContext*)>>
            PriorityCmpFuncImpl = {
                    {PriorityCmpFunc::TOP_MEMORY,
                     [](ResourceContext* resource_ctx) {
                         return resource_ctx->memory_context()->current_memory_bytes();
                     }},
                    {PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY,
                     [](ResourceContext* resource_ctx) {
                         int64_t mem_limit = resource_ctx->memory_context()->mem_limit();
                         int64_t mem_size = resource_ctx->memory_context()->current_memory_bytes();
                         if (mem_limit <= 0 || mem_limit > mem_size) {
                             return static_cast<int64_t>(-1); // skip not overcommited task.
                         }
                         return static_cast<int64_t>(
                                 (static_cast<double>(mem_size) / static_cast<double>(mem_limit)) *
                                 1000000); // mem_size will not be greater than 9000G, so not overflow int64_t.
                     }},
    };

    static std::string priority_cmp_func_string(PriorityCmpFunc func) {
        switch (func) {
        case PriorityCmpFunc::TOP_MEMORY:
            return "Top Memory";
        case PriorityCmpFunc::TOP_OVERCOMMITED_MEMORY:
            return "Top Overcommited Memory";
        default:
            return "Error";
        }
    }

    inline static std::unordered_map<FilterFunc, const std::function<bool(ResourceContext*)>>
            FilterFuncImpl = {
                    {FilterFunc::EXCLUDE_IS_SMALL,
                     [](ResourceContext* resource_ctx) {
                         return resource_ctx->memory_context()->current_memory_bytes() >
                                SMALL_MEMORY_TASK;
                     }},
                    {FilterFunc::EXCLUDE_IS_OVERCOMMITED,
                     [](ResourceContext* resource_ctx) {
                         return resource_ctx->memory_context()->current_memory_bytes() <
                                resource_ctx->memory_context()->mem_limit();
                     }},
                    {FilterFunc::IS_QUERY,
                     [](ResourceContext* resource_ctx) {
                         return resource_ctx->task_controller()->query_type() == TQueryType::SELECT;
                     }},
                    {FilterFunc::IS_LOAD,
                     [](ResourceContext* resource_ctx) {
                         return resource_ctx->task_controller()->query_type() == TQueryType::LOAD;
                     }},
                    // TODO, FilterFunc::IS_COMPACTION, IS_SCHEMA_CHANGE, etc.
    };

    static std::string filter_func_string(std::vector<FilterFunc> func) {
        std::vector<std::string> func_strs;
        std::for_each(func.begin(), func.end(), [&](const auto& it) {
            switch (it) {
            case FilterFunc::EXCLUDE_IS_SMALL:
                func_strs.emplace_back("Exclude Is Small");
                break;
            case FilterFunc::EXCLUDE_IS_OVERCOMMITED:
                func_strs.emplace_back("Exclude Is Overcommited");
                break;
            case FilterFunc::IS_QUERY:
                func_strs.emplace_back("Is Query");
                break;
            case FilterFunc::IS_LOAD:
                func_strs.emplace_back("Is Load");
                break;
            case FilterFunc::IS_COMPACTION:
                func_strs.emplace_back("Is Compaction");
                break;
            default:
                func_strs.emplace_back("Error");
            }
        });
        return join(func_strs, ",");
    }

    inline static std::unordered_map<ActionFunc,
                                     const std::function<bool(ResourceContext*, const Status&)>>
            ActionFuncImpl = {
                    {ActionFunc::CANCEL,
                     [](ResourceContext* resource_ctx, const Status& reason) {
                         return resource_ctx->task_controller()->cancel(reason);
                     }},
                    // TODO, FilterFunc::SPILL, etc.
    };

    static std::string action_func_string(ActionFunc func) {
        switch (func) {
        case ActionFunc::CANCEL:
            return "Cancel";
        default:
            return "Error";
        }
    }

    static int64_t revoke_tasks_memory(
            int64_t need_free_mem,
            const std::vector<std::shared_ptr<ResourceContext>>& resource_ctxs,
            const std::string& revoke_reason, RuntimeProfile* profile, PriorityCmpFunc priority_cmp,
            std::vector<FilterFunc> filters, ActionFunc action);

    static bool revoke_process_memory(const std::string& revoke_reason);

    static void je_purge_dirty_pages();

private:
};

#include "common/compile_check_end.h"
} // namespace doris
