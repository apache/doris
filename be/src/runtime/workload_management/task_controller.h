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
#include <gen_cpp/Types_types.h>

#include "common/factory_creator.h"
#include "common/status.h"
#include "util/time.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace pipeline {
class PipelineTask;
} // namespace pipeline

class ResourceContext;
class TaskController {
    ENABLE_FACTORY_CREATOR(TaskController);

public:
    TaskController() { task_id_ = TUniqueId(); };
    virtual ~TaskController() = default;

    /* common action
    */
    const TUniqueId& task_id() const { return task_id_; }
    void set_task_id(TUniqueId task_id) {
        task_id_ = task_id;
        start_time_ = MonotonicMillis();
    }
    TQueryType::type query_type() { return query_type_; }
    void set_query_type(TQueryType::type query_type) { query_type_ = query_type; }
    TNetworkAddress fe_addr() { return fe_addr_; }
    void set_fe_addr(TNetworkAddress fe_addr) { fe_addr_ = fe_addr; }
    std::string debug_string();

    /* finish action
    */
    bool is_finished() const { return is_finished_; }
    void finish() {
        if (!is_finished_) {
            is_finished_ = true;
            finish_time_ = MonotonicMillis();
        }
        finish_impl();
    }
    virtual void finish_impl() {}
    int64_t start_time() const { return start_time_; }
    int64_t finish_time() const { return finish_time_; }
    int64_t running_time() const { return finish_time() - start_time(); }

    /* cancel action
    */
    virtual bool is_cancelled() const { return false; }

    bool cancel(const Status& reason) {
        if (cancelled_time_ == 0) {
            cancelled_time_ = MonotonicMillis();
        }
        return cancel_impl(reason);
    }

    virtual bool cancel_impl(const Status& reason) { return false; }

    int64_t cancelled_time() const { return cancelled_time_; }

    /* pause action & property
    */
    void update_paused_reason(const Status& st);
    void reset_paused_reason() { paused_reason_.reset(); }
    Status paused_reason() { return paused_reason_.status(); }
    void add_paused_count() { paused_count_.fetch_add(1); }

    /* memory status action
    */
    virtual int32_t get_slot_count() const { return 1; }
    virtual bool is_pure_load_task() const { return false; }
    void set_low_memory_mode(bool low_memory_mode) { low_memory_mode_ = low_memory_mode; }
    bool low_memory_mode() { return low_memory_mode_; }
    void disable_reserve_memory() { enable_reserve_memory_ = false; }
    virtual bool is_enable_reserve_memory() const { return enable_reserve_memory_; }
    virtual void set_memory_sufficient(bool sufficient) {};
    virtual int64_t memory_sufficient_time() { return 0; };

    /* memory revoke action
    */
    virtual void get_revocable_info(size_t* revocable_size, size_t* memory_usage,
                                    bool* has_running_task) {};
    virtual size_t get_revocable_size() { return 0; };
    virtual Status revoke_memory() { return Status::OK(); };
    virtual std::vector<pipeline::PipelineTask*> get_revocable_tasks() { return {}; };
    void increase_revoking_tasks_count() { revoking_tasks_count_.fetch_add(1); }
    void decrease_revoking_tasks_count() { revoking_tasks_count_.fetch_sub(1); }
    int get_revoking_tasks_count() const { return revoking_tasks_count_.load(); }

protected:
    friend class ResourceContext;

    void set_resource_ctx(ResourceContext* resource_ctx) { resource_ctx_ = resource_ctx; }
    ResourceContext* resource_ctx_ {nullptr};

    /* common property
    */
    TUniqueId task_id_;
    TNetworkAddress fe_addr_;
    TQueryType::type query_type_;

    /* cancel property
    */
    std::atomic<int64_t> cancelled_time_ = 0;

    /* finish property
    */
    std::atomic<bool> is_finished_ = false;
    int64_t start_time_ = 0;
    std::atomic<int64_t> finish_time_ = 0;

    /* pause property
    */
    AtomicStatus paused_reason_;
    std::atomic<int64_t> paused_count_ = 0;

    /* memory status property
    */
    std::atomic<bool> low_memory_mode_ = false;
    std::atomic<bool> enable_reserve_memory_ = true;

    /* memory revoke property
    */
    std::atomic<int> revoking_tasks_count_ = 0;
};

#include "common/compile_check_end.h"
} // namespace doris
