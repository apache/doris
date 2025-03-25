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

class TaskController {
    ENABLE_FACTORY_CREATOR(TaskController);

public:
    TaskController() {
        task_id_ = TUniqueId();
        start_time_ = MonotonicMillis();
    };
    virtual ~TaskController() = default;

    bool is_attach_task() { return task_id_ != TUniqueId(); }
    const TUniqueId& task_id() const { return task_id_; }
    void set_task_id(TUniqueId task_id) { task_id_ = task_id; }

    virtual bool is_cancelled() const { return is_cancelled_; }
    virtual Status cancel(const Status& reason) {
        is_cancelled_ = true;
        return Status::OK();
    }

    virtual bool is_finished() const { return is_finished_; }
    virtual void finish() {
        is_finished_ = true;
        finish_time_ = MonotonicMillis();
    }

    int64_t start_time() const { return start_time_; }
    int64_t finish_time() const { return finish_time_; }
    int64_t running_time() const { return finish_time_ - start_time_; }
    TNetworkAddress fe_addr() { return fe_addr_; }
    TQueryType::type query_type() { return query_type_; }

    void set_fe_addr(TNetworkAddress fe_addr) { fe_addr_ = fe_addr; }
    void set_query_type(TQueryType::type query_type) { query_type_ = query_type; }

protected:
    TUniqueId task_id_;
    bool is_cancelled_ = false;
    bool is_finished_ = false;
    int64_t start_time_;
    int64_t finish_time_;
    TNetworkAddress fe_addr_;
    TQueryType::type query_type_;
};

} // namespace doris
