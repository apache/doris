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

#include <gen_cpp/Types_types.h>

#include "common/factory_creator.h"
#include "common/status.h"

namespace doris {

class TaskController {
    ENABLE_FACTORY_CREATOR(TaskController);

public:
    TaskController() { task_id_ = TUniqueId(); };
    virtual ~TaskController() = default;

    const TUniqueId& task_id() const { return task_id_; }
    void set_task_id(TUniqueId task_id) { task_id_ = task_id; }

    virtual Status cancel(Status cancel_reason) { return Status::OK(); }
    virtual Status running_time(int64_t* running_time_msecs) {
        *running_time_msecs = 0;
        return Status::OK();
    }

private:
    TUniqueId task_id_;
};

} // namespace doris
