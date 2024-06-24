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

#include "runtime/memory/global_memory_arbitrator.h"

namespace doris {

class MemoryReclamation {
public:
    static bool process_minor_gc(
            std::string mem_info =
                    doris::GlobalMemoryArbitrator::process_soft_limit_exceeded_errmsg_str());
    static bool process_full_gc(
            std::string mem_info =
                    doris::GlobalMemoryArbitrator::process_limit_exceeded_errmsg_str());

    static int64_t tg_disable_overcommit_group_gc();
    static int64_t tg_enable_overcommit_group_gc(int64_t request_free_memory,
                                                 RuntimeProfile* profile, bool is_minor_gc);

private:
};

} // namespace doris
