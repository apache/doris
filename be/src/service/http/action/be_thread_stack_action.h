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

#include "service/http/http_handler_with_auth.h"

namespace doris {

class ExecEnv;
class HttpRequest;

class BeThreadStackAction : public HttpHandlerWithAuth {
public:
    BeThreadStackAction(ExecEnv* exec_env) : HttpHandlerWithAuth(exec_env) {}
    ~BeThreadStackAction() override = default;

    // GET /api/stack_trace[?thread_id=<tid>[,<tid>...]][&tid=<tid>][&timeout_ms=<ms>]
    //                     [&mode=<DISABLED|FAST|FULL|FULL_WITH_INLINE>]
    //                     [&dwarf_location_info_mode=<DISABLED|FAST|FULL|FULL_WITH_INLINE>]
    //                     [&skip_blocking_syscalls=<true|false>]
    // thread_id is the preferred selector; tid is kept as a legacy alias. The default is full
    // process collection without a signal-attempt cap, because production incidents usually need
    // blocked worker stacks as much as running stacks. skip_blocking_syscalls is an explicit
    // conservative mode for isolating EINTR-sensitive paths, not the normal capture policy.
    void handle(HttpRequest* req) override;
};

} // namespace doris
