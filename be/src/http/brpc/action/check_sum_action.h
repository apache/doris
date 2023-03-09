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
#include "http/brpc/brpc_http_handler.h"

namespace doris {
class CheckSumHandler : public BaseHttpHandler {
public:
    CheckSumHandler();
    ~CheckSumHandler() override = default;

protected:
    void handle_sync(brpc::Controller* cntl) override;

    bool support_method(brpc::HttpMethod method) const override;

private:
    int64_t _do_check_sum(int64_t tablet_id, int64_t version, int32_t schema_hash);
};
} // namespace doris