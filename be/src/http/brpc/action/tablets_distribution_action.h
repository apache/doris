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
#include "util/easy_json.h"

namespace doris {

class TabletsDistributionHandler : public BaseHttpHandler {
public:
    TabletsDistributionHandler();
    ~TabletsDistributionHandler() override = default;
    EasyJson get_tablets_distribution_group_by_partition(brpc::Controller* cntl,
                                                         uint64_t partition_id);

protected:
    void handle_sync(brpc::Controller* cntl) override;

    bool support_method(brpc::HttpMethod method) const override;
};
} // namespace doris