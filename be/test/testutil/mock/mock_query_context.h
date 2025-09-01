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
#include "runtime/query_context.h"

namespace doris {

inline TQueryOptions create_fake_query_options() {
    TQueryOptions query_options;
    query_options.query_type = TQueryType::EXTERNAL;
    return query_options;
}

struct MockQueryContext : public QueryContext {
    ENABLE_FACTORY_CREATOR(MockQueryContext);

    MockQueryContext(TUniqueId query_id, ExecEnv* exec_env, const TQueryOptions& query_options,
                     TNetworkAddress coord_address, bool is_nereids,
                     TNetworkAddress current_connect_fe_addr, QuerySource query_type)
            : QueryContext(query_id, exec_env, query_options, coord_address, is_nereids,
                           current_connect_fe_addr, query_type) {}

    static std::shared_ptr<MockQueryContext> create(
            TUniqueId query_id = TUniqueId(), ExecEnv* exec_env = ExecEnv::GetInstance(),
            const TQueryOptions& query_options = create_fake_query_options(),
            TNetworkAddress coord_address = TNetworkAddress(), bool is_nereids = true,
            TNetworkAddress current_connect_fe_addr = TNetworkAddress(),
            QuerySource query_type = QuerySource::GROUP_COMMIT_LOAD) {
        auto ctx = MockQueryContext::create_shared(query_id, exec_env, query_options, coord_address,
                                                   is_nereids, current_connect_fe_addr, query_type);
        ctx->init_query_task_controller();
        return ctx;
    }

    void set_mock_llm_resource() {
        TLLMResource llm_resource;
        llm_resource.provider_type = "MOCK";
        llm_resource.model_name = "mock_model";
        llm_resource.endpoint = "http://localhost";
        llm_resource.api_key = "xxx";
        llm_resource.temperature = 0.5;
        llm_resource.max_tokens = 16;
        llm_resource.max_retries = 1;
        llm_resource.retry_delay_second = 1;
        llm_resource.dimensions = 514;

        set_llm_resources(std::map<std::string, TLLMResource> {{"mock_resource", llm_resource}});
    }
};

} // namespace doris
