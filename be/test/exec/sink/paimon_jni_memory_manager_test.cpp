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

#include "exec/sink/writer/paimon/paimon_jni_memory_manager.h"

#include "common/config.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"

namespace doris {

TEST(PaimonJniMemoryManagerTest, DivideQueryBudgetBySinkPipelineTaskCount) {
    constexpr int64_t QUERY_LIMIT = 256L * 1024 * 1024;
    constexpr int64_t CONFIGURED_LIMIT = 512L * 1024 * 1024;
    constexpr int SINK_PIPELINE_TASKS = 4;

    const int64_t old_configured_limit = config::paimon_jni_writer_memory_pool_limit_bytes;
    Defer restore_config {
            [&] { config::paimon_jni_writer_memory_pool_limit_bytes = old_configured_limit; }};
    config::paimon_jni_writer_memory_pool_limit_bytes = CONFIGURED_LIMIT;

    TUniqueId query_id;
    query_id.hi = 1;
    query_id.lo = 2;
    TQueryOptions query_options;
    query_options.__set_mem_limit(QUERY_LIMIT);
    query_options.__set_query_type(TQueryType::SELECT);
    TNetworkAddress fe_address;
    fe_address.hostname = "127.0.0.1";
    fe_address.port = 9030;
    auto query_ctx =
            QueryContext::create(query_id, ExecEnv::GetInstance(), query_options, fe_address, true,
                                 fe_address, QuerySource::INTERNAL_FRONTEND);
    ASSERT_NE(query_ctx, nullptr);

    auto state = RuntimeState::create_unique(query_id, 0, query_options, query_ctx->query_globals,
                                             ExecEnv::GetInstance(), query_ctx.get());
    state->set_task_num(SINK_PIPELINE_TASKS);
    // Paimon must not depend on this FE-provided OLAP sink field.
    state->set_num_local_sink(1);

    std::unique_ptr<PaimonJniMemoryManager> manager;
    ASSERT_TRUE(PaimonJniMemoryManager::create(state.get(), &manager).ok());
    ASSERT_NE(manager, nullptr);
    EXPECT_EQ(manager->memory_limit(), QUERY_LIMIT / SINK_PIPELINE_TASKS);
}

} // namespace doris
