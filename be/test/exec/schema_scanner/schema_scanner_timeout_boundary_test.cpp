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

#include <gen_cpp/FrontendService.h>
#include <gtest/gtest.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TServerSocket.h>

#include <future>
#include <limits>
#include <thread>

#include "common/object_pool.h"
#include "information_schema/schema_active_queries_scanner.h"
#include "information_schema/schema_authentication_integrations_scanner.h"
#include "information_schema/schema_catalog_meta_cache_stats_scanner.h"
#include "information_schema/schema_extensions_scanner.h"
#include "information_schema/schema_partitions_scanner.h"
#include "information_schema/schema_role_mappings_scanner.h"
#include "information_schema/schema_sql_block_rule_status_scanner.h"
#include "information_schema/schema_table_stream_consumption_scanner.h"
#include "information_schema/schema_table_streams_scanner.h"
#include "information_schema/schema_tso_status_scanner.h"
#include "information_schema/schema_view_dependency_scanner.h"
#include "information_schema/schema_workload_group_privileges.h"
#include "information_schema/schema_workload_groups_scanner.h"
#include "information_schema/schema_workload_sched_policy_scanner.h"
#include "testutil/mock/mock_runtime_state.h"
#include "util/client_cache.h"
#include "util/defer_op.h"
#include "util/dns_cache.h"
#include "util/thrift_rpc_helper.h"

namespace doris {
template <typename Scanner>
class SchemaScannerTimeoutBoundaryTest : public testing::Test {};

using SchemaTimeoutTypes =
        testing::Types<SchemaActiveQueriesScanner, SchemaAuthenticationIntegrationsScanner,
                       SchemaCatalogMetaCacheStatsScanner, SchemaExtensionsScanner,
                       SchemaRoleMappingsScanner, SchemaSqlBlockRuleStatusScanner,
                       SchemaTableStreamConsumptionScanner, SchemaTableStreamsScanner,
                       SchemaTsoStatusScanner, SchemaViewDependencyScanner,
                       SchemaWorkloadGroupsScanner, SchemaWorkloadSchedulePolicyScanner,
                       SchemaWorkloadGroupPrivilegesScanner>;
TYPED_TEST_SUITE(SchemaScannerTimeoutBoundaryTest, SchemaTimeoutTypes);

TYPED_TEST(SchemaScannerTimeoutBoundaryTest, StartPreservesPositiveRpcDeadline) {
    // Each scanner stores the timeout later passed to its FE RPC. One RuntimeState
    // and zero rows suffice. 2147484 seconds is the first overflowing INT32 product.
    MockRuntimeState state;
    state._query_options.__set_execution_timeout(2147484);
    SchemaScannerParam param;
    ObjectPool pool;
    TypeParam scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());
    ASSERT_TRUE(scanner.start(&state).ok());
    if constexpr (requires { scanner._rpc_timeout_ms; }) {
        EXPECT_EQ(scanner._rpc_timeout_ms, std::numeric_limits<int32_t>::max());
    } else {
        EXPECT_EQ(scanner._rpc_timeout, std::numeric_limits<int32_t>::max());
    }
}
TEST(SchemaPerDbTimeoutBoundaryTest, StartPreservesPositiveDeadlineAfterDatabaseLookup) {
    class EmptyFrontend final : public FrontendServiceNull {
    public:
        void getDbNames(TGetDbsResult& result, const TGetDbsParams&) override {
            result.__set_dbs({});
        }
    };
    class Ready final : public apache::thrift::server::TServerEventHandler {
    public:
        void preServe() override { promise.set_value(); }
        std::promise<void> promise;
    };
    // The per-database scanner performs one lookup before storing its deadline.
    // Return no databases from a local stub on an ephemeral port. No catalog or rows are needed.
    auto frontend = std::make_shared<EmptyFrontend>();
    auto processor = std::make_shared<FrontendServiceProcessor>(frontend);
    auto socket = std::make_shared<apache::thrift::transport::TServerSocket>("127.0.0.1", 0);
    apache::thrift::server::TThreadedServer server(
            processor, socket,
            std::make_shared<apache::thrift::transport::TBufferedTransportFactory>(),
            std::make_shared<apache::thrift::protocol::TBinaryProtocolFactory>());
    auto ready = std::make_shared<Ready>();
    server.setServerEventHandler(ready);
    auto cache = std::make_unique<ClientCache<FrontendServiceClient>>();
    auto* env = ExecEnv::GetInstance();
    DNSCache dns_cache([](const std::string& host, std::string& ip, bool, int*) {
        ip = host;
        return Status::OK();
    });
    auto* old_dns_cache = env->_dns_cache;
    env->_dns_cache = &dns_cache;
    auto* old_cache = env->_frontend_client_cache;
    auto* old_rpc_env = ThriftRpcHelper::get_exec_env();
    env->_frontend_client_cache = cache.get();
    ThriftRpcHelper::setup(env);
    std::thread worker([&] { server.serve(); });
    Defer restore {[&] {
        cache.reset();
        server.stop();
        worker.join();
        env->_frontend_client_cache = old_cache;
        env->_dns_cache = old_dns_cache;
        ThriftRpcHelper::setup(old_rpc_env);
    }};
    ASSERT_EQ(ready->promise.get_future().wait_for(std::chrono::seconds(5)),
              std::future_status::ready);
    MockRuntimeState state;
    state._query_options.__set_execution_timeout(2147484);
    std::string ip = "127.0.0.1";
    SchemaScannerParam param;
    param.common_param->ip = &ip;
    param.common_param->port = socket->getPort();
    ObjectPool pool;
    SchemaPartitionsScanner scanner;
    ASSERT_TRUE(scanner.init(&state, &param, &pool).ok());
    ASSERT_TRUE(scanner.start(&state).ok());
    EXPECT_EQ(scanner._rpc_timeout_ms, INT32_MAX);
}

} // namespace doris
