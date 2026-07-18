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

// Reaches the private _create_operator directly: constructing a full prepare()
// payload for one switch branch would couple the test to every unrelated
// prepare step. BE UT compiles with -fno-access-control (see be/test/AGENTS.md),
// so no access hack is needed.

#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/QueryCache_types.h>
#include <gen_cpp/Types_types.h>
#include <gtest/gtest.h>

#include <memory>

#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_fragment_context.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"

namespace doris {

static void empty_finish_function(RuntimeState*, Status*) {}

// The plan tree is built in pre-order, so when the query cache is enabled the
// cache source (above the scan) must have created the shared QueryCacheRuntime
// before the scan node is reached. A missing runtime means FE sent a malformed
// plan shape; silently creating one here would let a HIT decision skip the
// scan while no cache source emits the entry, i.e. drop data, so the operator
// factory must fail loudly instead.
class QueryCacheFragmentContextTest : public testing::Test {
protected:
    void SetUp() override {
        TQueryOptions query_options;
        TNetworkAddress fe_address;
        fe_address.hostname = "127.0.0.1";
        fe_address.port = 8060;
        _query_ctx =
                QueryContext::create(_query_id, ExecEnv::GetInstance(), query_options, fe_address,
                                     true, fe_address, QuerySource::INTERNAL_FRONTEND);

        TQueryCacheParam cache_param;
        cache_param.__set_node_id(0);
        cache_param.__set_digest("test_digest");
        _params.fragment.__set_query_cache_param(cache_param);
        _context = std::make_shared<PipelineFragmentContext>(
                _query_id, _params, _query_ctx, ExecEnv::GetInstance(), empty_finish_function);
    }

    Status create_scan_operator(const TPlanNode& tnode) {
        ObjectPool pool;
        DescriptorTbl descs;
        OperatorPtr op;
        OperatorPtr cache_op;
        PipelinePtr pipe = std::make_shared<Pipeline>(0, 1, 1);
        return _context->_create_operator(&pool, tnode, descs, op, pipe, /*parent_idx=*/-1,
                                          /*child_idx=*/0,
                                          /*followed_by_shuffled_operator=*/false,
                                          /*require_bucket_distribution=*/false, cache_op);
    }

    TUniqueId _query_id;
    TPipelineFragmentParams _params;
    std::shared_ptr<QueryContext> _query_ctx;
    std::shared_ptr<PipelineFragmentContext> _context;
};

TEST_F(QueryCacheFragmentContextTest, scan_without_runtime_fails_loudly) {
    TPlanNode tnode;
    tnode.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
    tnode.__set_node_id(7);
    tnode.__set_num_children(0);

    ASSERT_EQ(_context->_query_cache_runtime, nullptr);
    Status st = create_scan_operator(tnode);
    EXPECT_FALSE(st.ok());
    // The full message pins the scan-side wording and the argument order
    // (tnode.node_id first, then the FE-designated cache node id).
    EXPECT_TRUE(st.to_string().find("query cache runtime is absent at the scan node, "
                                    "node_id=7, cache node_id=0") != std::string::npos)
            << st.to_string();
}

TEST_F(QueryCacheFragmentContextTest, binlog_scan_without_runtime_fails_before_binlog_handling) {
    // The runtime check comes first: a row-binlog scan with a broken setup
    // must fail the same way instead of dereferencing the missing runtime in
    // disable_for_binlog_scan().
    TPlanNode tnode;
    tnode.__set_node_type(TPlanNodeType::OLAP_SCAN_NODE);
    tnode.__set_node_id(7);
    tnode.__set_num_children(0);
    TOlapScanNode olap_scan_node;
    olap_scan_node.__set_read_row_binlog(true);
    tnode.__set_olap_scan_node(olap_scan_node);

    ASSERT_EQ(_context->_query_cache_runtime, nullptr);
    Status st = create_scan_operator(tnode);
    EXPECT_FALSE(st.ok());
    EXPECT_TRUE(st.to_string().find("query cache runtime is absent at the scan node, "
                                    "node_id=7, cache node_id=0") != std::string::npos)
            << st.to_string();
}

} // namespace doris
