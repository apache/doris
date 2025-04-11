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

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "pipeline/thrift_builder.h"
#include "runtime/query_context.h"

namespace doris {

class RuntimeFilterTest : public testing::Test {
public:
    RuntimeFilterTest() = default;
    ~RuntimeFilterTest() override = default;
    void SetUp() override {
        _tbl._row_tuples.push_back({});

        _query_options = TQueryOptionsBuilder().build();
        auto fe_address = TNetworkAddress();
        fe_address.hostname = LOCALHOST;
        fe_address.port = DUMMY_PORT;

        _query_ctx =
                QueryContext::create(TUniqueId(), ExecEnv::GetInstance(), _query_options,
                                     fe_address, true, fe_address, QuerySource::INTERNAL_FRONTEND);
        _query_ctx->runtime_filter_mgr()->set_runtime_filter_params(
                TRuntimeFilterParamsBuilder().build());

        _runtime_states.resize(INSTANCE_NUM);
        _local_mgrs.resize(INSTANCE_NUM);
        for (int i = 0; i < INSTANCE_NUM; i++) {
            _runtime_states[i] = RuntimeState::create_unique(
                    TUniqueId(), 0, _query_options, _query_ctx->query_globals,
                    ExecEnv::GetInstance(), _query_ctx.get());

            _local_mgrs[i] = std::make_unique<RuntimeFilterMgr>(false);
            _runtime_states[i]->set_runtime_filter_mgr(_local_mgrs[i].get());
            _runtime_states[i]->set_desc_tbl(&_tbl);
        }
    }
    void TearDown() override {}

protected:
    RuntimeProfile _profile = RuntimeProfile("");
    std::shared_ptr<QueryContext> _query_ctx;
    TQueryOptions _query_options;
    const std::string LOCALHOST = BackendOptions::get_localhost();
    const int DUMMY_PORT = config::brpc_port;
    const int INSTANCE_NUM = 2;
    std::vector<std::unique_ptr<RuntimeState>> _runtime_states;
    std::vector<std::unique_ptr<RuntimeFilterMgr>> _local_mgrs;
    DescriptorTbl _tbl;
};

} // namespace doris
