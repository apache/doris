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

#include "spillable_operator_test_helper.h"

#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gmock/gmock-actions.h>
#include <gmock/gmock-function-mocker.h>
#include <gmock/gmock-spec-builders.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "testutil/creators.h"

namespace doris::pipeline {
void SpillableOperatorTestHelper::SetUp() {
    runtime_state = std::make_unique<MockRuntimeState>();
    obj_pool = std::make_unique<ObjectPool>();

    runtime_profile = std::make_shared<RuntimeProfile>("test");

    query_ctx = generate_one_query();

    runtime_state->_query_ctx = query_ctx.get();
    runtime_state->_query_id = query_ctx->query_id();
    runtime_state->resize_op_id_to_local_state(-100);
    runtime_state->set_max_operator_id(-100);

    ADD_TIMER(runtime_profile.get(), "ExecTime");
    runtime_profile->AddHighWaterMarkCounter("MemoryUsed", TUnit::BYTES, "", 0);

    auto desc_table = create_test_table_descriptor(false);
    auto st = DescriptorTbl::create(obj_pool.get(), desc_table, &desc_tbl);
    DCHECK(!desc_table.slotDescriptors.empty());
    EXPECT_TRUE(st.ok()) << "create descriptor table failed: " << st.to_string();
    runtime_state->set_desc_tbl(desc_tbl);

    auto spill_data_dir =
            std::make_unique<vectorized::SpillDataDir>("./ut_dir/spill_test", 1024L * 1024 * 4);
    st = io::global_local_filesystem()->create_directory(spill_data_dir->path(), false);
    EXPECT_TRUE(st.ok()) << "create directory: " << spill_data_dir->path()
                         << " failed: " << st.to_string();
    std::unordered_map<std::string, std::unique_ptr<vectorized::SpillDataDir>> data_map;
    data_map.emplace("test", std::move(spill_data_dir));
    auto* spill_stream_manager = new vectorized::SpillStreamManager(std::move(data_map));
    ExecEnv::GetInstance()->_spill_stream_mgr = spill_stream_manager;
    st = spill_stream_manager->init();
    EXPECT_TRUE(st.ok()) << "init spill stream manager failed: " << st.to_string();
}

void SpillableOperatorTestHelper::TearDown() {
    ExecEnv::GetInstance()->spill_stream_mgr()->async_cleanup_query(runtime_state->query_id());
    doris::ExecEnv::GetInstance()->spill_stream_mgr()->get_spill_io_thread_pool()->wait();
    doris::ExecEnv::GetInstance()->spill_stream_mgr()->stop();
    SAFE_DELETE(ExecEnv::GetInstance()->_spill_stream_mgr);
}

} // namespace doris::pipeline