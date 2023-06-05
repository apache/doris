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

#ifndef DORIS_BE_TEST_QUERY_RUNTIME_TEST_ENV_H
#define DORIS_BE_TEST_QUERY_RUNTIME_TEST_ENV_H

#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

namespace doris {
class ExecEnv;
class RuntimeState;
class StorageEngine;

/// Helper testing class that creates an environment with runtime memory management
/// similar to the one used by the Doris runtime. Only one TestEnv can be active at a
/// time, because it modifies the global ExecEnv singleton.
class TestEnv {
public:
    TestEnv();
    ~TestEnv();

    // If don't need to open, paths can be empty.
    void init_storage_engine(bool need_open, const std::vector<std::string>& paths = {});

    // Destroy all RuntimeStates and block managers created by this TestEnv.
    void tear_down_query_states();

    // Calculate memory limit accounting for overflow and negative values.
    // If max_buffers is -1, no memory limit will apply.
    static int64_t calculate_mem_tracker(int max_buffers, int block_size);

    ExecEnv* exec_env() { return _exec_env; }

private:
    ExecEnv* _exec_env;

    // Per-query states with associated block managers.
    std::vector<std::shared_ptr<RuntimeState> > _query_states;

    StorageEngine* _engine = nullptr;
};

} // end namespace doris

#endif // DORIS_BE_TEST_QUERY_RUNTIME_TEST_ENV_H
