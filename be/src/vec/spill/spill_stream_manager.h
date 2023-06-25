
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
#include <memory>
#include <mutex>
#include <unordered_map>

#include "common/status.h"
#include "olap/options.h"
#include "vec/spill/spill_stream.h"
namespace doris {

class RuntimeProfile;

namespace vectorized {

class SpillStreamManager {
public:
    SpillStreamManager(const std::vector<StorePath>& paths);

    Status init();

    // 创建SpillStream并登记
    Status register_spill_stream(SpillStreamSPtr& spill_stream, std::string query_id,
                                 std::string operator_name,
                                 int32_t operator_id, /* int32_t task_id, */
                                 int32_t batch_rows, size_t batch_bytes, RuntimeProfile* profile);

    // 标记SpillStream需要被删除，在GC线程中异步删除落盘文件
    void delete_spill_stream(SpillStreamSPtr spill_stream);

    Status spill_stream(SpillStreamSPtr spill_stream);

    Status spill_thread();

private:
    std::vector<StorePath> _store_paths;
    ThreadPool* io_thread_pool_;
    std::mutex lock_;
    int64_t id_ = 0;
    // all the stream
    std::unordered_map<int64_t, SpillStreamSPtr> id_to_spill_streams_;
    // streams that need to spill
    std::unordered_map<int64_t, SpillStreamSPtr> spill_streams_;
};
} // namespace vectorized
} // namespace doris