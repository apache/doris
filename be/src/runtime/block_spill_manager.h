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

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "olap/options.h"
#include "vec/core/block_spill_reader.h"
#include "vec/core/block_spill_writer.h"

namespace doris {

class ExecEnv;
class BlockSpillManager {
public:
    BlockSpillManager(const std::vector<StorePath>& paths);
    Status get_writer(int32_t batch_size, vectorized::BlockSpillWriterUPtr& writer);
    Status get_reader(int64_t stream_id, vectorized::BlockSpillReaderUPtr& reader);
    void remove(int64_t streamid_);

private:
    std::vector<StorePath> _store_paths;
    std::mutex lock_;
    int64_t id_ = 0;
    std::unordered_map<int64_t, std::string> id_to_file_paths_;
};
} // namespace doris