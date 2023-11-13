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

#include "block.h"
#include "util/lock.h"

namespace doris {

namespace vectorized {

class FutureBlock : public Block {
    ENABLE_FACTORY_CREATOR(FutureBlock);

public:
    FutureBlock() : Block() {};
    void swap_future_block(std::shared_ptr<FutureBlock> other);
    void set_info(int64_t block_schema_version, const TUniqueId& load_id);
    int64_t get_schema_version() { return _schema_version; }
    TUniqueId get_load_id() { return _load_id; }

    // hold lock before call this function
    void set_result(Status status, int64_t total_rows = 0, int64_t loaded_rows = 0);
    bool is_handled() { return std::get<0>(*(_result)); }
    Status get_status() { return std::get<1>(*(_result)); }
    int64_t get_total_rows() { return std::get<2>(*(_result)); }
    int64_t get_loaded_rows() { return std::get<3>(*(_result)); }

    std::shared_ptr<doris::Mutex> lock = std::make_shared<doris::Mutex>();
    std::shared_ptr<doris::ConditionVariable> cv = std::make_shared<doris::ConditionVariable>();

private:
    int64_t _schema_version;
    TUniqueId _load_id;

    std::shared_ptr<std::tuple<bool, Status, int64_t, int64_t>> _result =
            std::make_shared<std::tuple<bool, Status, int64_t, int64_t>>(false, Status::OK(), 0, 0);
};
} // namespace vectorized
} // namespace doris
