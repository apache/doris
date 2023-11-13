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

#include "vec/core/future_block.h"

#include <tuple>

namespace doris::vectorized {

void FutureBlock::set_info(int64_t schema_version, const TUniqueId& load_id) {
    this->_schema_version = schema_version;
    this->_load_id = load_id;
}

void FutureBlock::set_result(Status status, int64_t total_rows, int64_t loaded_rows) {
    auto result = std::make_tuple(true, status, total_rows, loaded_rows);
    result.swap(*_result);
}

void FutureBlock::swap_future_block(std::shared_ptr<FutureBlock> other) {
    Block::swap(*other.get());
    set_info(other->_schema_version, other->_load_id);
    lock = other->lock;
    cv = other->cv;
    _result = other->_result;
}

} // namespace doris::vectorized
