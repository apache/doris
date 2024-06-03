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
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "vec/common/sort/sorter.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeProfile;
class RuntimeState;

namespace vectorized {
class Block;
class VSortExecExprs;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {

class TopNSorter final : public Sorter {
    ENABLE_FACTORY_CREATOR(TopNSorter);

public:
    TopNSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset, ObjectPool* pool,
               std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first,
               const RowDescriptor& row_desc, RuntimeState* state, RuntimeProfile* profile);

    ~TopNSorter() override = default;

    Status append_block(Block* block) override;

    Status prepare_for_read() override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    size_t data_size() const override;

    static constexpr size_t TOPN_SORT_THRESHOLD = 256;

private:
    Status _do_sort(Block* block);

    std::unique_ptr<MergeSorterState> _state;
    const RowDescriptor& _row_desc;
};

} // namespace doris::vectorized
