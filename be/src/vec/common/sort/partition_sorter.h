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
#include <gen_cpp/PlanNodes_types.h>
#include <stddef.h>
#include <stdint.h>

#include <cstdint>
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

struct SortCursorCmp {
public:
    SortCursorCmp() {
        impl = nullptr;
        row = 0;
    }
    SortCursorCmp(const MergeSortCursor& cursor) : row(cursor->pos), impl(cursor.impl) {}

    void reset() {
        impl = nullptr;
        row = 0;
    }
    bool compare_two_rows(const MergeSortCursor& rhs) const {
        for (size_t i = 0; i < impl->sort_columns_size; ++i) {
            int direction = impl->desc[i].direction;
            int nulls_direction = impl->desc[i].nulls_direction;
            int res = direction * impl->sort_columns[i]->compare_at(row, rhs.impl->pos,
                                                                    *(rhs.impl->sort_columns[i]),
                                                                    nulls_direction);
            if (res != 0) {
                return false;
            }
        }
        return true;
    }
    int row = 0;
    MergeSortCursorImpl* impl = nullptr;
};

class PartitionSorter final : public Sorter {
    ENABLE_FACTORY_CREATOR(PartitionSorter);

public:
    PartitionSorter(VSortExecExprs& vsort_exec_exprs, int limit, int64_t offset, ObjectPool* pool,
                    std::vector<bool>& is_asc_order, std::vector<bool>& nulls_first,
                    const RowDescriptor& row_desc, RuntimeState* state, RuntimeProfile* profile,
                    bool has_global_limit, int partition_inner_limit,
                    TopNAlgorithm::type top_n_algorithm, SortCursorCmp* previous_row);

    ~PartitionSorter() override = default;

    Status append_block(Block* block) override;

    Status prepare_for_read() override;

    Status get_next(RuntimeState* state, Block* block, bool* eos) override;

    size_t data_size() const override { return _state->data_size(); }

    Status partition_sort_read(Block* block, bool* eos, int batch_size);
    int64 get_output_rows() const { return _output_total_rows; }
    void reset_sorter_state(RuntimeState* runtime_state);

private:
    std::unique_ptr<MergeSorterState> _state;
    const RowDescriptor& _row_desc;
    int64 _output_total_rows = 0;
    int64 _output_distinct_rows = 0;
    bool _has_global_limit = false;
    int _partition_inner_limit = 0;
    TopNAlgorithm::type _top_n_algorithm = TopNAlgorithm::type::ROW_NUMBER;
    SortCursorCmp* _previous_row = nullptr;
};

} // namespace doris::vectorized
