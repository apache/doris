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

#include <memory>
#include <string>
#include <tuple>
#include <vector>

#include "orc/sargs/SearchArgument.hh"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "vec/common/sort/sorter.h"
#include "vec/common/sort/topn_sorter.h"
#include "vec/common/sort/vsort_exec_exprs.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/exec/format/orc/orc_memory_pool.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exprs/mock_slot_ref.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/utils/util.hpp"
namespace doris::vectorized {
class SortTest : public testing::Test {
public:
    SortTest() = default;
    ~SortTest() override = default;
};

class MockRuntimeState : public RuntimeState {
public:
    MockRuntimeState() {};

    int batch_size() const override { return _batch_size; }

    int _batch_size = 10;
};

TEST_F(SortTest, test_full_sort) {
    ObjectPool pool;

    std::vector<DataTypePtr> data_types {std::make_shared<DataTypeInt32>()};
    RowDescriptor row_desc {data_types, &pool};

    Block block = VectorizedUtils::create_empty_block(row_desc, true /*ignore invalid slot*/);

    auto column = DataTypeInt32::ColumnType::create();

    for (int i = 0; i < 10; i++) {
        column->insert_value(i);
    }
    for (int i = 10; i >= 0; i--) {
        column->insert_value(i);
    }

    block.get_by_position(0).column = std::move(column);

    VSortExecExprs sort_exec_exprs;

    sort_exec_exprs._sort_tuple_slot_expr_ctxs.push_back(
            VExprContext::create_shared(std::make_shared<MockSlotRef>(0)));

    sort_exec_exprs._materialize_tuple = false;

    sort_exec_exprs._lhs_ordering_expr_ctxs.push_back(
            VExprContext::create_shared(std::make_shared<MockSlotRef>(0)));
    std::cout << block.dump_data() << std::endl;

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};

    std::unique_ptr<vectorized::Sorter> sorter = FullSorter::create_unique(
            sort_exec_exprs, -1, 0, &pool, is_asc_order, nulls_first, row_desc, nullptr, nullptr);

    MockRuntimeState state;

    {
        auto st = sorter->append_block(&block);
        std::cout << st.msg() << "\n";
    }

    {
        auto st = sorter->prepare_for_read();
        std::cout << st.msg() << "\n";
    }

    {
        Block output_block;
        bool eos = false;
        while (!eos) {
            output_block.clear();
            auto st = sorter->get_next(&state, &output_block, &eos);
            std::cout << "eos : " << eos << std::endl;
            std::cout << st.msg() << "\n";
            std::cout << output_block.dump_data() << std::endl;
        }
    }
}

TEST_F(SortTest, test_topn_sort) {
    ObjectPool pool;

    std::vector<DataTypePtr> data_types {std::make_shared<DataTypeInt32>()};
    RowDescriptor row_desc {data_types, &pool};

    Block block = VectorizedUtils::create_empty_block(row_desc, true /*ignore invalid slot*/);

    auto column = DataTypeInt32::ColumnType::create();

    for (int i = 0; i < 10; i++) {
        column->insert_value(i);
    }
    for (int i = 10; i >= 0; i--) {
        column->insert_value(i);
    }

    block.get_by_position(0).column = std::move(column);

    VSortExecExprs sort_exec_exprs;

    sort_exec_exprs._sort_tuple_slot_expr_ctxs.push_back(
            VExprContext::create_shared(std::make_shared<MockSlotRef>(0)));

    sort_exec_exprs._materialize_tuple = false;

    sort_exec_exprs._lhs_ordering_expr_ctxs.push_back(
            VExprContext::create_shared(std::make_shared<MockSlotRef>(0)));
    std::cout << block.dump_data() << std::endl;

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};

    std::unique_ptr<vectorized::Sorter> sorter = TopNSorter::create_unique(
            sort_exec_exprs, 10, 0, &pool, is_asc_order, nulls_first, row_desc, nullptr, nullptr);

    MockRuntimeState state;

    {
        auto st = sorter->append_block(&block);
        std::cout << st.msg() << "\n";
    }

    {
        auto st = sorter->prepare_for_read();
        std::cout << st.msg() << "\n";
    }

    {
        Block output_block;
        bool eos = false;
        while (!eos) {
            output_block.clear();
            auto st = sorter->get_next(&state, &output_block, &eos);
            std::cout << "eos : " << eos << std::endl;
            std::cout << st.msg() << "\n";
            std::cout << output_block.dump_data() << std::endl;
        }
    }
}

} // namespace doris::vectorized
