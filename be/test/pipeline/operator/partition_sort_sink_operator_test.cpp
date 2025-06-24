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

#include "pipeline/exec/partition_sort_sink_operator.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

#include "pipeline/exec/partition_sort_source_operator.h"
#include "pipeline/operator/operator_helper.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_descriptors.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/core/block.h"
namespace doris::pipeline {

using namespace vectorized;

class PartitionSortOperatorMockOperator : public OperatorXBase {
public:
    Status get_block_after_projects(RuntimeState* state, vectorized::Block* block,
                                    bool* eos) override {
        return Status::OK();
    }

    Status get_block(RuntimeState* state, vectorized::Block* block, bool* eos) override {
        return Status::OK();
    }
    Status setup_local_state(RuntimeState* state, LocalStateInfo& info) override {
        return Status::OK();
    }

    const RowDescriptor& row_desc() const override { return *_mock_row_desc; }

private:
    std::unique_ptr<MockRowDescriptor> _mock_row_desc;
};

struct PartitionSortOperatorTest : public ::testing::Test {
    void SetUp() override {
        state = std::make_shared<MockRuntimeState>();
        state->batsh_size = 10;
        _child_op = std::make_unique<PartitionSortOperatorMockOperator>();
    }

    RuntimeProfile profile {"test"};
    std::unique_ptr<PartitionSortSinkOperatorX> sink;
    std::unique_ptr<PartitionSortSourceOperatorX> source;

    std::unique_ptr<PartitionSortSinkLocalState> sink_local_state_uptr;

    PartitionSortSinkLocalState* sink_local_state;

    std::unique_ptr<PartitionSortSourceLocalState> source_local_state_uptr;
    PartitionSortSourceLocalState* source_local_state;

    std::shared_ptr<MockRuntimeState> state;

    std::shared_ptr<PartitionSortOperatorMockOperator> _child_op;

    ObjectPool pool;

    std::shared_ptr<BasicSharedState> shared_state;

    bool is_ready(std::vector<Dependency*> deps) {
        for (auto* dep : deps) {
            if (!dep->ready()) {
                return false;
            }
        }
        return true;
    }

    void test_for_sink_and_source(int partition_exprs_num = 1, bool has_global_limit = false,
                                  int partition_inner_limit = 0) {
        SetUp();
        sink = std::make_unique<PartitionSortSinkOperatorX>(
                &pool, -1, partition_exprs_num, has_global_limit, partition_inner_limit);
        sink->_is_asc_order = {true};
        sink->_nulls_first = {false};

        sink->_vsort_exec_exprs._sort_tuple_slot_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        sink->_vsort_exec_exprs._materialize_tuple = false;

        sink->_vsort_exec_exprs._ordering_expr_ctxs =
                MockSlotRef::create_mock_contexts(std::make_shared<DataTypeInt64>());

        if (partition_exprs_num > 0) {
            sink->_partition_expr_ctxs =
                    MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeInt64>());
        }
        _child_op->_mock_row_desc.reset(
                new MockRowDescriptor {{std::make_shared<vectorized::DataTypeInt64>()}, &pool});

        EXPECT_TRUE(sink->set_child(_child_op));

        source = std::make_unique<PartitionSortSourceOperatorX>();

        shared_state = sink->create_shared_state();
        {
            sink_local_state_uptr =
                    PartitionSortSinkLocalState ::create_unique(sink.get(), state.get());
            sink_local_state = sink_local_state_uptr.get();
            LocalSinkStateInfo info {.task_idx = 0,
                                     .parent_profile = &profile,
                                     .sender_id = 0,
                                     .shared_state = shared_state.get(),
                                     .shared_state_map = {},
                                     .tsink = TDataSink {}};
            EXPECT_TRUE(sink_local_state_uptr->init(state.get(), info).ok());
            state->emplace_sink_local_state(0, std::move(sink_local_state_uptr));
        }

        {
            source_local_state_uptr =
                    PartitionSortSourceLocalState::create_unique(state.get(), source.get());
            source_local_state = source_local_state_uptr.get();
            LocalStateInfo info {.parent_profile = &profile,
                                 .scan_ranges = {},
                                 .shared_state = shared_state.get(),
                                 .shared_state_map = {},
                                 .task_idx = 0};

            EXPECT_TRUE(source_local_state_uptr->init(state.get(), info).ok());
            state->resize_op_id_to_local_state(-100);
            state->emplace_local_state(source->operator_id(), std::move(source_local_state_uptr));
        }

        { EXPECT_TRUE(sink_local_state->open(state.get()).ok()); }
        { EXPECT_TRUE(source_local_state->open(state.get()).ok()); }
    }

    void test_partition_sort(int partition_exprs_num, int topn_num) {
        std::vector<int64_t> data_val1;
        for (int j = 0; j < 5; j++) {
            for (int i = 0; i < 5; i++) {
                data_val1.push_back(i + 666);
            }
        }

        std::vector<int64_t> data_val2;
        for (int i = 0; i < 6; i++) {
            data_val2.push_back(i + 666);
        }
        vectorized::Block block = ColumnHelper::create_block<DataTypeInt64>(data_val1);
        EXPECT_TRUE(sink->sink(state.get(), &block, false));
        vectorized::Block block2 = ColumnHelper::create_block<DataTypeInt64>(data_val2);
        EXPECT_TRUE(sink->sink(state.get(), &block2, true));
        bool eos = false;
        Block output_block;
        EXPECT_TRUE(source->get_block(state.get(), &output_block, &eos).ok());

        if (partition_exprs_num == 0) {
            std::vector<int64_t> expect_vals;
            for (int i = 0; i < topn_num; i++) {
                expect_vals.push_back(data_val1[0]);
            }
            vectorized::Block result_block = ColumnHelper::create_block<DataTypeInt64>(expect_vals);
            EXPECT_TRUE(ColumnHelper::block_equal(result_block, output_block));
            EXPECT_EQ(output_block.rows(), topn_num);
        } else {
            EXPECT_EQ(output_block.rows(), topn_num);
        }
        std::cout << "source get block: \n" << output_block.dump_data() << std::endl;
    }

    void test_thread_mutex() {
        auto sink_func = [&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            Block block = ColumnHelper::create_block<DataTypeInt64>({1, 2, 3, 4});
            EXPECT_TRUE(sink->sink(state.get(), &block, true));
        };

        auto source_func = [&]() {
            bool eos = false;
            while (true) {
                if (is_ready(source_local_state->dependencies())) {
                    Block block;
                    EXPECT_TRUE(source->get_block(state.get(), &block, &eos).ok());
                    std::cout << "source block\n" << block.dump_data() << std::endl;
                    if (eos) {
                        break;
                    }
                }
            }
        };

        std::thread sink_thread(sink_func);
        std::thread source_thread(source_func);
        sink_thread.join();
        source_thread.join();
    }
};

TEST_F(PartitionSortOperatorTest, test) {
    for (int i = 0; i < 100; i++) {
        test_for_sink_and_source();
        test_thread_mutex();
    }
}

TEST_F(PartitionSortOperatorTest, test_no_partition) {
    int partition_exprs_num = 0;
    int topn_num = 3;
    test_for_sink_and_source(partition_exprs_num, true, 3);
    test_partition_sort(partition_exprs_num, topn_num);
}

TEST_F(PartitionSortOperatorTest, test_one_partition) {
    int partition_exprs_num = 1;
    int topn_num = 3;
    test_for_sink_and_source(partition_exprs_num, true, 3);
    test_partition_sort(partition_exprs_num, topn_num);
}

TEST_F(PartitionSortOperatorTest, TestWithoutKey) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>()};
    std::unique_ptr<PartitionedHashMapVariants> _variants =
            std::make_unique<PartitionedHashMapVariants>();
    _variants->init(types, HashKeyType::without_key);
    ASSERT_TRUE(std::holds_alternative<std::monostate>(_variants->method_variant));
}

TEST_F(PartitionSortOperatorTest, TestSerializedKey) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeString>()};
    std::unique_ptr<PartitionedHashMapVariants> _variants =
            std::make_unique<PartitionedHashMapVariants>();
    _variants->init(types, HashKeyType::serialized);
    ASSERT_TRUE(std::holds_alternative<vectorized::MethodSerialized<PartitionDataWithStringKey>>(
            _variants->method_variant));
}

TEST_F(PartitionSortOperatorTest, TestNumericKeys) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>()};
    std::unique_ptr<PartitionedHashMapVariants> _variants =
            std::make_unique<PartitionedHashMapVariants>();
    // Test int8 key
    _variants->init(types, HashKeyType::int8_key);
    auto value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt8, PartitionData<vectorized::UInt8>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int16 key
    _variants->init(types, HashKeyType::int16_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt16, PartitionData<vectorized::UInt16>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int32 key
    _variants->init(types, HashKeyType::int32_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt32, PartitionData<vectorized::UInt32>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int64 key
    _variants->init(types, HashKeyType::int64_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt64, PartitionData<vectorized::UInt64>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int128 key
    _variants->init(types, HashKeyType::int128_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt128, PartitionData<vectorized::UInt128>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test int256 key
    _variants->init(types, HashKeyType::int256_key);
    value = std::holds_alternative<
            vectorized::MethodOneNumber<vectorized::UInt256, PartitionData<vectorized::UInt256>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);
}

TEST_F(PartitionSortOperatorTest, TestNullableKeys) {
    auto nullable_type = std::make_shared<vectorized::DataTypeNullable>(
            std::make_shared<vectorized::DataTypeInt32>());
    std::vector<vectorized::DataTypePtr> types {nullable_type};
    std::unique_ptr<PartitionedHashMapVariants> _variants =
            std::make_unique<PartitionedHashMapVariants>();
    // Test nullable int32
    _variants->init(types, HashKeyType::int32_key);
    auto value = std::holds_alternative<
            vectorized::MethodSingleNullableColumn<vectorized::MethodOneNumber<
                    vectorized::UInt32, DataWithNullKey<PartitionData<vectorized::UInt32>>>>>(
            _variants->method_variant);
    ASSERT_TRUE(value);

    // Test nullable string
    _variants->init(types, HashKeyType::string_key);
    auto value2 = std::holds_alternative<vectorized::MethodSingleNullableColumn<
            vectorized::MethodStringNoCache<DataWithNullKey<PartitionDataWithShortStringKey>>>>(
            _variants->method_variant);
    ASSERT_TRUE(value2);

    // Test not nullable string
    auto string_type = std::make_shared<vectorized::DataTypeString>();
    std::vector<vectorized::DataTypePtr> types2 {string_type};
    _variants->init(types2, HashKeyType::string_key);
    auto value3 = std::holds_alternative<
            vectorized::MethodStringNoCache<PartitionDataWithShortStringKey>>(
            _variants->method_variant);
    ASSERT_TRUE(value3);
}

TEST_F(PartitionSortOperatorTest, TestFixedKeys) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>(),
                                                std::make_shared<vectorized::DataTypeInt32>()};
    std::unique_ptr<PartitionedHashMapVariants> _variants =
            std::make_unique<PartitionedHashMapVariants>();
    // Test fixed64
    _variants->init(types, HashKeyType::fixed64);
    ASSERT_TRUE(
            std::holds_alternative<vectorized::MethodKeysFixed<PartitionData<vectorized::UInt64>>>(
                    _variants->method_variant));

    // Test fixed128
    _variants->init(types, HashKeyType::fixed128);
    ASSERT_TRUE(
            std::holds_alternative<vectorized::MethodKeysFixed<PartitionData<vectorized::UInt128>>>(
                    _variants->method_variant));

    // Test fixed136
    _variants->init(types, HashKeyType::fixed136);
    ASSERT_TRUE(
            std::holds_alternative<vectorized::MethodKeysFixed<PartitionData<vectorized::UInt136>>>(
                    _variants->method_variant));

    // Test fixed256
    _variants->init(types, HashKeyType::fixed256);
    ASSERT_TRUE(
            std::holds_alternative<vectorized::MethodKeysFixed<PartitionData<vectorized::UInt256>>>(
                    _variants->method_variant));
}

TEST_F(PartitionSortOperatorTest, TestInvalidKeyType) {
    std::vector<vectorized::DataTypePtr> types {std::make_shared<vectorized::DataTypeInt32>()};
    std::unique_ptr<PartitionedHashMapVariants> _variants =
            std::make_unique<PartitionedHashMapVariants>();
    ASSERT_THROW(_variants->init(types, static_cast<HashKeyType>(-1)), Exception);
}

} // namespace doris::pipeline
