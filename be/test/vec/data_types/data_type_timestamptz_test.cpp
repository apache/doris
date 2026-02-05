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

#include "vec/data_types/data_type_timestamptz.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>

#include "testutil/column_helper.h"
#include "testutil/datetime_ut_util.h"
#include "testutil/mock/mock_runtime_state.h"
#include "testutil/mock/mock_slot_ref.h"
#include "vec/common/sort/sorter.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/common_data_type_test.h"
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_varbinary.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/runtime/timestamptz_value.h"

namespace doris::vectorized {

class DataTypeTimeStampTzTest : public ::testing::Test {
public:
    void SetUp() override {
        type = std::make_shared<DataTypeTimeStampTz>();
        serder = type->get_serde();
    }

    std::shared_ptr<IDataType> type;
    DataTypeSerDeSPtr serder;
    MockRuntimeState _state;
};

TEST_F(DataTypeTimeStampTzTest, test_normal) {
    EXPECT_EQ(type->get_family_name(), "TimeStampTz");
    EXPECT_EQ(type->get_primitive_type(), PrimitiveType::TYPE_TIMESTAMPTZ);
    EXPECT_EQ(type->get_scale(), 6);

    auto other_type = std::make_shared<DataTypeTimeStampTz>();
    EXPECT_TRUE(type->equals(*other_type));
    EXPECT_TRUE(type->equals_ignore_precision(*other_type));
}

TEST_F(DataTypeTimeStampTzTest, test_serder) {
    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(8));
    TimezoneUtils::load_offsets_to_cache();

    DataTypeSerDe::FormatOptions options;
    options.timezone = &time_zone;

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:00:00 +08:00"};
        EXPECT_TRUE(serder->from_string(str, *column, options).ok());
        EXPECT_EQ(column->size(), 1);
        auto& col_data = assert_cast<ColumnTimeStampTz&>(*column);
        EXPECT_EQ(col_data.get_data()[0].to_date_int_val(), 142430890880925696);
    }

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:abc:00 +08:00"};
        EXPECT_FALSE(serder->from_string(str, *column, options).ok());
    }

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:00:00 +08:00"};
        EXPECT_TRUE(serder->from_string_strict_mode(str, *column, options).ok());
        EXPECT_EQ(column->size(), 1);
        auto& col_data = assert_cast<ColumnTimeStampTz&>(*column);
        EXPECT_EQ(col_data.get_data()[0].to_date_int_val(), 142430890880925696);
    }

    {
        auto column = type->create_column();
        StringRef str {"2024-01-01 12:abc:00 +08:00"};
        EXPECT_FALSE(serder->from_string_strict_mode(str, *column, options).ok());
    }
}

TEST_F(DataTypeTimeStampTzTest, test_sort) {
    MockRuntimeState _state;
    RuntimeProfile _profile {"test"};

    std::unique_ptr<FullSorter> sorter;

    std::unique_ptr<MockRowDescriptor> row_desc;

    ObjectPool pool;

    VSortExecExprs sort_exec_exprs;

    std::vector<bool> is_asc_order {true};
    std::vector<bool> nulls_first {false};

    row_desc.reset(new MockRowDescriptor({std::make_shared<DataTypeTimeStampTz>()}, &pool));

    sort_exec_exprs._sort_tuple_slot_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeTimeStampTz>());

    sort_exec_exprs._materialize_tuple = false;

    sort_exec_exprs._ordering_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeTimeStampTz>());

    sort_exec_exprs._sort_tuple_slot_expr_ctxs =
            MockSlotRef::create_mock_contexts(0, std::make_shared<DataTypeTimeStampTz>());

    sorter = FullSorter::create_unique(sort_exec_exprs, 3, 3, &pool, is_asc_order, nulls_first,
                                       *row_desc, &_state, nullptr);
    sorter->init_profile(&_profile);
    {
        Block block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(2024, 1, 1, 4, 0, 0, 0), make_timestamptz(2023, 1, 1, 4, 0, 0, 0),
                 make_timestamptz(2021, 1, 1, 4, 0, 0, 0)});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    {
        Block block = ColumnHelper::create_block<DataTypeTimeStampTz>(
                {make_timestamptz(2022, 1, 1, 4, 0, 0, 0), make_timestamptz(2055, 1, 1, 4, 0, 0, 0),
                 make_timestamptz(2021, 1, 4, 4, 0, 0, 0)});
        EXPECT_TRUE(sorter->append_block(&block).ok());
    }

    EXPECT_TRUE(sorter->prepare_for_read(false).ok());

    cctz::time_zone time_zone = cctz::fixed_time_zone(std::chrono::hours(8));
    TimezoneUtils::load_offsets_to_cache();

    MutableBlock sorted_block(VectorizedUtils::create_columns_with_type_and_name(*row_desc));

    {
        Block output_block;
        bool eos = false;

        while (!eos) {
            output_block.clear();
            EXPECT_TRUE(sorter->get_next(&_state, &output_block, &eos).ok());

            auto& col = output_block.get_by_position(0);
            const auto& column = assert_cast<const ColumnTimeStampTz&>(*col.column);

            for (size_t i = 0; i < column.size(); ++i) {
                std::cout << TimestampTzValue {column.get_data()[i]}.to_string(time_zone)
                          << std::endl;
            }

            EXPECT_TRUE(sorted_block.merge(std::move(output_block)));
        }
    }

    auto result_block = sorted_block.to_block();
    const auto* result_column =
            assert_cast<const ColumnTimeStampTz*>(result_block.get_by_position(0).column.get());

    EXPECT_EQ(result_column->get_element(0), make_timestamptz(2023, 1, 1, 4, 0, 0, 0));
    EXPECT_EQ(result_column->get_element(1), make_timestamptz(2024, 1, 1, 4, 0, 0, 0));
    EXPECT_EQ(result_column->get_element(2), make_timestamptz(2055, 1, 1, 4, 0, 0, 0));
}

} // namespace doris::vectorized