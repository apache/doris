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

#include "storage/iterator/olap_data_convertor.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_complex.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_hll.h"
#include "core/data_type/data_type_nullable.h"
#include "core/value/hll.h"
#include "storage/tablet/tablet_schema.h"
#include "util/slice.h"

namespace doris {
namespace {

Block create_nullable_hll_block(const std::vector<UInt8>& nullmap) {
    auto hll_column = ColumnHLL::create();
    auto nullmap_column = ColumnUInt8::create();
    for (size_t row = 0; row < nullmap.size(); ++row) {
        HyperLogLog hll;
        for (size_t value = 0; value <= row; ++value) {
            hll.update(row * 100 + value);
        }
        hll_column->insert_value(std::move(hll));
        nullmap_column->insert_value(nullmap[row]);
    }

    Block block;
    block.insert({ColumnNullable::create(std::move(hll_column), std::move(nullmap_column)),
                  make_nullable(std::make_shared<DataTypeHLL>()), "hll"});
    return block;
}

OlapBlockDataConvertor create_hll_convertor() {
    TabletColumn hll_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                            FieldType::OLAP_FIELD_TYPE_HLL, false);
    OlapBlockDataConvertor convertor;
    convertor.add_column_data_convertor(hll_column);
    return convertor;
}

TEST(OlapColumnDataConvertorHLLTest, ConvertsAllNonNullNullableColumn) {
    auto block = create_nullable_hll_block({0, 0, 0});
    auto convertor = create_hll_convertor();
    convertor.set_source_content(&block, 0, block.rows());

    auto [status, accessor] = convertor.convert_column_data(0);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, accessor->get_nullmap());

    const auto* slices = static_cast<const Slice*>(accessor->get_data());
    for (size_t row = 0; row < block.rows(); ++row) {
        EXPECT_EQ(0, accessor->get_nullmap()[row]);
        ASSERT_NE(nullptr, slices[row].data);
        ASSERT_GT(slices[row].size, 0);
        ASSERT_TRUE(HyperLogLog::is_valid(slices[row]));
        HyperLogLog deserialized(slices[row]);
        EXPECT_EQ(row + 1, deserialized.estimate_cardinality());
    }
}

TEST(OlapColumnDataConvertorHLLTest, ConvertsMixedNullsAtNonZeroRowPosition) {
    auto block = create_nullable_hll_block({1, 0, 1, 0, 1});
    auto convertor = create_hll_convertor();
    constexpr size_t row_pos = 1;
    constexpr size_t num_rows = 3;
    convertor.set_source_content(&block, row_pos, num_rows);

    auto [status, accessor] = convertor.convert_column_data(0);
    ASSERT_TRUE(status.ok()) << status;
    ASSERT_NE(nullptr, accessor->get_nullmap());
    EXPECT_EQ(0, accessor->get_nullmap()[0]);
    EXPECT_EQ(1, accessor->get_nullmap()[1]);
    EXPECT_EQ(0, accessor->get_nullmap()[2]);

    const auto* slices = static_cast<const Slice*>(accessor->get_data());
    ASSERT_NE(nullptr, slices[0].data);
    ASSERT_GT(slices[0].size, 0);
    EXPECT_EQ(nullptr, slices[1].data);
    EXPECT_EQ(0, slices[1].size);
    ASSERT_NE(nullptr, slices[2].data);
    ASSERT_GT(slices[2].size, 0);
    EXPECT_EQ(slices[0].data + slices[0].size, slices[2].data);

    ASSERT_TRUE(HyperLogLog::is_valid(slices[0]));
    ASSERT_TRUE(HyperLogLog::is_valid(slices[2]));
    HyperLogLog first(slices[0]);
    HyperLogLog third(slices[2]);
    EXPECT_EQ(row_pos + 1, first.estimate_cardinality());
    EXPECT_EQ(row_pos + 3, third.estimate_cardinality());
}

} // namespace
} // namespace doris
