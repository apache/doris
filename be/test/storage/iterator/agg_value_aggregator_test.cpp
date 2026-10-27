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

// AggValueAggregator folds key groups given as row ranges of merged Blocks: one group per
// range set, a group open across Blocks, NULLs skipped by the nullable functions, and whether
// one-row groups may be copied instead of folded.

#include <gtest/gtest.h>

#include <memory>
#include <optional>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_quantilestate.h"
#include "storage/iterator/agg_key_block_iterator.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

namespace {

TabletColumnPtr make_column(const std::string& name, FieldAggregationMethod aggregation,
                            FieldType type, bool is_nullable, int32_t unique_id, bool is_key) {
    auto column = std::make_shared<TabletColumn>(aggregation, type, is_nullable, unique_id,
                                                 sizeof(int64_t));
    column->set_name(name);
    column->set_is_key(is_key);
    return column;
}

// k BIGINT key, v BIGINT with `aggregation`, nullable when `nullable`.
ReadSchemaSPtr make_read_schema(FieldAggregationMethod aggregation, bool nullable = false) {
    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto value =
            make_column("v", aggregation, FieldType::OLAP_FIELD_TYPE_BIGINT, nullable, 1, false);
    DataTypePtr int_type = std::make_shared<DataTypeInt64>();
    DataTypePtr value_type = nullable ? make_nullable(int_type) : int_type;
    return std::make_shared<ReadSchema>(std::vector<TabletColumnPtr> {key, value},
                                        std::vector<DataTypePtr> {int_type, value_type});
}

// A Block of `values` in column 1 (NULL where nullopt) with key 1 in column 0.
Block make_block(const ReadSchema& schema, const std::vector<std::optional<int64_t>>& values) {
    Block block = schema.create_read_block();
    auto columns_guard = block.mutate_columns_scoped();
    auto& columns = columns_guard.mutable_columns();
    for (const auto& value : values) {
        assert_cast<ColumnInt64&>(*columns[0]).insert_value(1);
        if (auto* nullable = check_and_get_column<ColumnNullable>(*columns[1])) {
            nullable->get_null_map_data().push_back(value.has_value() ? 0 : 1);
            assert_cast<ColumnInt64&>(nullable->get_nested_column())
                    .insert_value(value.value_or(0));
        } else {
            assert_cast<ColumnInt64&>(*columns[1]).insert_value(*value);
        }
    }
    return block;
}

std::unique_ptr<AggValueAggregator> make_aggregator(const ReadSchema& schema) {
    auto aggregator = std::make_unique<AggValueAggregator>();
    EXPECT_TRUE(aggregator->init(schema, {1}, BeExecVersionManager::get_newest_version()).ok());
    return aggregator;
}

std::optional<int64_t> value_at(const IColumn& column, size_t row) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        if (nullable->is_null_at(row)) {
            return std::nullopt;
        }
        return assert_cast<const ColumnInt64&>(nullable->get_nested_column()).get_data()[row];
    }
    return assert_cast<const ColumnInt64&>(column).get_data()[row];
}

} // namespace

TEST(AggValueAggregatorTest, FoldsEachGroupFromItsRowRange) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM);
    auto aggregator = make_aggregator(*schema);
    Block block = make_block(*schema, {1, 2, 3, 4, 5, 6});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // Groups: rows [0,2) -> 1+2, rows [2,5) -> 3+4+5, row 5 -> 6.
    aggregator->start_block(block);
    aggregator->add_rows(block, 0, 2);
    aggregator->close_group(out);
    aggregator->add_rows(block, 2, 5);
    aggregator->close_group(out);
    aggregator->add_rows(block, 5, 6);
    aggregator->close_group(out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 3);
    EXPECT_EQ(value_at(*out[1], 1), 12);
    EXPECT_EQ(value_at(*out[1], 2), 6);
}

TEST(AggValueAggregatorTest, KeepsAGroupOpenAcrossBlocks) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX);
    auto aggregator = make_aggregator(*schema);
    Block first = make_block(*schema, {4, 9, 2});
    Block second = make_block(*schema, {7, 11});
    MutableColumns out = schema->create_read_block().mutate_columns();

    // One group from the last two rows of the first Block and the first row of the second.
    aggregator->start_block(first);
    aggregator->add_rows(first, 0, 1);
    aggregator->close_group(out);
    aggregator->add_rows(first, 1, 3);
    aggregator->start_block(second);
    aggregator->add_rows(second, 0, 1);
    aggregator->close_group(out);

    ASSERT_EQ(out[1]->size(), 2);
    EXPECT_EQ(value_at(*out[1], 0), 4);
    EXPECT_EQ(value_at(*out[1], 1), 9);
}

TEST(AggValueAggregatorTest, NullableSumSkipsNulls) {
    auto schema = make_read_schema(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                                   /*nullable=*/true);
    auto aggregator = make_aggregator(*schema);
    // The first Block holds NULLs, so the nullable function adds row by row; the second holds
    // none and takes the batch path.
    Block with_nulls = make_block(*schema, {1, std::nullopt, 3, std::nullopt, std::nullopt});
    Block without_nulls = make_block(*schema, {10, 20});
    MutableColumns out = schema->create_read_block().mutate_columns();

    aggregator->start_block(with_nulls);
    aggregator->add_rows(with_nulls, 0, 3);
    aggregator->close_group(out);
    aggregator->add_rows(with_nulls, 3, 5);
    aggregator->close_group(out);
    aggregator->start_block(without_nulls);
    aggregator->add_rows(without_nulls, 0, 2);
    aggregator->close_group(out);

    ASSERT_EQ(out[1]->size(), 3);
    EXPECT_EQ(value_at(*out[1], 0), 4);
    EXPECT_EQ(value_at(*out[1], 1), std::nullopt);
    EXPECT_EQ(value_at(*out[1], 2), 30);
}

TEST(AggValueAggregatorTest, OneRowGroupsAreCopiedOnlyForFunctionsReturningTheirInput) {
    for (auto aggregation : {FieldAggregationMethod::OLAP_FIELD_AGGREGATION_SUM,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MIN,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_MAX,
                             FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE_IF_NOT_NULL}) {
        auto schema = make_read_schema(aggregation);
        EXPECT_TRUE(make_aggregator(*schema)->returns_single_row_unchanged())
                << static_cast<int>(aggregation);
    }

    auto key = make_column("k", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                           FieldType::OLAP_FIELD_TYPE_BIGINT, false, 0, true);
    auto quantile = make_column("q", FieldAggregationMethod::OLAP_FIELD_AGGREGATION_QUANTILE_UNION,
                                FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE, false, 1, false);
    ReadSchema schema(std::vector<TabletColumnPtr> {key, quantile},
                      std::vector<DataTypePtr> {std::make_shared<DataTypeInt64>(),
                                                std::make_shared<DataTypeQuantileState>()});
    EXPECT_FALSE(make_aggregator(schema)->returns_single_row_unchanged());
}

} // namespace doris
