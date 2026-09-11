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

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "storage/schema.h"

namespace doris {
namespace {

TabletColumnPtr create_int_column(int32_t unique_id, std::string name, bool is_key = false) {
    auto column = std::make_shared<TabletColumn>(
            FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, FieldType::OLAP_FIELD_TYPE_INT,
            false, unique_id, sizeof(int32_t));
    column->set_name(std::move(name));
    column->set_is_key(is_key);
    return column;
}

TabletColumnPtr create_struct_column(int32_t unique_id) {
    auto column = std::make_shared<TabletColumn>();
    column->set_unique_id(unique_id);
    column->set_name("s");
    column->set_type(FieldType::OLAP_FIELD_TYPE_STRUCT);
    column->set_is_nullable(false);

    auto first_child = create_int_column(unique_id + 100, "a");
    auto second_child = create_int_column(unique_id + 101, "b");
    column->add_sub_column(*first_child);
    column->add_sub_column(*second_child);
    return column;
}

TEST(ReadSchemaTest, DefaultColumnsAreVisible) {
    std::vector<TabletColumnPtr> storage_columns {create_int_column(10, "k", true),
                                                  create_int_column(11, "dropped"),
                                                  create_struct_column(12)};

    ReadSchema read_schema(storage_columns);
    ASSERT_EQ(3, read_schema.num_block_columns());
    ASSERT_EQ(3, read_schema.num_read_columns());
    for (size_t ordinal = 0; ordinal < storage_columns.size(); ++ordinal) {
        EXPECT_EQ(storage_columns[ordinal].get(), read_schema.column(ordinal));
        EXPECT_TRUE(
                read_schema.data_type(ordinal)->equals(*storage_columns[ordinal]->get_vec_type()));
        EXPECT_EQ(static_cast<int32_t>(ordinal),
                  read_schema.ordinal_by_uid(storage_columns[ordinal]->unique_id()));
    }
}

TEST(ReadSchemaTest, ProjectionPreservesRequestedOrder) {
    std::vector<TabletColumnPtr> storage_columns {create_int_column(10, "k", true),
                                                  create_int_column(11, "dropped"),
                                                  create_struct_column(12)};

    ReadSchema read_schema(
            project_columns_by_ordinal(storage_columns, std::vector<ColumnId> {2, 0, 2}));
    ASSERT_EQ(3, read_schema.num_block_columns());
    ASSERT_EQ(3, read_schema.num_read_columns());
    EXPECT_EQ("s", read_schema.column(0)->name());
    EXPECT_EQ("k", read_schema.column(1)->name());
    EXPECT_EQ("s", read_schema.column(2)->name());
    EXPECT_EQ(0, read_schema.ordinal_by_uid(12));
    EXPECT_EQ(1, read_schema.ordinal_by_uid(10));
    EXPECT_EQ(-1, read_schema.ordinal_by_uid(11));
}

TEST(ReadSchemaTest, ExpectedTypesDefineReadBlock) {
    std::vector<TabletColumnPtr> read_columns {create_struct_column(12),
                                               create_int_column(10, "k", true)};

    auto pruned_struct_type = std::make_shared<DataTypeStruct>(
            DataTypes {std::make_shared<DataTypeInt32>()}, Strings {"a"});
    auto int_type = std::make_shared<DataTypeInt32>();
    ReadSchema read_schema(std::move(read_columns),
                           std::vector<DataTypePtr> {pruned_struct_type, int_type});
    EXPECT_FALSE(read_schema.column(0)->get_vec_type()->equals(*pruned_struct_type));

    Block block = read_schema.create_read_block();
    ASSERT_EQ(2, block.columns());
    EXPECT_EQ("s", block.get_by_position(0).name);
    EXPECT_EQ("k", block.get_by_position(1).name);
    EXPECT_TRUE(block.get_by_position(0).type->equals(*pruned_struct_type));
    EXPECT_TRUE(block.get_by_position(1).type->equals(*int_type));
}

TEST(ReadSchemaTest, AppendedDroppedColumnDoesNotExtendReadBlock) {
    std::vector<TabletColumnPtr> read_columns {create_int_column(10, "k", true),
                                               create_struct_column(12)};
    auto dropped_column = create_int_column(11, "dropped", true);
    ReadSchema read_schema(std::move(read_columns));

    EXPECT_EQ(1, read_schema.num_key_columns());
    read_schema.append_dropped_columns({*dropped_column});
    ColumnId suffix_ordinal = read_schema.ordinal_by_uid(11);
    EXPECT_EQ(2, suffix_ordinal);
    EXPECT_EQ(2, read_schema.num_block_columns());
    EXPECT_EQ(3, read_schema.num_read_columns());
    EXPECT_EQ(1, read_schema.num_key_columns());
    EXPECT_EQ(suffix_ordinal, read_schema.ordinal_by_uid(11));
    EXPECT_TRUE(read_schema.data_type(suffix_ordinal)->equals(*dropped_column->get_vec_type()));
    EXPECT_EQ(2, read_schema.create_read_block().columns());
}

TEST(ReadSchemaTest, RowBinlogMappingsUseExplicitReadOrdinals) {
    // Names and layout deliberately carry no row-binlog meaning. The supplied read ordinals are
    // the only source of truth for current/before and special-column relationships.
    ReadSchema read_schema({create_int_column(10, "key", true), create_int_column(11, "first"),
                            create_int_column(12, "second"), create_int_column(13, "third"),
                            create_int_column(14, "fourth"), create_int_column(15, "fifth"),
                            create_int_column(16, "sixth")});

    ASSERT_TRUE(read_schema.init_row_binlog_column_mappings({{4, 1}, {5, 6}}, 3, -1, 2).ok());

    EXPECT_TRUE(read_schema.row_binlog_value_pairs_complete());
    EXPECT_EQ(read_schema.row_binlog_value_column_pairs(),
              (ReadSchema::RowBinlogValueColumnPairs {{4, 1}, {5, 6}}));
    EXPECT_EQ(1, read_schema.before_column_ordinal(4));
    EXPECT_EQ(6, read_schema.before_column_ordinal(5));
    for (ColumnId ordinal : {0, 1, 2, 3, 6}) {
        EXPECT_EQ(ordinal, read_schema.before_column_ordinal(ordinal));
    }
    EXPECT_EQ(3, read_schema.tso_ordinal());
    EXPECT_EQ(-1, read_schema.lsn_ordinal());
    EXPECT_EQ(2, read_schema.op_ordinal());
}

TEST(ReadSchemaTest, RowBinlogSpecialOrdinalsUseTabletColumnUids) {
    TabletSchema tablet_schema;
    tablet_schema.append_column(*create_int_column(10, "key", true));
    tablet_schema.append_column(*create_int_column(11, BINLOG_TSO_COL));
    tablet_schema.append_column(*create_int_column(12, BINLOG_LSN_COL));
    tablet_schema.append_column(*create_int_column(13, BINLOG_OP_COL));

    ReadSchema read_schema(
            project_columns_by_ordinal(tablet_schema.columns(), std::vector<ColumnId> {3, 0, 1}));

    ASSERT_TRUE(read_schema.init_row_binlog_column_mappings({}, tablet_schema).ok());
    EXPECT_EQ(2, read_schema.tso_ordinal());
    EXPECT_EQ(-1, read_schema.lsn_ordinal());
    EXPECT_EQ(0, read_schema.op_ordinal());
}

TEST(ReadSchemaTest, RowBinlogMappingReportsIncompleteProjection) {
    ReadSchema read_schema({create_int_column(10, "key", true), create_int_column(11, "current"),
                            create_int_column(12, "unpaired"), create_int_column(13, "tso"),
                            create_int_column(14, "op")});

    ASSERT_TRUE(read_schema.init_row_binlog_column_mappings({}, 3, -1, 4).ok());

    EXPECT_FALSE(read_schema.row_binlog_value_pairs_complete());
    EXPECT_TRUE(read_schema.row_binlog_value_column_pairs().empty());
    EXPECT_EQ(1, read_schema.before_column_ordinal(1));
}

TEST(ReadSchemaTest, RowBinlogMappingRejectsInvalidOrdinals) {
    const std::vector<TabletColumnPtr> columns {
            create_int_column(10, "key", true), create_int_column(11, "before"),
            create_int_column(12, "op"),        create_int_column(13, "tso"),
            create_int_column(14, "current"),   create_int_column(15, "other")};
    const std::vector<ReadSchema::RowBinlogValueColumnPairs> invalid_pairs {
            {{6, 1}}, {{4, 4}}, {{4, 1}, {4, 5}}, {{4, 1}, {5, 1}}, {{4, 2}}, {{0, 1}}};

    for (const auto& pairs : invalid_pairs) {
        ReadSchema read_schema(columns);
        EXPECT_TRUE(read_schema.init_row_binlog_column_mappings(pairs, 3, -1, 2)
                            .is<ErrorCode::INVALID_ARGUMENT>());
    }

    ReadSchema read_schema(columns);
    EXPECT_TRUE(read_schema.init_row_binlog_column_mappings({{4, 1}}, 3, -1, 3)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(read_schema.init_row_binlog_column_mappings({{4, 1}}, 0, -1, 2)
                        .is<ErrorCode::INVALID_ARGUMENT>());
    EXPECT_TRUE(read_schema.init_row_binlog_column_mappings({{4, 1}}, 6, -1, 2)
                        .is<ErrorCode::INVALID_ARGUMENT>());

    auto mismatched_columns = columns;
    mismatched_columns[1] = create_struct_column(11);
    ReadSchema mismatched_read_schema(std::move(mismatched_columns));
    EXPECT_TRUE(mismatched_read_schema.init_row_binlog_column_mappings({{4, 1}}, 3, -1, 2)
                        .is<ErrorCode::INVALID_ARGUMENT>());
}

} // namespace
} // namespace doris
