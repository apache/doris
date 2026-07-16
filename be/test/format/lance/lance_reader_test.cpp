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

#include "format/lance/lance_reader.h"

#include <lance/lance.h>

#include <array>
#include <cassert>
#include <filesystem>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris {

namespace {

struct LanceDatasetInfo {
    int64_t version;
    std::vector<int64_t> fragment_ids;
};

Status get_lance_dataset_info(const std::filesystem::path& dataset_uri, LanceDatasetInfo* info) {
    if (!std::filesystem::exists(dataset_uri / "_versions") ||
        !std::filesystem::exists(dataset_uri / "data")) {
        return Status::InternalError("Lance fixture is incomplete: {}", dataset_uri.string());
    }

    std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)> dataset(
            lance_dataset_open(dataset_uri.c_str(), nullptr, 0), lance_dataset_close);
    if (dataset == nullptr) {
        return Status::InternalError("Failed to open Lance fixture: {}", dataset_uri.string());
    }

    info->version = static_cast<int64_t>(lance_dataset_version(dataset.get()));
    const auto fragment_count = lance_dataset_fragment_count(dataset.get());
    std::vector<uint64_t> raw_fragment_ids(fragment_count);
    if (const int result = lance_dataset_fragment_ids(dataset.get(), raw_fragment_ids.data());
        result != 0) {
        return Status::InternalError("Failed to read Lance fragment ids from {}: error {}",
                                     dataset_uri.string(), result);
    }

    info->fragment_ids.clear();
    info->fragment_ids.reserve(raw_fragment_ids.size());
    for (const uint64_t fragment_id : raw_fragment_ids) {
        info->fragment_ids.emplace_back(static_cast<int64_t>(fragment_id));
    }
    return Status::OK();
}

TFileRangeDesc make_lance_file_range(const std::filesystem::path& dataset_uri,
                                     const LanceDatasetInfo& dataset_info) {
    TFileRangeDesc range;
    range.__isset.table_format_params = true;
    range.table_format_params.__isset.lance_params = true;
    auto& params = range.table_format_params.lance_params;
    params.__set_dataset_uri(dataset_uri.string());
    params.__set_version(dataset_info.version);
    params.__set_fragment_ids(dataset_info.fragment_ids);
    return range;
}

Status get_lance_fixture_schema(const TFileRangeDesc& range, RuntimeProfile* profile,
                                std::vector<std::string>* names, std::vector<DataTypePtr>* types) {
    LanceReader schema_reader({}, nullptr, profile, range, nullptr);
    RETURN_IF_ERROR(schema_reader.init_schema_reader());
    Status schema_status = schema_reader.get_parsed_schema(names, types);
    Status close_status = schema_reader.close();
    return schema_status.ok() ? close_status : schema_status;
}

} // namespace

TEST(LanceReaderTypeTest, ReadsSparkGeneratedNumericTypesFixture) {
    // Data source: docker/thirdparties/docker-compose/iceberg/scripts/
    // create_preinstalled_scripts/lance/run02_create_numeric_types.sql.
    const std::filesystem::path dataset_uri = "./be/test/format/lance/data/numeric_types.lance";
    LanceDatasetInfo dataset_info;
    ASSERT_TRUE(get_lance_dataset_info(dataset_uri, &dataset_info).ok());
    ASSERT_GT(dataset_info.version, 0);
    ASSERT_EQ(3U, dataset_info.fragment_ids.size());
    std::cout << "version: " << dataset_info.version << std::endl;
    for (const int64_t fragment_id : dataset_info.fragment_ids) {
        std::cout << "fragment_id: " << fragment_id << std::endl;
    }
    TFileRangeDesc range = make_lance_file_range(dataset_uri, dataset_info);

    RuntimeProfile fixture_profile {"spark_numeric_types_fixture"};
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(get_lance_fixture_schema(range, &fixture_profile, &names, &types).ok());
    EXPECT_EQ(
            (std::vector<std::string> {"row_id", "bool_value", "tinyint_value", "smallint_value",
                                       "int_value", "bigint_value", "float_value", "double_value"}),
            names);
    ASSERT_EQ(8U, types.size());
    EXPECT_EQ(TYPE_BIGINT, types[0]->get_primitive_type());
    EXPECT_EQ(TYPE_BOOLEAN, types[1]->get_primitive_type());
    EXPECT_EQ(TYPE_TINYINT, types[2]->get_primitive_type());
    EXPECT_EQ(TYPE_SMALLINT, types[3]->get_primitive_type());
    EXPECT_EQ(TYPE_INT, types[4]->get_primitive_type());
    EXPECT_EQ(TYPE_BIGINT, types[5]->get_primitive_type());
    EXPECT_EQ(TYPE_FLOAT, types[6]->get_primitive_type());
    EXPECT_EQ(TYPE_DOUBLE, types[7]->get_primitive_type());
    ObjectPool fixture_pool;
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;
    const auto add_slot = [&tuple_builder](PrimitiveType type, bool nullable, const char* name,
                                           int position) {
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(type)
                                       .nullable(nullable)
                                       .column_name(name)
                                       .column_pos(position)
                                       .build());
    };
    add_slot(TYPE_BIGINT, false, "row_id", 0);
    add_slot(TYPE_BOOLEAN, true, "bool_value", 1);
    add_slot(TYPE_TINYINT, true, "tinyint_value", 2);
    add_slot(TYPE_SMALLINT, true, "smallint_value", 3);
    add_slot(TYPE_INT, true, "int_value", 4);
    add_slot(TYPE_BIGINT, true, "bigint_value", 5);
    add_slot(TYPE_FLOAT, true, "float_value", 6);
    add_slot(TYPE_DOUBLE, true, "double_value", 7);
    tuple_builder.build(&table_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&fixture_pool, table_builder.build(), &descriptor_table).ok());
    const auto& file_slots = descriptor_table->get_tuple_descriptor(0)->slots();

    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(query_globals);
    runtime_state.set_query_options(query_options);
    LanceReader reader(file_slots, &runtime_state, &fixture_profile, range, nullptr);
    ASSERT_TRUE(reader.init_reader().ok());

    Block block;
    const auto insert_column = [&block](PrimitiveType type, bool nullable, const char* name) {
        auto data_type = DataTypeFactory::instance().create_data_type(type, nullable);
        block.insert(ColumnWithTypeAndName(data_type->create_column(), std::move(data_type), name));
    };
    insert_column(TYPE_BIGINT, false, "row_id");
    insert_column(TYPE_BOOLEAN, true, "bool_value");
    insert_column(TYPE_TINYINT, true, "tinyint_value");
    insert_column(TYPE_SMALLINT, true, "smallint_value");
    insert_column(TYPE_INT, true, "int_value");
    insert_column(TYPE_BIGINT, true, "bigint_value");
    insert_column(TYPE_FLOAT, true, "float_value");
    insert_column(TYPE_DOUBLE, true, "double_value");

    size_t total_rows = 0;
    std::vector<size_t> block_row_counts;
    std::array<bool, 4> seen_rows {};
    bool eof = false;
    while (!eof) {
        block.clear_column_data(block.columns());
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        std::cout << "read_rows: " << read_rows << std::endl;
        if (read_rows == 0) {
            continue;
        }
        block_row_counts.emplace_back(read_rows);
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& bools = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& tinyints = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& smallints =
                assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
        const auto& ints = assert_cast<const ColumnNullable&>(*block.get_by_position(4).column);
        const auto& bigints = assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
        const auto& floats = assert_cast<const ColumnNullable&>(*block.get_by_position(6).column);
        const auto& doubles = assert_cast<const ColumnNullable&>(*block.get_by_position(7).column);

        const auto& bool_values = assert_cast<const ColumnUInt8&>(bools.get_nested_column());
        const auto& tinyint_values = assert_cast<const ColumnInt8&>(tinyints.get_nested_column());
        const auto& smallint_values =
                assert_cast<const ColumnInt16&>(smallints.get_nested_column());
        const auto& int_values = assert_cast<const ColumnInt32&>(ints.get_nested_column());
        const auto& bigint_values = assert_cast<const ColumnInt64&>(bigints.get_nested_column());
        const auto& float_values = assert_cast<const ColumnFloat32&>(floats.get_nested_column());
        const auto& double_values = assert_cast<const ColumnFloat64&>(doubles.get_nested_column());

        for (size_t row = 0; row < read_rows; ++row) {
            const int64_t row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 3);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 3) { // all data is null
                EXPECT_EQ(1, bools.get_null_map_data()[row]);
                EXPECT_EQ(1, tinyints.get_null_map_data()[row]);
                EXPECT_EQ(1, smallints.get_null_map_data()[row]);
                EXPECT_EQ(1, ints.get_null_map_data()[row]);
                EXPECT_EQ(1, bigints.get_null_map_data()[row]);
                EXPECT_EQ(1, floats.get_null_map_data()[row]);
                EXPECT_EQ(1, doubles.get_null_map_data()[row]);
                continue;
            }
            // not null
            EXPECT_EQ(0, bools.get_null_map_data()[row]);
            EXPECT_EQ(0, tinyints.get_null_map_data()[row]);
            EXPECT_EQ(0, smallints.get_null_map_data()[row]);
            EXPECT_EQ(0, ints.get_null_map_data()[row]);
            EXPECT_EQ(0, bigints.get_null_map_data()[row]);
            EXPECT_EQ(0, floats.get_null_map_data()[row]);
            EXPECT_EQ(0, doubles.get_null_map_data()[row]);

            if (row_id == 1) {
                EXPECT_EQ(1, bool_values.get_data()[row]);
                EXPECT_EQ(-128, tinyint_values.get_data()[row]);
                EXPECT_EQ(-32768, smallint_values.get_data()[row]);
                EXPECT_EQ(-2147483648, int_values.get_data()[row]);
                EXPECT_EQ(-9223372036854775807LL, bigint_values.get_data()[row]);
                EXPECT_FLOAT_EQ(-1.25F, float_values.get_data()[row]);
                EXPECT_DOUBLE_EQ(-1.25, double_values.get_data()[row]);
            } else if (row_id == 2) {
                EXPECT_EQ(0, bool_values.get_data()[row]);
                EXPECT_EQ(127, tinyint_values.get_data()[row]);
                EXPECT_EQ(32767, smallint_values.get_data()[row]);
                EXPECT_EQ(2147483647, int_values.get_data()[row]);
                EXPECT_EQ(9223372036854775807LL, bigint_values.get_data()[row]);
                EXPECT_FLOAT_EQ(3.5F, float_values.get_data()[row]);
                EXPECT_DOUBLE_EQ(3.5, double_values.get_data()[row]);
            } else {
                assert(false);
            }
        }
        total_rows += read_rows;
    }
    EXPECT_EQ(3U, total_rows);
    EXPECT_EQ((std::vector<size_t> {3}), block_row_counts);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceReaderTypeTest, ReadsPythonGeneratedDecimalTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/create_lance_decimal_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format/lance/data/python_decimal_types.lance";
    LanceDatasetInfo dataset_info;
    ASSERT_TRUE(get_lance_dataset_info(dataset_uri, &dataset_info).ok());
    ASSERT_GT(dataset_info.version, 0);
    ASSERT_EQ(1U, dataset_info.fragment_ids.size());
    TFileRangeDesc range = make_lance_file_range(dataset_uri, dataset_info);

    RuntimeProfile fixture_profile {"python_decimal_types_fixture"};
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(get_lance_fixture_schema(range, &fixture_profile, &names, &types).ok());
    EXPECT_EQ((std::vector<std::string> {"row_id", "decimal_1_0", "decimal_9_2", "decimal_10_0",
                                         "decimal_18_4", "decimal_19_0", "decimal_38_10",
                                         "decimal_39_4", "decimal_76_38"}),
              names);
    ASSERT_EQ(9U, types.size());
    EXPECT_EQ(TYPE_BIGINT, types[0]->get_primitive_type());
    const std::array<std::pair<PrimitiveType, std::pair<int, int>>, 8> expected_types {{
            {TYPE_DECIMAL32, {1, 0}},
            {TYPE_DECIMAL32, {9, 2}},
            {TYPE_DECIMAL64, {10, 0}},
            {TYPE_DECIMAL64, {18, 4}},
            {TYPE_DECIMAL128I, {19, 0}},
            {TYPE_DECIMAL128I, {38, 10}},
            {TYPE_DECIMAL256, {39, 4}},
            {TYPE_DECIMAL256, {76, 38}},
    }};
    for (size_t i = 0; i < expected_types.size(); ++i) {
        const auto [primitive_type, precision_scale] = expected_types[i];
        EXPECT_EQ(primitive_type, types[i + 1]->get_primitive_type());
        EXPECT_EQ(static_cast<UInt32>(precision_scale.first), types[i + 1]->get_precision());
        EXPECT_EQ(static_cast<UInt32>(precision_scale.second), types[i + 1]->get_scale());
    }
    ObjectPool fixture_pool;
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .nullable(false)
                                   .column_name("row_id")
                                   .column_pos(0)
                                   .build());
    for (size_t i = 1; i < types.size(); ++i) {
        TSlotDescriptorBuilder slot_builder;
        auto slot_type = types[i]->to_thrift();
        tuple_builder.add_slot(slot_builder.set_slotType(slot_type)
                                       .nullable(true)
                                       .column_name(names[i])
                                       .column_pos(static_cast<int>(i))
                                       .build());
    }
    tuple_builder.build(&table_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&fixture_pool, table_builder.build(), &descriptor_table).ok());
    const auto& file_slots = descriptor_table->get_tuple_descriptor(0)->slots();

    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(query_globals);
    runtime_state.set_query_options(query_options);
    LanceReader reader(file_slots, &runtime_state, &fixture_profile, range, nullptr);
    ASSERT_TRUE(reader.init_reader().ok());

    auto row_id_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    Block block;
    block.insert(ColumnWithTypeAndName(row_id_type->create_column(), row_id_type, "row_id"));
    for (size_t i = 1; i < types.size(); ++i) {
        block.insert(ColumnWithTypeAndName(types[i]->create_column(), types[i], names[i]));
    }

    // For every column, the entries are the values of row_id 1, 2 and 4.
    // row_id 3 is the explicit NULL row.
    const std::array<std::array<const char*, 3>, 8> expected_values {{
            {{"-9", "9", "0"}},
            {{"-9999999.99", "9999999.99", "0.00"}},
            {{"-9999999999", "9999999999", "0"}},
            {{"-99999999999999.9999", "99999999999999.9999", "0.0000"}},
            {{"-9999999999999999999", "9999999999999999999", "0"}},
            {{"-9999999999999999999999999999.9999999999", "9999999999999999999999999999.9999999999",
              "0.0000000000"}},
            {{"-99999999999999999999999999999999999.9999",
              "99999999999999999999999999999999999.9999", "0.0000"}},
            {{"-99999999999999999999999999999999999999.99999999999999999999999999999999999999",
              "99999999999999999999999999999999999999.99999999999999999999999999999999999999",
              "0.00000000000000000000000000000000000000"}},
    }};

    size_t total_rows = 0;
    std::array<bool, 5> seen_rows {};
    std::vector<size_t> block_row_counts;
    bool eof = false;
    while (!eof) {
        block.clear_column_data(block.columns());
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        if (read_rows == 0) {
            continue;
        }
        block_row_counts.emplace_back(read_rows);

        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < read_rows; ++row) {
            const int64_t row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;

            for (size_t column = 0; column < expected_types.size(); ++column) {
                const auto& decimal_column = assert_cast<const ColumnNullable&>(
                        *block.get_by_position(column + 1).column);
                if (row_id == 3) {
                    EXPECT_EQ(1, decimal_column.get_null_map_data()[row]);
                } else {
                    EXPECT_EQ(0, decimal_column.get_null_map_data()[row]);
                    const size_t expected_value_index = row_id == 1 ? 0 : row_id == 2 ? 1 : 2;
                    EXPECT_EQ(expected_values[column][expected_value_index],
                              types[column + 1]->to_string(decimal_column, row));
                }
            }
        }
        total_rows += read_rows;
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_EQ((std::vector<size_t> {4}), block_row_counts);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceReaderTypeTest, ReadsSparkGeneratedStringAndBinaryTypesFixture) {
    // Data source: docker/thirdparties/docker-compose/iceberg/scripts/
    // create_preinstalled_scripts/lance/run04_create_string_binary_types.sql.
    const std::filesystem::path dataset_uri =
            "./be/test/format/lance/data/string_binary_types.lance";
    LanceDatasetInfo dataset_info;
    ASSERT_TRUE(get_lance_dataset_info(dataset_uri, &dataset_info).ok());
    ASSERT_GT(dataset_info.version, 0);
    ASSERT_EQ(4U, dataset_info.fragment_ids.size());
    TFileRangeDesc range = make_lance_file_range(dataset_uri, dataset_info);

    RuntimeProfile fixture_profile {"spark_string_binary_types_fixture"};
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(get_lance_fixture_schema(range, &fixture_profile, &names, &types).ok());
    EXPECT_EQ((std::vector<std::string> {"row_id", "text_value", "binary_value"}), names);
    ASSERT_EQ(3U, types.size());
    EXPECT_EQ(TYPE_BIGINT, types[0]->get_primitive_type());
    EXPECT_EQ(TYPE_STRING, types[1]->get_primitive_type());
    EXPECT_EQ(TYPE_VARBINARY, types[2]->get_primitive_type());
    ObjectPool fixture_pool;
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;
    const auto add_slot = [&tuple_builder](PrimitiveType type, bool nullable, const char* name,
                                           int position) {
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(type)
                                       .nullable(nullable)
                                       .column_name(name)
                                       .column_pos(position)
                                       .build());
    };
    add_slot(TYPE_BIGINT, false, "row_id", 0);
    add_slot(TYPE_STRING, true, "text_value", 1);
    add_slot(TYPE_VARBINARY, true, "binary_value", 2);
    tuple_builder.build(&table_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&fixture_pool, table_builder.build(), &descriptor_table).ok());
    const auto& file_slots = descriptor_table->get_tuple_descriptor(0)->slots();

    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(query_globals);
    runtime_state.set_query_options(query_options);
    LanceReader reader(file_slots, &runtime_state, &fixture_profile, range, nullptr);
    ASSERT_TRUE(reader.init_reader().ok());

    auto row_id_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    auto text_type = DataTypeFactory::instance().create_data_type(TYPE_STRING, true);
    auto binary_type = DataTypeFactory::instance().create_data_type(TYPE_VARBINARY, true);
    Block block;
    block.insert(ColumnWithTypeAndName(row_id_type->create_column(), row_id_type, "row_id"));
    block.insert(ColumnWithTypeAndName(text_type->create_column(), text_type, "text_value"));
    block.insert(ColumnWithTypeAndName(binary_type->create_column(), binary_type, "binary_value"));

    size_t total_rows = 0;
    std::vector<size_t> block_row_counts;
    std::array<bool, 5> seen_rows {};
    bool eof = false;
    while (!eof) {
        block.clear_column_data(block.columns());
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        if (read_rows == 0) {
            continue;
        }
        block_row_counts.emplace_back(read_rows);

        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& text_values =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& binary_values =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < read_rows; ++row) {
            const int64_t row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;

            const auto text = text_values.get_nested_column().get_data_at(row).to_string();
            const auto binary = binary_values.get_nested_column().get_data_at(row).to_string();
            if (row_id == 1) {
                EXPECT_EQ(0, text_values.get_null_map_data()[row]);
                EXPECT_EQ(0, binary_values.get_null_map_data()[row]);
                EXPECT_EQ("", text);
                EXPECT_EQ("", binary);
            } else if (row_id == 2) {
                EXPECT_EQ(0, text_values.get_null_map_data()[row]);
                EXPECT_EQ(0, binary_values.get_null_map_data()[row]);
                EXPECT_EQ("Doris 与 Lance ��", text);
                EXPECT_EQ(std::string("\x00\x01\xFF\x7F", 4), binary);
            } else if (row_id == 3) {
                EXPECT_EQ(1, text_values.get_null_map_data()[row]);
                EXPECT_EQ(1, binary_values.get_null_map_data()[row]);
            } else {
                EXPECT_EQ(0, text_values.get_null_map_data()[row]);
                EXPECT_EQ(0, binary_values.get_null_map_data()[row]);
                EXPECT_EQ(
                        "a moderately long text value used to exercise variable-length Arrow "
                        "buffers",
                        text);
                EXPECT_EQ("lance", binary);
            }
        }
        total_rows += read_rows;
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_EQ((std::vector<size_t> {4}), block_row_counts);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceReaderTypeTest, ReadsPythonGeneratedComplexTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/compare_spark_complex_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format/lance/data/python_spark_complex_types.lance";
    LanceDatasetInfo dataset_info;
    ASSERT_TRUE(get_lance_dataset_info(dataset_uri, &dataset_info).ok());
    ASSERT_GT(dataset_info.version, 0);
    ASSERT_EQ(1U, dataset_info.fragment_ids.size());
    TFileRangeDesc range = make_lance_file_range(dataset_uri, dataset_info);

    RuntimeProfile fixture_profile {"python_complex_types_fixture"};
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(get_lance_fixture_schema(range, &fixture_profile, &names, &types).ok());
    EXPECT_EQ((std::vector<std::string> {"row_id", "int_array", "attributes", "profile", "visits",
                                         "scores_by_source"}),
              names);
    ASSERT_EQ(6U, types.size());
    EXPECT_EQ(TYPE_BIGINT, types[0]->get_primitive_type());
    EXPECT_EQ(TYPE_ARRAY, types[1]->get_primitive_type());
    EXPECT_EQ(TYPE_MAP, types[2]->get_primitive_type());
    EXPECT_EQ(TYPE_STRUCT, types[3]->get_primitive_type());
    EXPECT_EQ(TYPE_ARRAY, types[4]->get_primitive_type());
    EXPECT_EQ(TYPE_MAP, types[5]->get_primitive_type());
    ObjectPool fixture_pool;
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;
    const auto add_slot = [&tuple_builder](const DataTypePtr& type, bool nullable, const char* name,
                                           int position) {
        TSlotDescriptorBuilder slot_builder;
        auto slot_type = type->to_thrift();
        tuple_builder.add_slot(slot_builder.set_slotType(slot_type)
                                       .nullable(nullable)
                                       .column_name(name)
                                       .column_pos(position)
                                       .build());
    };
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .nullable(false)
                                   .column_name("row_id")
                                   .column_pos(0)
                                   .build());
    for (size_t i = 1; i < types.size(); ++i) {
        add_slot(types[i], true, names[i].c_str(), static_cast<int>(i));
    }
    tuple_builder.build(&table_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&fixture_pool, table_builder.build(), &descriptor_table).ok());
    const auto& file_slots = descriptor_table->get_tuple_descriptor(0)->slots();

    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(query_globals);
    runtime_state.set_query_options(query_options);
    LanceReader reader(file_slots, &runtime_state, &fixture_profile, range, nullptr);
    ASSERT_TRUE(reader.init_reader().ok());

    auto row_id_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    Block block;
    block.insert(ColumnWithTypeAndName(row_id_type->create_column(), row_id_type, "row_id"));
    for (size_t i = 1; i < types.size(); ++i) {
        block.insert(ColumnWithTypeAndName(types[i]->create_column(), types[i], names[i]));
    }

    size_t total_rows = 0;
    std::array<bool, 4> seen_rows {};
    bool eof = false;
    while (!eof) {
        block.clear_column_data(block.columns());
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        if (read_rows == 0) {
            continue;
        }

        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& attributes =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& profiles = assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
        const auto& scores = assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
        const auto& attribute_values =
                assert_cast<const ColumnMap&>(attributes.get_nested_column());
        const auto& attribute_keys =
                assert_cast<const ColumnNullable&>(attribute_values.get_keys());
        const auto& attribute_key_values =
                assert_cast<const ColumnString&>(attribute_keys.get_nested_column());
        const auto& attribute_items =
                assert_cast<const ColumnNullable&>(attribute_values.get_values());
        const auto& attribute_item_values =
                assert_cast<const ColumnInt32&>(attribute_items.get_nested_column());
        const auto& score_values = assert_cast<const ColumnMap&>(scores.get_nested_column());
        const auto& score_keys = assert_cast<const ColumnNullable&>(score_values.get_keys());
        const auto& score_key_values =
                assert_cast<const ColumnString&>(score_keys.get_nested_column());
        const auto& score_items = assert_cast<const ColumnNullable&>(score_values.get_values());
        const auto& score_arrays = assert_cast<const ColumnArray&>(score_items.get_nested_column());
        const auto& score_array_items = assert_cast<const ColumnNullable&>(score_arrays.get_data());
        const auto& score_array_values =
                assert_cast<const ColumnInt32&>(score_array_items.get_nested_column());
        const auto map_range = [](const ColumnMap& map, size_t row) {
            const auto& offsets = map.get_offsets();
            const size_t begin = row == 0 ? 0 : offsets[row - 1];
            return std::pair {begin, static_cast<size_t>(offsets[row])};
        };
        const auto array_range = [](const ColumnArray& array, size_t row) {
            const auto& offsets = array.get_offsets();
            const size_t begin = row == 0 ? 0 : offsets[row - 1];
            return std::pair {begin, static_cast<size_t>(offsets[row])};
        };
        for (size_t row = 0; row < read_rows; ++row) {
            const int64_t row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 3);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;

            if (row_id == 1) {
                EXPECT_EQ(0, attributes.get_null_map_data()[row]);
                const auto [attribute_begin, attribute_end] = map_range(attribute_values, row);
                ASSERT_EQ(2U, attribute_end - attribute_begin);
                EXPECT_EQ("views", attribute_key_values.get_data_at(attribute_begin).to_string());
                EXPECT_EQ(10, attribute_item_values.get_data()[attribute_begin]);
                EXPECT_EQ("likes",
                          attribute_key_values.get_data_at(attribute_begin + 1).to_string());
                EXPECT_EQ(2, attribute_item_values.get_data()[attribute_begin + 1]);

                EXPECT_EQ(0, scores.get_null_map_data()[row]);
                const auto [score_begin, score_end] = map_range(score_values, row);
                ASSERT_EQ(2U, score_end - score_begin);
                EXPECT_EQ("organic", score_key_values.get_data_at(score_begin).to_string());
                const auto [organic_begin, organic_end] = array_range(score_arrays, score_begin);
                ASSERT_EQ(2U, organic_end - organic_begin);
                EXPECT_EQ(1, score_array_values.get_data()[organic_begin]);
                EXPECT_EQ(2, score_array_values.get_data()[organic_begin + 1]);
                EXPECT_EQ("ad", score_key_values.get_data_at(score_begin + 1).to_string());
                const auto [ad_begin, ad_end] = array_range(score_arrays, score_begin + 1);
                ASSERT_EQ(1U, ad_end - ad_begin);
                EXPECT_EQ(3, score_array_values.get_data()[ad_begin]);
            } else if (row_id == 2) {
                const auto [attribute_begin, attribute_end] = map_range(attribute_values, row);
                EXPECT_EQ(attribute_begin, attribute_end);
                const auto [score_begin, score_end] = map_range(score_values, row);
                EXPECT_EQ(score_begin, score_end);
            } else {
                EXPECT_EQ(1, attributes.get_null_map_data()[row]);
                EXPECT_EQ(1, profiles.get_null_map_data()[row]);
                EXPECT_EQ(1, scores.get_null_map_data()[row]);
            }
        }
        total_rows += read_rows;
    }
    EXPECT_EQ(3U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceReaderTypeTest, ReadsSparkGeneratedVectorTypesFixture) {
    // Data source: docker/thirdparties/docker-compose/iceberg/scripts/
    // create_preinstalled_scripts/lance/run07_create_vector_types.sql.
    const std::filesystem::path dataset_uri = "./be/test/format/lance/data/vector_types.lance";
    LanceDatasetInfo dataset_info;
    ASSERT_TRUE(get_lance_dataset_info(dataset_uri, &dataset_info).ok());
    ASSERT_GT(dataset_info.version, 0);
    ASSERT_EQ(3U, dataset_info.fragment_ids.size());
    TFileRangeDesc range = make_lance_file_range(dataset_uri, dataset_info);

    RuntimeProfile fixture_profile {"spark_vector_types_fixture"};
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(get_lance_fixture_schema(range, &fixture_profile, &names, &types).ok());
    EXPECT_EQ((std::vector<std::string> {"row_id", "label", "embedding"}), names);
    ASSERT_EQ(3U, types.size());
    EXPECT_EQ(TYPE_BIGINT, types[0]->get_primitive_type());
    EXPECT_EQ(TYPE_STRING, types[1]->get_primitive_type());
    EXPECT_EQ(TYPE_ARRAY, types[2]->get_primitive_type());

    ObjectPool fixture_pool;
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;
    tuple_builder.add_slot(TSlotDescriptorBuilder()
                                   .type(TYPE_BIGINT)
                                   .nullable(false)
                                   .column_name("row_id")
                                   .column_pos(0)
                                   .build());
    for (size_t i = 1; i < types.size(); ++i) {
        TSlotDescriptorBuilder slot_builder;
        auto slot_type = types[i]->to_thrift();
        tuple_builder.add_slot(slot_builder.set_slotType(slot_type)
                                       .nullable(true)
                                       .column_name(names[i])
                                       .column_pos(static_cast<int>(i))
                                       .build());
    }
    tuple_builder.build(&table_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&fixture_pool, table_builder.build(), &descriptor_table).ok());
    const auto& file_slots = descriptor_table->get_tuple_descriptor(0)->slots();

    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState runtime_state(query_globals);
    runtime_state.set_query_options(query_options);
    LanceReader reader(file_slots, &runtime_state, &fixture_profile, range, nullptr);
    ASSERT_TRUE(reader.init_reader().ok());

    auto row_id_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    Block block;
    block.insert(ColumnWithTypeAndName(row_id_type->create_column(), row_id_type, "row_id"));
    block.insert(ColumnWithTypeAndName(types[1]->create_column(), types[1], "label"));
    block.insert(ColumnWithTypeAndName(types[2]->create_column(), types[2], "embedding"));

    size_t total_rows = 0;
    std::array<bool, 4> seen_rows {};
    std::vector<size_t> block_row_counts;
    bool eof = false;
    while (!eof) {
        block.clear_column_data(block.columns());
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        if (read_rows == 0) {
            continue;
        }
        block_row_counts.emplace_back(read_rows);

        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& labels = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& label_values = assert_cast<const ColumnString&>(labels.get_nested_column());
        const auto& embeddings =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& embedding_arrays =
                assert_cast<const ColumnArray&>(embeddings.get_nested_column());
        const auto& embedding_items =
                assert_cast<const ColumnNullable&>(embedding_arrays.get_data());
        const auto& embedding_values =
                assert_cast<const ColumnFloat32&>(embedding_items.get_nested_column());
        const auto array_range = [](const ColumnArray& array, size_t row) {
            const auto& offsets = array.get_offsets();
            const size_t begin = row == 0 ? 0 : offsets[row - 1];
            return std::pair {begin, static_cast<size_t>(offsets[row])};
        };
        const auto expect_embedding = [&](size_t row, const std::string& label,
                                          const std::array<float, 3>& expected) {
            EXPECT_EQ(0, labels.get_null_map_data()[row]);
            EXPECT_EQ(label, label_values.get_data_at(row).to_string());
            EXPECT_EQ(0, embeddings.get_null_map_data()[row]);
            const auto [begin, end] = array_range(embedding_arrays, row);
            ASSERT_EQ(3U, end - begin);
            for (size_t i = 0; i < expected.size(); ++i) {
                EXPECT_EQ(0, embedding_items.get_null_map_data()[begin + i]);
                EXPECT_FLOAT_EQ(expected[i], embedding_values.get_data()[begin + i]);
            }
        };

        for (size_t row = 0; row < read_rows; ++row) {
            const int64_t row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 3);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                expect_embedding(row, "origin", {0.0F, 0.0F, 0.0F});
            } else if (row_id == 2) {
                expect_embedding(row, "unit-x", {1.0F, 0.0F, 0.0F});
            } else {
                expect_embedding(row, "mixed", {-1.5F, 0.25F, 3.75F});
            }
        }
        total_rows += read_rows;
    }
    EXPECT_EQ(3U, total_rows);
    EXPECT_EQ((std::vector<size_t> {3}), block_row_counts);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceReaderTypeTest, ReadsSparkGeneratedTemporalTypesFixture) {
    // Data source: docker/thirdparties/docker-compose/iceberg/scripts/
    // create_preinstalled_scripts/lance/run05_create_temporal_types.sql.
    const std::filesystem::path dataset_uri = "./be/test/format/lance/data/temporal_types.lance";
    LanceDatasetInfo dataset_info;
    ASSERT_TRUE(get_lance_dataset_info(dataset_uri, &dataset_info).ok());
    ASSERT_GT(dataset_info.version, 0);
    ASSERT_EQ(4U, dataset_info.fragment_ids.size());
    TFileRangeDesc range = make_lance_file_range(dataset_uri, dataset_info);

    RuntimeProfile fixture_profile {"spark_temporal_types_fixture"};
    std::vector<std::string> names;
    std::vector<DataTypePtr> types;
    ASSERT_TRUE(get_lance_fixture_schema(range, &fixture_profile, &names, &types).ok());
    EXPECT_EQ((std::vector<std::string> {"row_id", "date_value", "timestamp_value"}), names);
    ASSERT_EQ(3U, types.size());
    EXPECT_EQ(TYPE_BIGINT, types[0]->get_primitive_type());
    EXPECT_EQ(TYPE_DATEV2, types[1]->get_primitive_type());
    EXPECT_EQ(TYPE_DATETIMEV2, types[2]->get_primitive_type());
    EXPECT_EQ(6U, types[2]->get_scale());
    ObjectPool fixture_pool;
    TDescriptorTableBuilder table_builder;
    TTupleDescriptorBuilder tuple_builder;
    const auto add_slot = [&tuple_builder](PrimitiveType type, bool nullable, const char* name,
                                           int position, int scale = 0) {
        TSlotDescriptorBuilder slot_builder;
        if (type == TYPE_DATETIMEV2) {
            auto slot_type = slot_builder.get_common_type(to_thrift(type));
            slot_type.types[0].scalar_type.__set_scale(scale);
            slot_builder.set_slotType(slot_type);
        } else {
            slot_builder.type(type);
        }
        tuple_builder.add_slot(
                slot_builder.nullable(nullable).column_name(name).column_pos(position).build());
    };
    add_slot(TYPE_BIGINT, false, "row_id", 0);
    add_slot(TYPE_DATEV2, true, "date_value", 1);
    add_slot(TYPE_DATETIMEV2, true, "timestamp_value", 2, 6);
    tuple_builder.build(&table_builder);

    DescriptorTbl* descriptor_table = nullptr;
    ASSERT_TRUE(
            DescriptorTbl::create(&fixture_pool, table_builder.build(), &descriptor_table).ok());
    const auto& file_slots = descriptor_table->get_tuple_descriptor(0)->slots();

    // Spark generated this fixture with spark.sql.session.timeZone=Asia/Shanghai (+08:00).
    // Load fixed-offset zones explicitly because this standalone BE UT does not start ExecEnv.
    TimezoneUtils::load_offsets_to_cache();
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    query_globals.__set_time_zone("+08:00");
    RuntimeState runtime_state(query_globals);
    runtime_state.set_query_options(query_options);
    LanceReader reader(file_slots, &runtime_state, &fixture_profile, range, nullptr);
    ASSERT_TRUE(reader.init_reader().ok());

    auto row_id_type = DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false);
    auto date_type = DataTypeFactory::instance().create_data_type(TYPE_DATEV2, true);
    auto timestamp_type = DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, true, 0, 6);
    Block block;
    block.insert(ColumnWithTypeAndName(row_id_type->create_column(), row_id_type, "row_id"));
    block.insert(ColumnWithTypeAndName(date_type->create_column(), date_type, "date_value"));
    block.insert(ColumnWithTypeAndName(timestamp_type->create_column(), timestamp_type,
                                       "timestamp_value"));

    size_t total_rows = 0;
    std::vector<size_t> block_row_counts;
    std::array<bool, 5> seen_rows {};
    bool eof = false;
    while (!eof) {
        block.clear_column_data(block.columns());
        size_t read_rows = 0;
        ASSERT_TRUE(reader.get_next_block(&block, &read_rows, &eof).ok());
        if (read_rows == 0) {
            continue;
        }
        block_row_counts.emplace_back(read_rows);

        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& dates = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& timestamps =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < read_rows; ++row) {
            const int64_t row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;

            if (row_id == 1) {
                EXPECT_EQ(0, dates.get_null_map_data()[row]);
                EXPECT_EQ(0, timestamps.get_null_map_data()[row]);
                EXPECT_EQ("1969-12-31", date_type->to_string(dates, row));
                EXPECT_EQ("1969-12-31 23:59:59.123456", timestamp_type->to_string(timestamps, row));
            } else if (row_id == 2) {
                EXPECT_EQ(0, dates.get_null_map_data()[row]);
                EXPECT_EQ(0, timestamps.get_null_map_data()[row]);
                EXPECT_EQ("1970-01-01", date_type->to_string(dates, row));
                EXPECT_EQ("1970-01-01 08:00:00.000001", timestamp_type->to_string(timestamps, row));
            } else if (row_id == 3) {
                EXPECT_EQ(0, dates.get_null_map_data()[row]);
                EXPECT_EQ(0, timestamps.get_null_map_data()[row]);
                EXPECT_EQ("2024-02-29", date_type->to_string(dates, row));
                EXPECT_EQ("2024-02-29 12:34:56.654321", timestamp_type->to_string(timestamps, row));
            } else {
                EXPECT_EQ(1, dates.get_null_map_data()[row]);
                EXPECT_EQ(1, timestamps.get_null_map_data()[row]);
            }
        }
        total_rows += read_rows;
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_EQ((std::vector<size_t> {4}), block_row_counts);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

} // namespace doris
