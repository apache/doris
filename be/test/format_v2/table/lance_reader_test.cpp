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

#include "format_v2/table/lance_reader.h"

#include <lance/lance.h>

#include <array>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/timezone_utils.h"

namespace doris::format::lance {
namespace {

using Columns = std::vector<ColumnDefinition>;

struct LanceFixtureInfo {
    int64_t version = 0;
    std::vector<int64_t> fragment_ids;
};

Status get_fixture_info(const std::filesystem::path& dataset_uri, LanceFixtureInfo* info) {
    std::unique_ptr<LanceDataset, decltype(&lance_dataset_close)> dataset(
            lance_dataset_open(dataset_uri.c_str(), nullptr, 0), lance_dataset_close);
    if (dataset == nullptr) {
        return Status::InternalError("Failed to open Lance fixture: {}", dataset_uri.string());
    }

    info->version = static_cast<int64_t>(lance_dataset_version(dataset.get()));
    const auto fragment_count = lance_dataset_fragment_count(dataset.get());
    std::vector<uint64_t> raw_fragment_ids(fragment_count);
    if (lance_dataset_fragment_ids(dataset.get(), raw_fragment_ids.data()) != 0) {
        return Status::InternalError("Failed to list Lance fragments from {}",
                                     dataset_uri.string());
    }
    info->fragment_ids.clear();
    info->fragment_ids.reserve(raw_fragment_ids.size());
    for (const auto fragment_id : raw_fragment_ids) {
        info->fragment_ids.emplace_back(static_cast<int64_t>(fragment_id));
    }
    return Status::OK();
}

TFileRangeDesc make_lance_range(const std::filesystem::path& dataset_uri, int64_t version,
                                std::vector<int64_t> fragment_ids) {
    TFileRangeDesc range;
    range.__set_format_type(TFileFormatType::FORMAT_LANCE);
    TLanceFileDesc lance_params;
    lance_params.__set_dataset_uri(dataset_uri.string());
    lance_params.__set_version(version);
    lance_params.__set_fragment_ids(std::move(fragment_ids));
    TTableFormatFileDesc table_params;
    table_params.__set_table_format_type("lance");
    table_params.__set_lance_params(std::move(lance_params));
    range.__set_table_format_params(std::move(table_params));
    return range;
}

ColumnDefinition projected_column(std::string name, DataTypePtr type) {
    return {
            .identifier = Field::create_field<TYPE_STRING>(name),
            .name = std::move(name),
            .type = std::move(type),
    };
}

ColumnDefinition projected_column(std::string name, PrimitiveType type, bool nullable) {
    return projected_column(std::move(name),
                            DataTypeFactory::instance().create_data_type(type, nullable));
}

void add_output_columns(Block* block, const Columns& columns) {
    for (const auto& column : columns) {
        block->insert({column.type->create_column(), column.type, column.name});
    }
}

Status init_reader(LanceTableReader* reader, const Columns& projected_columns,
                   RuntimeState* runtime_state, RuntimeProfile* profile) {
    return reader->init({
            .projected_columns = projected_columns,
            .conjuncts = {},
            .format = FileFormat::LANCE,
            .scan_params = nullptr,
            .io_ctx = nullptr,
            .runtime_state = runtime_state,
            .scanner_profile = profile,
    });
}

Status prepare_fixture(LanceTableReader* reader, const std::filesystem::path& dataset_uri,
                       const LanceFixtureInfo& fixture, std::vector<int64_t> fragment_ids) {
    return reader->prepare_split({
            .partition_values = {},
            .conjuncts = std::nullopt,
            .partition_prune_conjuncts = {},
            .all_runtime_filters_applied = true,
            .cache = nullptr,
            .current_range =
                    make_lance_range(dataset_uri, fixture.version, std::move(fragment_ids)),
            .current_split_format = FileFormat::LANCE,
            .global_rowid_context = std::nullopt,
    });
}

DataTypePtr nullable_type(PrimitiveType type, int precision = 0, int scale = 0) {
    return DataTypeFactory::instance().create_data_type(type, true, precision, scale);
}

std::pair<size_t, size_t> array_range(const ColumnArray& array, size_t row) {
    const auto& offsets = array.get_offsets();
    return {row == 0 ? 0 : static_cast<size_t>(offsets[row - 1]),
            static_cast<size_t>(offsets[row])};
}

std::pair<size_t, size_t> map_range(const ColumnMap& map, size_t row) {
    const auto& offsets = map.get_offsets();
    return {row == 0 ? 0 : static_cast<size_t>(offsets[row - 1]),
            static_cast<size_t>(offsets[row])};
}

} // namespace

TEST(LanceTableReaderTypeTest, ReadsNumericTypesFromAllTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/generate_all_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_EQ(3U, fixture.fragment_ids.size());

    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("bool_value", TYPE_BOOLEAN, true),
            projected_column("tinyint_value", TYPE_TINYINT, true),
            projected_column("smallint_value", TYPE_SMALLINT, true),
            projected_column("int_value", TYPE_INT, true),
            projected_column("bigint_value", TYPE_BIGINT, true),
            projected_column("float_value", TYPE_FLOAT, true),
            projected_column("double_value", TYPE_DOUBLE, true),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_numeric_types_fixture");
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    const auto read_split = [&](std::vector<int64_t> fragment_ids) {
        ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, std::move(fragment_ids)).ok());
        bool eos = false;
        while (!eos) {
            ASSERT_TRUE(reader.get_block(&block, &eos).ok());
            if (eos) {
                continue;
            }
            const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
            const auto& bools =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
            const auto& tinyints =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
            const auto& smallints =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
            const auto& ints = assert_cast<const ColumnNullable&>(*block.get_by_position(4).column);
            const auto& bigints =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
            const auto& floats =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(6).column);
            const auto& doubles =
                    assert_cast<const ColumnNullable&>(*block.get_by_position(7).column);
            const auto& bool_values = assert_cast<const ColumnUInt8&>(bools.get_nested_column());
            const auto& tinyint_values =
                    assert_cast<const ColumnInt8&>(tinyints.get_nested_column());
            const auto& smallint_values =
                    assert_cast<const ColumnInt16&>(smallints.get_nested_column());
            const auto& int_values = assert_cast<const ColumnInt32&>(ints.get_nested_column());
            const auto& bigint_values =
                    assert_cast<const ColumnInt64&>(bigints.get_nested_column());
            const auto& float_values =
                    assert_cast<const ColumnFloat32&>(floats.get_nested_column());
            const auto& double_values =
                    assert_cast<const ColumnFloat64&>(doubles.get_nested_column());
            for (size_t row = 0; row < block.rows(); ++row) {
                const auto row_id = row_ids.get_data()[row];
                ASSERT_GE(row_id, 1);
                ASSERT_LE(row_id, 4);
                EXPECT_FALSE(seen_rows[row_id]);
                seen_rows[row_id] = true;
                if (row_id == 3) {
                    EXPECT_EQ(1, bools.get_null_map_data()[row]);
                    EXPECT_EQ(1, tinyints.get_null_map_data()[row]);
                    EXPECT_EQ(1, smallints.get_null_map_data()[row]);
                    EXPECT_EQ(1, ints.get_null_map_data()[row]);
                    EXPECT_EQ(1, bigints.get_null_map_data()[row]);
                    EXPECT_EQ(1, floats.get_null_map_data()[row]);
                    EXPECT_EQ(1, doubles.get_null_map_data()[row]);
                } else {
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
                        EXPECT_EQ(0, bool_values.get_data()[row]);
                        EXPECT_EQ(0, tinyint_values.get_data()[row]);
                        EXPECT_EQ(0, smallint_values.get_data()[row]);
                        EXPECT_EQ(0, int_values.get_data()[row]);
                        EXPECT_EQ(0, bigint_values.get_data()[row]);
                        EXPECT_FLOAT_EQ(0.0F, float_values.get_data()[row]);
                        EXPECT_DOUBLE_EQ(0.0, double_values.get_data()[row]);
                    }
                }
            }
        }
    };
    read_split({fixture.fragment_ids[0]});
    read_split({fixture.fragment_ids[1], fixture.fragment_ids[2]});
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsDecimalTypesFromAllTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/generate_all_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_EQ(3U, fixture.fragment_ids.size());

    const std::array<std::tuple<const char*, PrimitiveType, int, int>, 8> decimal_specs {{
            {"decimal_1_0", TYPE_DECIMAL32, 1, 0},
            {"decimal_9_2", TYPE_DECIMAL32, 9, 2},
            {"decimal_10_0", TYPE_DECIMAL64, 10, 0},
            {"decimal_18_4", TYPE_DECIMAL64, 18, 4},
            {"decimal_19_0", TYPE_DECIMAL128I, 19, 0},
            {"decimal_38_10", TYPE_DECIMAL128I, 38, 10},
            {"decimal_39_4", TYPE_DECIMAL256, 39, 4},
            {"decimal_76_38", TYPE_DECIMAL256, 76, 38},
    }};
    Columns columns {projected_column("row_id", TYPE_BIGINT, false)};
    for (const auto& [name, type, precision, scale] : decimal_specs) {
        columns.emplace_back(projected_column(name, nullable_type(type, precision, scale)));
    }

    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_decimal_types_fixture");
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
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
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            for (size_t column = 0; column < decimal_specs.size(); ++column) {
                const auto& decimal_column = assert_cast<const ColumnNullable&>(
                        *block.get_by_position(column + 1).column);
                if (row_id == 3) {
                    EXPECT_EQ(1, decimal_column.get_null_map_data()[row]);
                } else {
                    EXPECT_EQ(0, decimal_column.get_null_map_data()[row]);
                    const auto expected_index = row_id == 1 ? 0 : row_id == 2 ? 1 : 2;
                    EXPECT_EQ(expected_values[column][expected_index],
                              columns[column + 1].type->to_string(decimal_column, row));
                }
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsStringAndBinaryTypesFromAllTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/generate_all_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_EQ(3U, fixture.fragment_ids.size());
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("text_value", TYPE_STRING, true),
            projected_column("binary_value", TYPE_VARBINARY, true),
    };

    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_string_binary_types_fixture");
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& texts = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& binaries = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 3) {
                EXPECT_EQ(1, texts.get_null_map_data()[row]);
                EXPECT_EQ(1, binaries.get_null_map_data()[row]);
                continue;
            }
            EXPECT_EQ(0, texts.get_null_map_data()[row]);
            EXPECT_EQ(0, binaries.get_null_map_data()[row]);
            const auto text = texts.get_nested_column().get_data_at(row).to_string();
            const auto binary = binaries.get_nested_column().get_data_at(row).to_string();
            if (row_id == 1) {
                EXPECT_EQ("", text);
                EXPECT_EQ("", binary);
            } else if (row_id == 2) {
                EXPECT_EQ("Doris 与 Lance ��", text);
                EXPECT_EQ(std::string("\x00\x01\xFF\x7F", 4), binary);
            } else {
                EXPECT_EQ(
                        "a moderately long text value used to exercise variable-length Arrow "
                        "buffers",
                        text);
                EXPECT_EQ("lance", binary);
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsComplexTypesFromAllTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/generate_all_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_EQ(3U, fixture.fragment_ids.size());

    const auto int_type = nullable_type(TYPE_INT);
    const auto string_type = nullable_type(TYPE_STRING);
    const auto int_array = make_nullable(std::make_shared<DataTypeArray>(int_type));
    const auto attributes = make_nullable(std::make_shared<DataTypeMap>(string_type, int_type));
    const auto profile = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {string_type, int_type}, Strings {"city", "level"}));
    const auto visit = make_nullable(std::make_shared<DataTypeStruct>(
            DataTypes {string_type, int_type}, Strings {"page", "duration_seconds"}));
    const auto visits = make_nullable(std::make_shared<DataTypeArray>(visit));
    const auto scores = make_nullable(std::make_shared<DataTypeMap>(
            string_type, make_nullable(std::make_shared<DataTypeArray>(int_type))));
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("int_array", int_array),
            projected_column("attributes", attributes),
            projected_column("profile", profile),
            projected_column("visits", visits),
            projected_column("scores_by_source", scores),
    };

    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile_stats("lance_complex_types_fixture");
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile_stats).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& int_arrays_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& attributes_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        const auto& profiles = assert_cast<const ColumnNullable&>(*block.get_by_position(3).column);
        const auto& visits_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(4).column);
        const auto& scores_column =
                assert_cast<const ColumnNullable&>(*block.get_by_position(5).column);
        const auto& int_arrays =
                assert_cast<const ColumnArray&>(int_arrays_column.get_nested_column());
        const auto& int_array_items = assert_cast<const ColumnNullable&>(int_arrays.get_data());
        const auto& int_array_values =
                assert_cast<const ColumnInt32&>(int_array_items.get_nested_column());
        const auto& attribute_map =
                assert_cast<const ColumnMap&>(attributes_column.get_nested_column());
        const auto& attribute_keys = assert_cast<const ColumnNullable&>(attribute_map.get_keys());
        const auto& attribute_key_values =
                assert_cast<const ColumnString&>(attribute_keys.get_nested_column());
        const auto& attribute_items =
                assert_cast<const ColumnNullable&>(attribute_map.get_values());
        const auto& attribute_item_values =
                assert_cast<const ColumnInt32&>(attribute_items.get_nested_column());
        const auto& profile_struct = assert_cast<const ColumnStruct&>(profiles.get_nested_column());
        const auto& profile_cities =
                assert_cast<const ColumnNullable&>(profile_struct.get_column(0));
        const auto& profile_city_values =
                assert_cast<const ColumnString&>(profile_cities.get_nested_column());
        const auto& profile_levels =
                assert_cast<const ColumnNullable&>(profile_struct.get_column(1));
        const auto& profile_level_values =
                assert_cast<const ColumnInt32&>(profile_levels.get_nested_column());
        const auto& visits = assert_cast<const ColumnArray&>(visits_column.get_nested_column());
        const auto& visit_items = assert_cast<const ColumnNullable&>(visits.get_data());
        const auto& visit_struct =
                assert_cast<const ColumnStruct&>(visit_items.get_nested_column());
        const auto& visit_pages = assert_cast<const ColumnNullable&>(visit_struct.get_column(0));
        const auto& visit_page_values =
                assert_cast<const ColumnString&>(visit_pages.get_nested_column());
        const auto& visit_durations =
                assert_cast<const ColumnNullable&>(visit_struct.get_column(1));
        const auto& visit_duration_values =
                assert_cast<const ColumnInt32&>(visit_durations.get_nested_column());
        const auto& score_map = assert_cast<const ColumnMap&>(scores_column.get_nested_column());
        const auto& score_keys = assert_cast<const ColumnNullable&>(score_map.get_keys());
        const auto& score_key_values =
                assert_cast<const ColumnString&>(score_keys.get_nested_column());
        const auto& score_items = assert_cast<const ColumnNullable&>(score_map.get_values());
        const auto& score_arrays = assert_cast<const ColumnArray&>(score_items.get_nested_column());
        const auto& score_array_items = assert_cast<const ColumnNullable&>(score_arrays.get_data());
        const auto& score_array_values =
                assert_cast<const ColumnInt32&>(score_array_items.get_nested_column());
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                EXPECT_EQ(0, int_arrays_column.get_null_map_data()[row]);
                const auto [int_begin, int_end] = array_range(int_arrays, row);
                ASSERT_EQ(3U, int_end - int_begin);
                EXPECT_EQ(0, int_array_items.get_null_map_data()[int_begin]);
                EXPECT_EQ(1, int_array_values.get_data()[int_begin]);
                EXPECT_EQ(1, int_array_items.get_null_map_data()[int_begin + 1]);
                EXPECT_EQ(0, int_array_items.get_null_map_data()[int_begin + 2]);
                EXPECT_EQ(3, int_array_values.get_data()[int_begin + 2]);

                EXPECT_EQ(0, attributes_column.get_null_map_data()[row]);
                const auto [attribute_begin, attribute_end] = map_range(attribute_map, row);
                ASSERT_EQ(2U, attribute_end - attribute_begin);
                EXPECT_EQ("views", attribute_key_values.get_data_at(attribute_begin).to_string());
                EXPECT_EQ(10, attribute_item_values.get_data()[attribute_begin]);
                EXPECT_EQ("likes",
                          attribute_key_values.get_data_at(attribute_begin + 1).to_string());
                EXPECT_EQ(2, attribute_item_values.get_data()[attribute_begin + 1]);

                EXPECT_EQ(0, profiles.get_null_map_data()[row]);
                EXPECT_EQ(0, profile_cities.get_null_map_data()[row]);
                EXPECT_EQ("Beijing", profile_city_values.get_data_at(row).to_string());
                EXPECT_EQ(0, profile_levels.get_null_map_data()[row]);
                EXPECT_EQ(7, profile_level_values.get_data()[row]);

                EXPECT_EQ(0, visits_column.get_null_map_data()[row]);
                const auto [visit_begin, visit_end] = array_range(visits, row);
                ASSERT_EQ(2U, visit_end - visit_begin);
                EXPECT_EQ(0, visit_items.get_null_map_data()[visit_begin]);
                EXPECT_EQ("home", visit_page_values.get_data_at(visit_begin).to_string());
                EXPECT_EQ(0, visit_durations.get_null_map_data()[visit_begin]);
                EXPECT_EQ(5, visit_duration_values.get_data()[visit_begin]);
                EXPECT_EQ(0, visit_items.get_null_map_data()[visit_begin + 1]);
                EXPECT_EQ("search", visit_page_values.get_data_at(visit_begin + 1).to_string());
                EXPECT_EQ(1, visit_durations.get_null_map_data()[visit_begin + 1]);

                EXPECT_EQ(0, scores_column.get_null_map_data()[row]);
                const auto [score_begin, score_end] = map_range(score_map, row);
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
                EXPECT_EQ(0, int_arrays_column.get_null_map_data()[row]);
                const auto [int_begin, int_end] = array_range(int_arrays, row);
                EXPECT_EQ(int_begin, int_end);

                EXPECT_EQ(0, attributes_column.get_null_map_data()[row]);
                const auto [attribute_begin, attribute_end] = map_range(attribute_map, row);
                EXPECT_EQ(attribute_begin, attribute_end);

                EXPECT_EQ(0, profiles.get_null_map_data()[row]);
                EXPECT_EQ(0, profile_cities.get_null_map_data()[row]);
                EXPECT_EQ("", profile_city_values.get_data_at(row).to_string());
                EXPECT_EQ(0, profile_levels.get_null_map_data()[row]);
                EXPECT_EQ(0, profile_level_values.get_data()[row]);

                EXPECT_EQ(0, visits_column.get_null_map_data()[row]);
                const auto [visit_begin, visit_end] = array_range(visits, row);
                EXPECT_EQ(visit_begin, visit_end);

                EXPECT_EQ(0, scores_column.get_null_map_data()[row]);
                const auto [score_begin, score_end] = map_range(score_map, row);
                EXPECT_EQ(score_begin, score_end);
            } else {
                EXPECT_EQ(1, int_arrays_column.get_null_map_data()[row]);
                EXPECT_EQ(1, attributes_column.get_null_map_data()[row]);
                EXPECT_EQ(1, profiles.get_null_map_data()[row]);
                EXPECT_EQ(1, visits_column.get_null_map_data()[row]);
                EXPECT_EQ(1, scores_column.get_null_map_data()[row]);
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsVectorTypesFromAllTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/generate_all_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_EQ(3U, fixture.fragment_ids.size());

    const auto embedding =
            make_nullable(std::make_shared<DataTypeArray>(nullable_type(TYPE_FLOAT)));
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("label", TYPE_STRING, true),
            projected_column("embedding", embedding),
    };
    TQueryOptions query_options;
    query_options.__set_batch_size(3);
    TQueryGlobals query_globals;
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_vector_types_fixture");
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
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
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                expect_embedding(row, "origin", {0.0F, 0.0F, 0.0F});
            } else if (row_id == 2) {
                expect_embedding(row, "unit-x", {1.0F, 0.0F, 0.0F});
            } else if (row_id == 3) {
                expect_embedding(row, "mixed", {-1.5F, 0.25F, 3.75F});
            } else {
                expect_embedding(row, "extra", {2.0F, -2.0F, 0.5F});
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

TEST(LanceTableReaderTypeTest, ReadsTemporalTypesFromAllTypesFixture) {
    // Data source: /mnt/disk2/zhangsida/test_lancedb/generate_all_types.py.
    const std::filesystem::path dataset_uri =
            "./be/test/format_v2/table/lance/data/all_types.lance";
    LanceFixtureInfo fixture;
    ASSERT_TRUE(get_fixture_info(dataset_uri, &fixture).ok());
    ASSERT_EQ(3U, fixture.fragment_ids.size());
    const Columns columns {
            projected_column("row_id", TYPE_BIGINT, false),
            projected_column("date_value", TYPE_DATEV2, true),
            projected_column("timestamp_value", nullable_type(TYPE_DATETIMEV2, 0, 6)),
    };

    // Spark generated this fixture with spark.sql.session.timeZone=Asia/Shanghai (+08:00).
    TimezoneUtils::load_offsets_to_cache();
    TQueryOptions query_options;
    query_options.__set_batch_size(4);
    TQueryGlobals query_globals;
    query_globals.__set_time_zone("+08:00");
    RuntimeState state(query_globals);
    state.set_query_options(query_options);
    RuntimeProfile profile("lance_temporal_types_fixture");
    LanceTableReader reader;
    ASSERT_TRUE(init_reader(&reader, columns, &state, &profile).ok());
    ASSERT_TRUE(prepare_fixture(&reader, dataset_uri, fixture, fixture.fragment_ids).ok());

    Block block;
    add_output_columns(&block, columns);
    std::array<bool, 5> seen_rows {};
    size_t total_rows = 0;
    bool eos = false;
    while (!eos) {
        ASSERT_TRUE(reader.get_block(&block, &eos).ok());
        if (eos) {
            continue;
        }
        const auto& row_ids = assert_cast<const ColumnInt64&>(*block.get_by_position(0).column);
        const auto& dates = assert_cast<const ColumnNullable&>(*block.get_by_position(1).column);
        const auto& timestamps =
                assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < block.rows(); ++row) {
            const auto row_id = row_ids.get_data()[row];
            ASSERT_GE(row_id, 1);
            ASSERT_LE(row_id, 4);
            EXPECT_FALSE(seen_rows[row_id]);
            seen_rows[row_id] = true;
            if (row_id == 1) {
                EXPECT_EQ("1969-12-31", columns[1].type->to_string(dates, row));
                EXPECT_EQ("1969-12-31 23:59:59.123456",
                          columns[2].type->to_string(timestamps, row));
            } else if (row_id == 2) {
                EXPECT_EQ("1970-01-01", columns[1].type->to_string(dates, row));
                EXPECT_EQ("1970-01-01 08:00:00.000001",
                          columns[2].type->to_string(timestamps, row));
            } else if (row_id == 3) {
                EXPECT_EQ("2024-02-29", columns[1].type->to_string(dates, row));
                EXPECT_EQ("2024-02-29 12:34:56.654321",
                          columns[2].type->to_string(timestamps, row));
            } else {
                EXPECT_EQ(1, dates.get_null_map_data()[row]);
                EXPECT_EQ(1, timestamps.get_null_map_data()[row]);
            }
        }
        total_rows += block.rows();
    }
    EXPECT_EQ(4U, total_rows);
    EXPECT_TRUE(seen_rows[1]);
    EXPECT_TRUE(seen_rows[2]);
    EXPECT_TRUE(seen_rows[3]);
    EXPECT_TRUE(seen_rows[4]);
    EXPECT_TRUE(reader.close().ok());
}

} // namespace doris::format::lance
