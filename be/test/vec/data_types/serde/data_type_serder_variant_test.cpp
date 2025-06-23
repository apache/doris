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
#include <rapidjson/document.h>

#include <memory>
#include <string>

#include "common/status.h"
#include "testutil/test_util.h"
#include "vec/columns/column.h"
#include "vec/columns/column_variant.h"
#include "vec/columns/common_column_test.h"
#include "vec/data_types/common_data_type_serder_test.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/data_types/serde/data_type_object_serde.h"
#include "vec/exec/format/orc/orc_memory_pool.h"

namespace doris::vectorized {
static std::string root_dir;
static std::string test_data_dir;
static std::vector<std::string> json_files;
static auto obj_serde = std::make_shared<DataTypeVariantSerDe>();
static auto column_variant = ColumnVariant::create(2, true);

class DataTypeVariantSerDeTest : public ::testing::Test {
protected:
    void SetUp() override {
        root_dir = std::string(getenv("ROOT"));
        test_data_dir = root_dir + "/be/test/data/vec/data_types";

        load_columns_data();
    }

    static void load_columns_data() {
        std::cout << "loading test dataset" << std::endl;
        column_variant->clear();
        MutableColumns columns;
        columns.push_back(column_variant->get_ptr());
        auto test_data_dir_json = root_dir + "/regression-test/data/nereids_function_p0/";
        json_files = {
                test_data_dir_json + "json_variant/boolean_boundary.jsonl",
                test_data_dir_json + "json_variant/null_boundary.jsonl",
                test_data_dir_json + "json_variant/number_boundary.jsonl",
                test_data_dir_json + "json_variant/string_boundary.jsonl",
                test_data_dir_json + "json_variant/array_boolean_boundary.jsonl",
                test_data_dir_json + "json_variant/array_nullable_null_boundary.jsonl",
                test_data_dir_json + "json_variant/array_number_boundary.jsonl",
                test_data_dir_json + "json_variant/array_string_boundary.jsonl",
                test_data_dir_json + "json_variant/array_object_boundary.jsonl",
                test_data_dir_json + "json_variant/array_nullable_boolean_boundary.jsonl",
                test_data_dir_json + "json_variant/array_nullable_number_boundary.jsonl",
                test_data_dir_json + "json_variant/array_nullable_string_boundary.jsonl",
                test_data_dir_json + "json_variant/array_nullable_object_boundary.jsonl",
                test_data_dir_json + "json_variant/array_array_boolean_boundary.jsonl",
                test_data_dir_json + "json_variant/array_array_number_boundary.jsonl",
                test_data_dir_json +
                        "json_variant/array_nullable_array_nullable_boolean_boundary.jsonl",
                test_data_dir_json +
                        "json_variant/array_nullable_array_nullable_null_boundary.jsonl",
                test_data_dir_json +
                        "json_variant/array_nullable_array_nullable_number_boundary.jsonl",
                test_data_dir_json + "json_variant/object_boundary.jsonl",
                test_data_dir_json + "json_variant/object_nested_100.jsonl",
                test_data_dir_json + "json_variant/object_nested_1025.jsonl",
        };

        DataTypeSerDeSPtrs serdes = {obj_serde};
        for (const auto& json_file : json_files) {
            load_columns_data_from_file(columns, serdes, '\n', {0}, json_file);
            EXPECT_TRUE(!column_variant->empty());
            column_variant->insert_default();
            std::cout << "column variant size: " << column_variant->size() << std::endl;
        }
        column_variant->finalize();
    }
};

TEST_F(DataTypeVariantSerDeTest, SerdeHiveTextAndJsonFormatTest) {
    // insert from data csv and assert insert result
    MutableColumns obj_cols;
    MutableColumns obj_cols2;
    obj_cols.push_back(ColumnVariant::create(2, true)->get_ptr());
    obj_cols2.push_back(ColumnVariant::create(2, true)->get_ptr());
    obj_cols[0]->finalize();
    obj_cols2[0]->finalize();
    // for loop json_files
    // which will throw out of range exception
    //    for (int j = 0; j < json_files.size(); j++) {
    //        CommonDataTypeSerdeTest::load_data_and_assert_from_csv<true, true>(
    //                {obj_serde}, obj_cols, json_files[j], ';', {0});
    //        CommonDataTypeSerdeTest::load_data_and_assert_from_csv<false, true>(
    //                {obj_serde}, obj_cols2, json_files[j], ';', {0});
    //        obj_cols[0]->finalize();
    //        obj_cols2[0]->finalize();
    //        CommonColumnTest::checkColumn(*obj_cols[0], *obj_cols2[0], obj_cols[0]->size());
    //    }
}

TEST_F(DataTypeVariantSerDeTest, SerdePbTest) {
    MutableColumns cols;
    cols.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes;
    serdes.push_back(obj_serde);
    CommonDataTypeSerdeTest::assert_pb_format(cols, serdes);
}

TEST_F(DataTypeVariantSerDeTest, SerdeJsonbTest) {
    MutableColumns cols;
    cols.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes;
    serdes.push_back(obj_serde);
    CommonDataTypeSerdeTest::assert_jsonb_format(cols, serdes);
}

TEST_F(DataTypeVariantSerDeTest, SerdeMysqlTest) {
    // insert from data csv and assert insert result
    MutableColumns cols;
    cols.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes;
    serdes.push_back(obj_serde);
    CommonDataTypeSerdeTest::assert_mysql_format(cols, serdes);
}

TEST_F(DataTypeVariantSerDeTest, SerdeArrowTest) {
    MutableColumns cols;
    cols.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes;
    serdes.push_back(obj_serde);
    DataTypes types {std::make_shared<DataTypeVariant>()};
    // read_column_from_arrow not implemented
    EXPECT_ANY_THROW(CommonDataTypeSerdeTest::assert_arrow_format(cols, types));
}

TEST_F(DataTypeVariantSerDeTest, OrcOperations) {
    // Test write_column_to_orc
    {
        std::unique_ptr<orc::MemoryPool> orc_pool(new ORCMemoryPool());
        orc::StringVectorBatch batch(uint64_t(1024), *orc_pool);
        batch.notNull.resize(column_variant->size());
        NullMap null_map;
        null_map.resize(column_variant->size(), 0);
        std::vector<StringRef> buffer_list;
        Defer defer {[&]() {
            for (auto& bufferRef : buffer_list) {
                if (bufferRef.data) {
                    free(const_cast<char*>(bufferRef.data));
                }
            }
        }};
        auto status = obj_serde->write_column_to_orc("UTC", *column_variant, &null_map, &batch, 0,
                                                     column_variant->size(), buffer_list);
        EXPECT_TRUE(status.ok());
    }
}

TEST_F(DataTypeVariantSerDeTest, DeserializeJsonVectorTest) {
    // Create test data
    std::vector<Slice> json_slices = {
            Slice("{\"a\": 1, \"b\": \"test\"}"),
            Slice("{\"arr\": [1,2,3], \"obj\": {\"x\": true}}"), Slice("null"),
            Slice("{\"nested\": {\"arr\": [1,2,3], \"str\": \"hello\"}}"), Slice("[1,2,3]")};

    std::vector<Slice> expect_json_slices = {
            Slice("{}"),
            Slice("{\"a\":1,\"b\":\"test\"}"),
            Slice("{\"arr\": [1,2,3], \"obj\": {\"x\":1}}"),
            Slice("{}"),
            Slice("{\"nested\":{\"arr\":[1,2,3],\"str\":\"hello\"}}"),
            Slice("[1,2,3]")};

    // Create a new column for testing
    auto test_column = ColumnVariant::create(2, true);
    uint64_t num_deserialized = 0;
    DataTypeVariantSerDe::FormatOptions options;

    // Test deserialize_column_from_json_vector
    auto status = obj_serde->deserialize_column_from_json_vector(*test_column, json_slices,
                                                                 &num_deserialized, options);
    test_column->finalize(ColumnVariant::FinalizeMode::WRITE_MODE);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(num_deserialized, json_slices.size());
    EXPECT_EQ(test_column->size(), json_slices.size() + 1); // for root

    // Verify the deserialized data by serializing it back
    for (size_t i = 0; i < test_column->size(); ++i) {
        std::string serialized;
        serialized.clear();
        test_column->serialize_one_row_to_string(i, &serialized);
        // Remove whitespace and newlines for comparison
        serialized.erase(std::remove_if(serialized.begin(), serialized.end(),
                                        [](unsigned char x) { return std::isspace(x); }),
                         serialized.end());

        std::string expected(expect_json_slices[i].data, expect_json_slices[i].size);
        expected.erase(std::remove_if(expected.begin(), expected.end(),
                                      [](unsigned char x) { return std::isspace(x); }),
                       expected.end());

        EXPECT_EQ(serialized, expected) << "Mismatch at index " << i;
    }
}

TEST_F(DataTypeVariantSerDeTest, ErrorMsg) {
    DataTypeVariantSerDe::FormatOptions options;
    MutableColumns cols;
    cols.push_back(column_variant->get_ptr());
    DataTypeSerDeSPtrs serdes;
    serdes.push_back(obj_serde);
    JsonbWriterT<JsonbOutStream> jsonb_writer;
    jsonb_writer.writeInt(Int128(1));
    EXPECT_ANY_THROW(serdes[0]->read_one_cell_from_jsonb(*cols[0], jsonb_writer.getValue()));
}

} // namespace doris::vectorized