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

#include <memory>
#include <string>

#include "common/config.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "runtime/define_primitive_type.h"
#include "runtime/runtime_state.h"
#include "util/uid_util.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_struct.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_struct.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/data_types/serde/data_type_serde.h"
#include "vec/exec/format/native/native_reader.h"
#include "vec/runtime/vnative_transformer.h"

namespace doris::vectorized {

class NativeReaderWriterTest : public ::testing::Test {};

static void fill_primitive_columns(Block& block, size_t rows) {
    DataTypePtr int_type =
            make_nullable(DataTypeFactory::instance().create_data_type(TYPE_INT, false));
    DataTypePtr str_type =
            make_nullable(DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false));

    {
        MutableColumnPtr col = int_type->create_column();
        for (size_t i = 0; i < rows; ++i) {
            if (i % 3 == 0) {
                // null
                col->insert_default();
            } else {
                // insert int value via Field to match column interface
                auto field =
                        Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(i * 10));
                col->insert(field);
            }
        }
        block.insert(ColumnWithTypeAndName(std::move(col), int_type, "int_col"));
    }

    {
        MutableColumnPtr col = str_type->create_column();
        for (size_t i = 0; i < rows; ++i) {
            if (i % 4 == 0) {
                col->insert_default();
            } else {
                std::string v = "s" + std::to_string(i);
                // insert varchar value via Field
                auto field = Field::create_field<PrimitiveType::TYPE_VARCHAR>(v);
                col->insert(field);
            }
        }
        block.insert(ColumnWithTypeAndName(std::move(col), str_type, "str_col"));
    }
}

static void fill_array_column(Block& block, size_t rows) {
    // array<int>
    DataTypePtr arr_nested_type = DataTypeFactory::instance().create_data_type(TYPE_INT, false);
    DataTypePtr arr_type = make_nullable(std::make_shared<DataTypeArray>(arr_nested_type));

    {
        MutableColumnPtr col = arr_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();
        auto& null_map = nullable_col->get_null_map_data();
        auto& array_col = assert_cast<ColumnArray&>(nested);
        auto& offsets = array_col.get_offsets();
        auto& data = array_col.get_data();
        auto mutable_data = data.assume_mutable();

        for (size_t i = 0; i < rows; ++i) {
            if (i % 5 == 0) {
                // null array
                nullable_col->insert_default();
            } else {
                // non-null array with 3 elements: [i, i+1, i+2]
                null_map.push_back(0);
                mutable_data->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(i)));
                mutable_data->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(i + 1)));
                mutable_data->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(i + 2)));
                offsets.push_back(offsets.empty() ? 3 : offsets.back() + 3);
            }
        }
        block.insert(ColumnWithTypeAndName(std::move(col), arr_type, "arr_col"));
    }
}

static void fill_map_column(Block& block, size_t rows) {
    // map<string, int>
    DataTypePtr map_key_type = DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false);
    DataTypePtr map_value_type = DataTypeFactory::instance().create_data_type(TYPE_INT, false);
    DataTypePtr map_type =
            make_nullable(std::make_shared<DataTypeMap>(map_key_type, map_value_type));

    // map<string, int> column
    {
        MutableColumnPtr col = map_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();
        auto& null_map = nullable_col->get_null_map_data();

        for (size_t i = 0; i < rows; ++i) {
            if (i % 7 == 0) {
                // null map
                nullable_col->insert_default();
            } else {
                null_map.push_back(0);
                auto& offsets = assert_cast<ColumnMap&>(nested).get_offsets();
                auto& keys = assert_cast<ColumnMap&>(nested).get_keys();
                auto& values = assert_cast<ColumnMap&>(nested).get_values();

                auto mutable_keys = keys.assume_mutable();
                auto mutable_values = values.assume_mutable();

                std::string k1 = "k" + std::to_string(i);
                std::string k2 = "k" + std::to_string(i + 1);
                mutable_keys->insert(Field::create_field<PrimitiveType::TYPE_VARCHAR>(k1));
                mutable_values->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(i)));
                mutable_keys->insert(Field::create_field<PrimitiveType::TYPE_VARCHAR>(k2));
                mutable_values->insert(
                        Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(i + 1)));

                offsets.push_back(offsets.empty() ? 2 : offsets.back() + 2);
            }
        }
        block.insert(ColumnWithTypeAndName(std::move(col), map_type, "map_col"));
    }
}

static void fill_struct_column(Block& block, size_t rows) {
    // struct<si:int, ss:string>
    DataTypes struct_fields;
    struct_fields.emplace_back(
            make_nullable(DataTypeFactory::instance().create_data_type(TYPE_INT, false)));
    struct_fields.emplace_back(
            make_nullable(DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false)));
    DataTypePtr struct_type = make_nullable(std::make_shared<DataTypeStruct>(struct_fields));

    // struct<si:int, ss:string> column
    {
        MutableColumnPtr col = struct_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();
        auto& null_map = nullable_col->get_null_map_data();

        auto& struct_col = assert_cast<ColumnStruct&>(nested);
        const auto& fields = struct_col.get_columns();

        for (size_t i = 0; i < rows; ++i) {
            if (i % 6 == 0) {
                nullable_col->insert_default();
            } else {
                null_map.push_back(0);
                auto mutable_field0 = fields[0]->assume_mutable();
                auto mutable_field1 = fields[1]->assume_mutable();
                // int field
                mutable_field0->insert(Field::create_field<PrimitiveType::TYPE_INT>(
                        static_cast<int32_t>(i * 100)));
                // string field
                std::string vs = "ss" + std::to_string(i);
                mutable_field1->insert(Field::create_field<PrimitiveType::TYPE_VARCHAR>(vs));
            }
        }
        block.insert(ColumnWithTypeAndName(std::move(col), struct_type, "struct_col"));
    }
}

static void fill_variant_column(Block& block, size_t rows) {
    // variant
    DataTypePtr variant_type = make_nullable(std::make_shared<DataTypeVariant>());

    // variant column: use JSON strings + deserialize_column_from_json_vector to populate ColumnVariant
    {
        MutableColumnPtr col = variant_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column(); // ColumnVariant

        // Prepare JSON strings with variable number of keys per row
        std::vector<std::string> json_rows;
        json_rows.reserve(rows);
        std::vector<Slice> slices;
        slices.reserve(rows);

        size_t key_cnt = 100;
        for (size_t i = 0; i < rows; ++i) {
            // key count cycles between 1, 2, 3, ... for better coverage
            std::string json = "{";
            for (size_t k = 0; k < key_cnt; ++k) {
                if (k > 0) {
                    json += ",";
                }
                // keys: "k0", "k1", ...
                json += "\"k" + std::to_string(k) + "\":";
                // mix int and string values
                if ((i + k) % 2 == 0) {
                    json += std::to_string(static_cast<int32_t>(i * 10 + k));
                } else {
                    json += "\"v" + std::to_string(i * 10 + k) + "\"";
                }
            }
            json += "}";
            json_rows.emplace_back(std::move(json));
            slices.emplace_back(json_rows.back().data(), json_rows.back().size());
        }

        // Use Variant SerDe to parse JSON into ColumnVariant
        auto variant_type_inner = std::make_shared<DataTypeVariant>();
        auto serde = variant_type_inner->get_serde();
        auto* variant_serde = assert_cast<DataTypeVariantSerDe*>(serde.get());

        uint64_t num_deserialized = 0;
        DataTypeSerDe::FormatOptions options;
        Status st = variant_serde->deserialize_column_from_json_vector(nested, slices,
                                                                       &num_deserialized, options);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(rows, num_deserialized);

        // All rows are treated as non-null here
        auto& null_map = nullable_col->get_null_map_data();
        null_map.clear();
        null_map.resize_fill(rows, 0);

        block.insert(ColumnWithTypeAndName(std::move(col), variant_type, "variant_col"));
    }
}

static void fill_complex_columns(Block& block, size_t rows) {
    fill_array_column(block, rows);
    fill_map_column(block, rows);
    fill_struct_column(block, rows);
    fill_variant_column(block, rows);
}

static Block create_test_block(size_t rows) {
    Block block;
    // simple schema with primitive + complex types:
    // int_col, str_col, arr_col, map_col, struct_col, variant_col
    fill_primitive_columns(block, rows);
    fill_complex_columns(block, rows);

    return block;
}

TEST_F(NativeReaderWriterTest, round_trip_native_file) {
    // Prepare a temporary local file path
    UniqueId uid = UniqueId::gen_uid();
    std::string uuid = uid.to_string();
    std::string file_path = "./native_format_" + uuid + ".native";

    // 1. Write block to Native file via VNativeTransformer
    auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs; // empty, VNativeTransformer won't use it directly
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    Block src_block = create_test_block(16);
    st = transformer.write(src_block);
    ASSERT_TRUE(st.ok()) << st;
    // VNativeTransformer::close() will also close the underlying FileWriter,
    // so we don't need (and must not) close file_writer again here.
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    // 2. Read back via NativeReader using normal file scan path
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, 4064, nullptr, &state);

    Block dst_block;
    size_t read_rows = 0;
    bool eof = false;
    st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(src_block.rows(), read_rows);
    ASSERT_EQ(src_block.columns(), dst_block.columns());

    // Compare column-wise values
    for (size_t col = 0; col < src_block.columns(); ++col) {
        const auto& src = src_block.get_by_position(col);
        const auto& dst = dst_block.get_by_position(col);
        ASSERT_EQ(src.type->get_family_name(), dst.type->get_family_name());
        ASSERT_EQ(src.column->size(), dst.column->size());
        for (size_t row = 0; row < src_block.rows(); ++row) {
            auto src_field = (*src.column)[row];
            auto dst_field = (*dst.column)[row];
            ASSERT_EQ(src_field, dst_field) << "mismatch at col=" << col << " row=" << row;
        }
    }

    // Next call should hit EOF
    Block dst_block2;
    read_rows = 0;
    eof = false;
    st = reader_impl.get_next_block(&dst_block2, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(eof);
    ASSERT_EQ(read_rows, 0);

    // Clean up temp file
    bool exists = false;
    st = fs->exists(file_path, &exists);
    if (st.ok() && exists) {
        static_cast<void>(fs->delete_file(file_path));
    }
}

// Edge cases: empty file and single-row file
TEST_F(NativeReaderWriterTest, round_trip_empty_and_single_row) {
    auto fs = io::global_local_filesystem();

    // 1. Empty native file (no data blocks)
    {
        UniqueId uid = UniqueId::gen_uid();
        std::string uuid = uid.to_string();
        std::string file_path = "./native_format_empty_" + uuid + ".native";

        io::FileWriterPtr file_writer;
        Status st = fs->create_file(file_path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        RuntimeState state;
        VExprContextSPtrs exprs;
        VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
        st = transformer.open();
        ASSERT_TRUE(st.ok()) << st;

        // Write an empty block, should not produce any data block in file.
        Block empty_block;
        st = transformer.write(empty_block);
        ASSERT_TRUE(st.ok()) << st;
        st = transformer.close();
        ASSERT_TRUE(st.ok()) << st;

        // Read back: should directly hit EOF with 0 rows.
        TFileScanRangeParams scan_params;
        scan_params.__set_file_type(TFileType::FILE_LOCAL);
        TFileRangeDesc scan_range;
        scan_range.__set_path(file_path);
        scan_range.__set_file_type(TFileType::FILE_LOCAL);
        NativeReader reader_impl(nullptr, scan_params, scan_range, 4096, nullptr, &state);

        Block dst_block;
        size_t read_rows = 0;
        bool eof = false;
        st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(0U, read_rows);
        ASSERT_TRUE(eof);

        bool exists = false;
        st = fs->exists(file_path, &exists);
        if (st.ok() && exists) {
            static_cast<void>(fs->delete_file(file_path));
        }
    }

    // 2. Single-row native file
    {
        UniqueId uid = UniqueId::gen_uid();
        std::string uuid = uid.to_string();
        std::string file_path = "./native_format_single_row_" + uuid + ".native";

        io::FileWriterPtr file_writer;
        Status st = fs->create_file(file_path, &file_writer);
        ASSERT_TRUE(st.ok()) << st;

        RuntimeState state;
        VExprContextSPtrs exprs;
        VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
        st = transformer.open();
        ASSERT_TRUE(st.ok()) << st;

        Block src_block = create_test_block(1);
        st = transformer.write(src_block);
        ASSERT_TRUE(st.ok()) << st;
        st = transformer.close();
        ASSERT_TRUE(st.ok()) << st;

        TFileScanRangeParams scan_params;
        scan_params.__set_file_type(TFileType::FILE_LOCAL);
        TFileRangeDesc scan_range;
        scan_range.__set_path(file_path);
        scan_range.__set_file_type(TFileType::FILE_LOCAL);
        NativeReader reader_impl(nullptr, scan_params, scan_range, 4096, nullptr, &state);

        Block dst_block;
        size_t read_rows = 0;
        bool eof = false;
        st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(src_block.rows(), read_rows);
        ASSERT_EQ(src_block.columns(), dst_block.columns());
        ASSERT_FALSE(eof);

        // Verify data equality for the single row.
        for (size_t col = 0; col < src_block.columns(); ++col) {
            const auto& src = src_block.get_by_position(col);
            const auto& dst = dst_block.get_by_position(col);
            ASSERT_EQ(src.type->get_family_name(), dst.type->get_family_name());
            ASSERT_EQ(src.column->size(), dst.column->size());
            auto src_field = (*src.column)[0];
            auto dst_field = (*dst.column)[0];
            ASSERT_EQ(src_field, dst_field) << "mismatch at col=" << col << " row=0";
        }

        // Next call should hit EOF
        Block dst_block2;
        read_rows = 0;
        eof = false;
        st = reader_impl.get_next_block(&dst_block2, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_TRUE(eof);
        ASSERT_EQ(read_rows, 0U);

        bool exists = false;
        st = fs->exists(file_path, &exists);
        if (st.ok() && exists) {
            static_cast<void>(fs->delete_file(file_path));
        }
    }
}

// Large volume test: verify round-trip correctness with many rows.
TEST_F(NativeReaderWriterTest, round_trip_native_file_large_rows) {
    UniqueId uid = UniqueId::gen_uid();
    std::string uuid = uid.to_string();
    std::string file_path = "./native_format_large_" + uuid + ".native";

    auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs;
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    // Use a relatively large number of rows to test stability and performance characteristics.
    const size_t kRows = 9999;
    Block src_block = create_test_block(kRows);

    // Split into multiple blocks and write them one by one to exercise multi-block path.
    const size_t kBatchRows = 128;
    for (size_t offset = 0; offset < kRows; offset += kBatchRows) {
        size_t len = std::min(kBatchRows, kRows - offset);
        // Create a sub block with the same schema and a row range [offset, offset+len)
        Block sub_block = src_block.clone_empty();
        for (size_t col = 0; col < src_block.columns(); ++col) {
            const auto& src_col = *src_block.get_by_position(col).column;
            const auto& dst_col_holder = *sub_block.get_by_position(col).column;
            auto dst_mutable = dst_col_holder.assume_mutable();
            dst_mutable->insert_range_from(src_col, offset, len);
        }
        st = transformer.write(sub_block);
        ASSERT_TRUE(st.ok()) << st;
    }
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, 8192, nullptr, &state);

    // Read back in multiple blocks and merge into a single result block.
    Block merged_block;
    bool first_block = true;
    size_t total_read_rows = 0;
    while (true) {
        Block dst_block;
        size_t read_rows = 0;
        bool eof = false;
        st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st;
        if (read_rows > 0) {
            if (first_block) {
                merged_block = dst_block;
                total_read_rows = read_rows;
                first_block = false;
            } else {
                MutableBlock merged_mutable(&merged_block);
                Status add_st = merged_mutable.add_rows(&dst_block, 0, read_rows);
                ASSERT_TRUE(add_st.ok()) << add_st;
                total_read_rows += read_rows;
            }
        }
        if (eof) {
            break;
        }
        if (read_rows == 0) {
            break;
        }
    }

    ASSERT_EQ(src_block.rows(), total_read_rows);
    ASSERT_EQ(src_block.columns(), merged_block.columns());

    // Compare column-wise values
    for (size_t col = 0; col < src_block.columns(); ++col) {
        const auto& src = src_block.get_by_position(col);
        const auto& dst = merged_block.get_by_position(col);
        ASSERT_EQ(src.type->get_family_name(), dst.type->get_family_name());
        ASSERT_EQ(src.column->size(), dst.column->size());
        for (size_t row = 0; row < src_block.rows(); row += 10) {
            auto src_field = (*src.column)[row];
            auto dst_field = (*dst.column)[row];
            ASSERT_EQ(src_field, dst_field) << "mismatch at col=" << col << " row=" << row;
        }
    }
    bool exists = false;
    st = fs->exists(file_path, &exists);
    if (st.ok() && exists) {
        static_cast<void>(fs->delete_file(file_path));
    }
}

} // namespace doris::vectorized
