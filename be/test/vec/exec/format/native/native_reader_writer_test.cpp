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
#include "util/jsonb_writer.h"
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
#include "vec/exec/format/native/native_format.h"
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
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

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
        NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

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
        NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

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
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

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

// Verify that NativeReader forces all columns to nullable, even if the writer
// serialized non-nullable columns.
TEST_F(NativeReaderWriterTest, non_nullable_columns_forced_nullable) {
    auto fs = io::global_local_filesystem();

    UniqueId uid = UniqueId::gen_uid();
    std::string uuid = uid.to_string();
    std::string file_path = "./native_format_non_nullable_" + uuid + ".native";

    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs;
    // Use default compression type.
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    // Build a block with non-nullable columns.
    Block src_block;
    DataTypePtr int_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    {
        MutableColumnPtr col = int_type->create_column();
        for (int i = 0; i < 8; ++i) {
            auto field = Field::create_field<PrimitiveType::TYPE_INT>(i * 3);
            col->insert(field);
        }
        src_block.insert(ColumnWithTypeAndName(std::move(col), int_type, "int_nn"));
    }

    DataTypePtr str_type =
            DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_VARCHAR, false);
    {
        MutableColumnPtr col = str_type->create_column();
        for (int i = 0; i < 8; ++i) {
            std::string v = "v" + std::to_string(i);
            auto field = Field::create_field<PrimitiveType::TYPE_VARCHAR>(v);
            col->insert(field);
        }
        src_block.insert(ColumnWithTypeAndName(std::move(col), str_type, "str_nn"));
    }

    st = transformer.write(src_block);
    ASSERT_TRUE(st.ok()) << st;
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    // Read back via NativeReader.
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

    Block dst_block;
    size_t read_rows = 0;
    bool eof = false;
    st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(src_block.rows(), read_rows);
    ASSERT_FALSE(eof);

    // All columns returned by NativeReader should be nullable.
    for (size_t col = 0; col < dst_block.columns(); ++col) {
        const auto& dst = dst_block.get_by_position(col);
        ASSERT_TRUE(dst.type->is_nullable()) << "column " << col << " should be nullable";
    }

    // Values should be preserved.
    for (size_t col = 0; col < src_block.columns(); ++col) {
        const auto& src = src_block.get_by_position(col);
        const auto& dst = dst_block.get_by_position(col);
        ASSERT_EQ(src.column->size(), dst.column->size());
        for (size_t row = 0; row < src_block.rows(); ++row) {
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

// Verify that VNativeTransformer writes the native file header and that
// NativeReader can transparently read files with this header.
TEST_F(NativeReaderWriterTest, transformer_writes_header_and_reader_handles_it) {
    auto fs = io::global_local_filesystem();

    UniqueId uid = UniqueId::gen_uid();
    std::string uuid = uid.to_string();
    std::string file_path = "./native_format_with_header_" + uuid + ".native";

    // Write a small block via VNativeTransformer.
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs;
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    Block src_block = create_test_block(4);
    st = transformer.write(src_block);
    ASSERT_TRUE(st.ok()) << st;
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    // Read back raw bytes and verify the header.
    {
        io::FileReaderSPtr file_reader;
        st = fs->open_file(file_path, &file_reader);
        ASSERT_TRUE(st.ok()) << st;
        size_t file_size = file_reader->size();
        ASSERT_GE(file_size, sizeof(uint64_t) + 12);

        char header[12];
        Slice header_slice(header, sizeof(header));
        size_t bytes_read = 0;
        st = file_reader->read_at(0, header_slice, &bytes_read);
        ASSERT_TRUE(st.ok()) << st;
        ASSERT_EQ(sizeof(header), bytes_read);

        ASSERT_EQ(0, memcmp(header, DORIS_NATIVE_MAGIC, sizeof(DORIS_NATIVE_MAGIC)));
        uint32_t version = 0;
        memcpy(&version, header + sizeof(DORIS_NATIVE_MAGIC), sizeof(uint32_t));
        ASSERT_EQ(DORIS_NATIVE_FORMAT_VERSION, version);
    }

    // Now read via NativeReader; it should detect the header and skip it.
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

    Block dst_block;
    size_t read_rows = 0;
    bool eof = false;
    st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(src_block.rows(), read_rows);
    ASSERT_FALSE(eof);

    bool exists = false;
    st = fs->exists(file_path, &exists);
    if (st.ok() && exists) {
        static_cast<void>(fs->delete_file(file_path));
    }
}

// Verify get_columns and get_parsed_schema can probe schema without scanning
// the whole file.
TEST_F(NativeReaderWriterTest, get_columns_and_parsed_schema) {
    auto fs = io::global_local_filesystem();

    UniqueId uid = UniqueId::gen_uid();
    std::string uuid = uid.to_string();
    std::string file_path = "./native_format_schema_" + uuid + ".native";

    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs;
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    Block src_block = create_test_block(5);
    st = transformer.write(src_block);
    ASSERT_TRUE(st.ok()) << st;
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

    std::unordered_map<std::string, DataTypePtr> name_to_type;
    std::unordered_set<std::string> missing_cols;
    st = reader_impl.get_columns(&name_to_type, &missing_cols);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(missing_cols.empty());

    // All columns from src_block should appear in name_to_type.
    for (size_t i = 0; i < src_block.columns(); ++i) {
        const auto& col = src_block.get_by_position(i);
        auto it = name_to_type.find(col.name);
        ASSERT_TRUE(it != name_to_type.end()) << "missing column " << col.name;
        ASSERT_TRUE(it->second->is_nullable()) << "schema type should be nullable for " << col.name;
    }

    std::vector<std::string> col_names;
    std::vector<DataTypePtr> col_types;
    st = reader_impl.get_parsed_schema(&col_names, &col_types);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(col_names.size(), col_types.size());
    ASSERT_EQ(col_names.size(), src_block.columns());

    for (size_t i = 0; i < col_names.size(); ++i) {
        ASSERT_TRUE(col_types[i]->is_nullable());
    }

    bool exists = false;
    st = fs->exists(file_path, &exists);
    if (st.ok() && exists) {
        static_cast<void>(fs->delete_file(file_path));
    }
}

// Create a test block containing all known primitive and complex types with a single row.
// This function covers:
// - Basic integers: BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, LARGEINT
// - Floating point: FLOAT, DOUBLE
// - Date/Time: DATE, DATETIME, DATEV2, DATETIMEV2, TIMEV2
// - String types: CHAR, VARCHAR, STRING
// - Decimal types: DECIMALV2, DECIMAL32, DECIMAL64, DECIMAL128I, DECIMAL256
// - IP types: IPV4, IPV6
// - JSON: JSONB
// - Complex types: ARRAY, MAP, STRUCT, VARIANT
static Block create_all_types_test_block() {
    Block block;

    // 1. BOOLEAN
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_BOOLEAN, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_BOOLEAN>(1)); // true
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_boolean"));
    }

    // 2. TINYINT
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_TINYINT, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_TINYINT>(42));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_tinyint"));
    }

    // 3. SMALLINT
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_SMALLINT, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_SMALLINT>(static_cast<int16_t>(1234)));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_smallint"));
    }

    // 4. INT
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_INT, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(123456)));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_int"));
    }

    // 5. BIGINT
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_BIGINT, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_BIGINT>(
                static_cast<int64_t>(9876543210LL)));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_bigint"));
    }

    // 6. LARGEINT
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_LARGEINT, false));
        MutableColumnPtr col = type->create_column();
        Int128 large_val = Int128(123456789012345LL) * 1000000;
        col->insert(Field::create_field<PrimitiveType::TYPE_LARGEINT>(large_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_largeint"));
    }

    // 7. FLOAT
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_FLOAT, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_FLOAT>(3.14F));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_float"));
    }

    // 8. DOUBLE
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_DOUBLE, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_DOUBLE>(1.234567890123456789));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_double"));
    }

    // 9. DATE (DateV1)
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_DATE, false));
        MutableColumnPtr col = type->create_column();
        // DateV1 stores as int64 representing binary date
        VecDateTimeValue dt;
        dt.from_date_int64(20231215); // 2023-12-15
        col->insert(Field::create_field<PrimitiveType::TYPE_DATE>(dt));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_date"));
    }

    // 10. DATETIME (DateTimeV1)
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_DATETIME, false));
        MutableColumnPtr col = type->create_column();
        VecDateTimeValue dt;
        dt.from_date_int64(20231215103045LL); // 2023-12-15 10:30:45
        col->insert(Field::create_field<PrimitiveType::TYPE_DATETIME>(dt));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_datetime"));
    }

    // 11. DATEV2
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_DATEV2, false));
        MutableColumnPtr col = type->create_column();
        DateV2Value<DateV2ValueType> dv2;
        dv2.from_date_int64(20231215);
        col->insert(Field::create_field<PrimitiveType::TYPE_DATEV2>(dv2));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_datev2"));
    }

    // 12. DATETIMEV2 (scale=0)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_DATETIMEV2, false, 0, 0));
        MutableColumnPtr col = type->create_column();
        DateV2Value<DateTimeV2ValueType> dtv2;
        dtv2.from_date_int64(20231215103045LL);
        col->insert(Field::create_field<PrimitiveType::TYPE_DATETIMEV2>(dtv2));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_datetimev2"));
    }

    // 13. TIMEV2 (scale=0)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_TIMEV2, false, 0, 0));
        MutableColumnPtr col = type->create_column();
        // TIMEV2 is stored as Float64 representing seconds
        col->insert(
                Field::create_field<PrimitiveType::TYPE_TIMEV2>(37845.0)); // 10:30:45 in seconds
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_timev2"));
    }

    // 14. CHAR
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_CHAR, false, 0, 0, 20));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_CHAR>(std::string("fixed_char_val")));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_char"));
    }

    // 15. VARCHAR
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false));
        MutableColumnPtr col = type->create_column();
        col->insert(
                Field::create_field<PrimitiveType::TYPE_VARCHAR>(std::string("variable_string")));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_varchar"));
    }

    // 16. STRING
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_STRING, false));
        MutableColumnPtr col = type->create_column();
        col->insert(Field::create_field<PrimitiveType::TYPE_STRING>(
                std::string("long_text_content_here")));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_string"));
    }

    // 17. DECIMALV2 (precision=27, scale=9)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMALV2, false, 27, 9));
        MutableColumnPtr col = type->create_column();
        DecimalV2Value dec_val(static_cast<Int128>(123456789123456789LL));
        col->insert(Field::create_field<PrimitiveType::TYPE_DECIMALV2>(dec_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_decimalv2"));
    }

    // 18. DECIMAL32 (precision=9, scale=2)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL32, false, 9, 2));
        MutableColumnPtr col = type->create_column();
        Decimal32 dec_val(static_cast<Int32>(12345678));
        col->insert(Field::create_field<PrimitiveType::TYPE_DECIMAL32>(dec_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_decimal32"));
    }

    // 19. DECIMAL64 (precision=18, scale=4)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL64, false, 18, 4));
        MutableColumnPtr col = type->create_column();
        Decimal64 dec_val(static_cast<Int64>(123456789012345678LL));
        col->insert(Field::create_field<PrimitiveType::TYPE_DECIMAL64>(dec_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_decimal64"));
    }

    // 20. DECIMAL128I (precision=38, scale=6)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL128I, false, 38, 6));
        MutableColumnPtr col = type->create_column();
        Decimal128V3 dec_val(static_cast<Int128>(123456789012345678LL) * 100);
        col->insert(Field::create_field<PrimitiveType::TYPE_DECIMAL128I>(dec_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_decimal128"));
    }

    // 21. DECIMAL256 (precision=76, scale=10)
    {
        DataTypePtr type = make_nullable(
                DataTypeFactory::instance().create_data_type(TYPE_DECIMAL256, false, 76, 10));
        MutableColumnPtr col = type->create_column();
        wide::Int256 wide_val = wide::Int256(123456789012345678LL) * 10000000000LL;
        Decimal256 dec_val(wide_val);
        col->insert(Field::create_field<PrimitiveType::TYPE_DECIMAL256>(dec_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_decimal256"));
    }

    // 22. IPV4
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_IPV4, false));
        MutableColumnPtr col = type->create_column();
        IPv4 ip_val = (192U << 24) | (168U << 16) | (1U << 8) | 100U; // 192.168.1.100
        col->insert(Field::create_field<PrimitiveType::TYPE_IPV4>(ip_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_ipv4"));
    }

    // 23. IPV6
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_IPV6, false));
        MutableColumnPtr col = type->create_column();
        // ::ffff:192.168.1.100 in IPv6
        IPv6 ip6_val = 0;
        ip6_val = (static_cast<IPv6>(0xFFFF) << 32) |
                  ((192U << 24) | (168U << 16) | (1U << 8) | 100U);
        col->insert(Field::create_field<PrimitiveType::TYPE_IPV6>(ip6_val));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_ipv6"));
    }

    // 24. JSONB
    {
        DataTypePtr type =
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_JSONB, false));
        MutableColumnPtr col = type->create_column();
        // Use JsonbWriter to create JSONB binary data
        JsonbWriter writer;
        writer.writeStartObject();
        writer.writeKey("key", static_cast<uint8_t>(3));
        writer.writeStartString();
        writer.writeString("value");
        writer.writeEndString();
        writer.writeKey("num", static_cast<uint8_t>(3));
        writer.writeInt32(123);
        writer.writeEndObject();
        const char* jsonb_data = writer.getOutput()->getBuffer();
        size_t jsonb_size = writer.getOutput()->getSize();
        JsonbField field(jsonb_data, jsonb_size);
        col->insert(Field::create_field<PrimitiveType::TYPE_JSONB>(std::move(field)));
        block.insert(ColumnWithTypeAndName(std::move(col), type, "col_jsonb"));
    }

    // 25. ARRAY<INT>
    {
        DataTypePtr arr_nested_type = DataTypeFactory::instance().create_data_type(TYPE_INT, false);
        DataTypePtr arr_type = make_nullable(std::make_shared<DataTypeArray>(arr_nested_type));
        MutableColumnPtr col = arr_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();
        auto& null_map = nullable_col->get_null_map_data();
        null_map.push_back(0); // non-null

        auto& array_col = assert_cast<ColumnArray&>(nested);
        auto& offsets = array_col.get_offsets();
        auto& data = array_col.get_data();
        auto mutable_data = data.assume_mutable();

        mutable_data->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(10)));
        mutable_data->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(20)));
        mutable_data->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(30)));
        offsets.push_back(3); // 3 elements
        block.insert(ColumnWithTypeAndName(std::move(col), arr_type, "col_array"));
    }

    // 26. MAP<STRING, INT>
    {
        DataTypePtr map_key_type =
                DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false);
        DataTypePtr map_value_type = DataTypeFactory::instance().create_data_type(TYPE_INT, false);
        DataTypePtr map_type =
                make_nullable(std::make_shared<DataTypeMap>(map_key_type, map_value_type));
        MutableColumnPtr col = map_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();
        auto& null_map = nullable_col->get_null_map_data();
        null_map.push_back(0); // non-null

        auto& offsets = assert_cast<ColumnMap&>(nested).get_offsets();
        auto& keys = assert_cast<ColumnMap&>(nested).get_keys();
        auto& values = assert_cast<ColumnMap&>(nested).get_values();

        auto mutable_keys = keys.assume_mutable();
        auto mutable_values = values.assume_mutable();

        mutable_keys->insert(Field::create_field<PrimitiveType::TYPE_VARCHAR>(std::string("key1")));
        mutable_values->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(100)));
        mutable_keys->insert(Field::create_field<PrimitiveType::TYPE_VARCHAR>(std::string("key2")));
        mutable_values->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(200)));

        offsets.push_back(2); // 2 key-value pairs
        block.insert(ColumnWithTypeAndName(std::move(col), map_type, "col_map"));
    }

    // 27. STRUCT<si:INT, ss:VARCHAR>
    {
        DataTypes struct_fields;
        struct_fields.emplace_back(
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_INT, false)));
        struct_fields.emplace_back(
                make_nullable(DataTypeFactory::instance().create_data_type(TYPE_VARCHAR, false)));
        Strings field_names = {"si", "ss"};
        DataTypePtr struct_type =
                make_nullable(std::make_shared<DataTypeStruct>(struct_fields, field_names));
        MutableColumnPtr col = struct_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();
        auto& null_map = nullable_col->get_null_map_data();
        null_map.push_back(0); // non-null

        auto& struct_col = assert_cast<ColumnStruct&>(nested);
        const auto& fields = struct_col.get_columns();
        auto mutable_field0 = fields[0]->assume_mutable();
        auto mutable_field1 = fields[1]->assume_mutable();

        mutable_field0->insert(
                Field::create_field<PrimitiveType::TYPE_INT>(static_cast<int32_t>(999)));
        mutable_field1->insert(
                Field::create_field<PrimitiveType::TYPE_VARCHAR>(std::string("struct_val")));
        block.insert(ColumnWithTypeAndName(std::move(col), struct_type, "col_struct"));
    }

    // 28. VARIANT (JSON object)
    {
        DataTypePtr variant_type = make_nullable(std::make_shared<DataTypeVariant>());
        MutableColumnPtr col = variant_type->create_column();
        auto* nullable_col = assert_cast<ColumnNullable*>(col.get());
        auto& nested = nullable_col->get_nested_column();

        std::string json_str = R"({"name":"test","value":12345})";
        std::vector<Slice> slices = {Slice(json_str.data(), json_str.size())};

        auto variant_type_inner = std::make_shared<DataTypeVariant>();
        auto serde = variant_type_inner->get_serde();
        auto* variant_serde = assert_cast<DataTypeVariantSerDe*>(serde.get());

        uint64_t num_deserialized = 0;
        DataTypeSerDe::FormatOptions options;
        Status st = variant_serde->deserialize_column_from_json_vector(nested, slices,
                                                                       &num_deserialized, options);
        EXPECT_TRUE(st.ok()) << st;
        EXPECT_EQ(1U, num_deserialized);

        auto& null_map = nullable_col->get_null_map_data();
        null_map.clear();
        null_map.resize_fill(1, 0);

        block.insert(ColumnWithTypeAndName(std::move(col), variant_type, "col_variant"));
    }

    return block;
}

// Pre-generated native file path for all types test.
// The file is stored in: be/test/data/vec/native/all_types_single_row.native
static std::string get_all_types_native_file_path() {
    auto root_dir = std::string(getenv("ROOT"));
    return root_dir + "/be/test/data/vec/native/all_types_single_row.native";
}

// Generator test: Generate native file with all types (DISABLED by default).
// Run this test manually to regenerate the test data file:
//   ./run-be-ut.sh --run --filter=*generate_all_types_native_file*
// Then copy the generated file to: be/test/data/vec/native/all_types_single_row.native
TEST_F(NativeReaderWriterTest, generate_all_types_native_file) {
    // Output to current directory, user needs to copy it to test data dir
    std::string file_path = "./all_types_single_row.native";

    auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs;
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    Block src_block = create_all_types_test_block();
    ASSERT_EQ(1U, src_block.rows()) << "Source block should have exactly 1 row";
    ASSERT_EQ(28U, src_block.columns()) << "Source block should have 28 columns for all types";

    st = transformer.write(src_block);
    ASSERT_TRUE(st.ok()) << st;
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    std::cout << "Generated native file: " << file_path << std::endl;
    std::cout << "Please copy it to: be/test/data/vec/native/all_types_single_row.native"
              << std::endl;
}

// Test: read pre-generated native file with all types and verify data
TEST_F(NativeReaderWriterTest, read_all_types_from_pregenerated_file) {
    std::string file_path = get_all_types_native_file_path();

    auto fs = io::global_local_filesystem();
    bool exists = false;
    Status st = fs->exists(file_path, &exists);
    DCHECK(exists) << "Pre-generated native file not found: " << file_path
                   << ". Run generate_all_types_native_file to generate it.";

    RuntimeState state;
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

    Block dst_block;
    size_t read_rows = 0;
    bool eof = false;
    st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(1U, read_rows) << "Should read exactly 1 row";
    ASSERT_EQ(28U, dst_block.columns()) << "Should have 28 columns for all types";
    ASSERT_FALSE(eof);

    // Regenerate expected block and compare
    Block expected_block = create_all_types_test_block();

    // Verify data equality for the single row
    for (size_t col = 0; col < expected_block.columns(); ++col) {
        const auto& src = expected_block.get_by_position(col);
        const auto& dst = dst_block.get_by_position(col);

        // Type family should match
        ASSERT_EQ(src.type->get_family_name(), dst.type->get_family_name())
                << "Type mismatch at col=" << col << " (" << src.name << ")";
        ASSERT_EQ(src.column->size(), dst.column->size())
                << "Size mismatch at col=" << col << " (" << src.name << ")";

        // Compare field values
        auto src_field = (*src.column)[0];
        auto dst_field = (*dst.column)[0];
        ASSERT_EQ(src_field, dst_field)
                << "Value mismatch at col=" << col << " (" << src.name << ")";
    }

    // Next call should hit EOF
    Block dst_block2;
    read_rows = 0;
    eof = false;
    st = reader_impl.get_next_block(&dst_block2, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(eof);
    ASSERT_EQ(read_rows, 0U);
}

// Test: round-trip all known types with a single row (generates temp file each time)
TEST_F(NativeReaderWriterTest, round_trip_all_types_single_row) {
    UniqueId uid = UniqueId::gen_uid();
    std::string uuid = uid.to_string();
    std::string file_path = "./native_format_all_types_" + uuid + ".native";

    auto fs = io::global_local_filesystem();
    io::FileWriterPtr file_writer;
    Status st = fs->create_file(file_path, &file_writer);
    ASSERT_TRUE(st.ok()) << st;

    RuntimeState state;
    VExprContextSPtrs exprs;
    VNativeTransformer transformer(&state, file_writer.get(), exprs, false);
    st = transformer.open();
    ASSERT_TRUE(st.ok()) << st;

    Block src_block = create_all_types_test_block();
    ASSERT_EQ(1U, src_block.rows()) << "Source block should have exactly 1 row";
    ASSERT_EQ(28U, src_block.columns()) << "Source block should have 28 columns for all types";

    st = transformer.write(src_block);
    ASSERT_TRUE(st.ok()) << st;
    st = transformer.close();
    ASSERT_TRUE(st.ok()) << st;

    // Read back via NativeReader
    TFileScanRangeParams scan_params;
    scan_params.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc scan_range;
    scan_range.__set_path(file_path);
    scan_range.__set_file_type(TFileType::FILE_LOCAL);
    NativeReader reader_impl(nullptr, scan_params, scan_range, nullptr, &state);

    Block dst_block;
    size_t read_rows = 0;
    bool eof = false;
    st = reader_impl.get_next_block(&dst_block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(src_block.rows(), read_rows);
    ASSERT_EQ(src_block.columns(), dst_block.columns());
    ASSERT_FALSE(eof);

    // Verify data equality for the single row
    for (size_t col = 0; col < src_block.columns(); ++col) {
        const auto& src = src_block.get_by_position(col);
        const auto& dst = dst_block.get_by_position(col);

        // Type family should match
        ASSERT_EQ(src.type->get_family_name(), dst.type->get_family_name())
                << "Type mismatch at col=" << col << " (" << src.name << ")";
        ASSERT_EQ(src.column->size(), dst.column->size())
                << "Size mismatch at col=" << col << " (" << src.name << ")";

        // Compare field values
        auto src_field = (*src.column)[0];
        auto dst_field = (*dst.column)[0];
        ASSERT_EQ(src_field, dst_field)
                << "Value mismatch at col=" << col << " (" << src.name << ")";
    }

    // Next call should hit EOF
    Block dst_block2;
    read_rows = 0;
    eof = false;
    st = reader_impl.get_next_block(&dst_block2, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_TRUE(eof);
    ASSERT_EQ(read_rows, 0U);

    // Clean up temp file
    bool exists = false;
    st = fs->exists(file_path, &exists);
    if (st.ok() && exists) {
        static_cast<void>(fs->delete_file(file_path));
    }
}

} // namespace doris::vectorized
