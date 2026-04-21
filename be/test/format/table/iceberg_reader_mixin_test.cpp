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

#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "format/table/iceberg_reader.h"
#include "runtime/runtime_profile.h"
#include "storage/olap_common.h"

namespace doris {

// ============================================================================
// Helper: Build the Iceberg $row_id struct type.
//
// The $row_id column is Nullable<Struct<file_path:String, pos:Int64,
//   partition_spec_id:Int32, partition_data:String>>.
// ============================================================================
static DataTypePtr build_iceberg_rowid_type() {
    DataTypes field_types = {
            std::make_shared<DataTypeString>(), // file_path
            std::make_shared<DataTypeInt64>(),  // pos (row position)
            std::make_shared<DataTypeInt32>(),  // partition_spec_id
            std::make_shared<DataTypeString>(), // partition_data
    };
    Strings field_names = {"file_path", "pos", "partition_spec_id", "partition_data"};
    auto struct_type = std::make_shared<DataTypeStruct>(field_types, field_names);
    return std::make_shared<DataTypeNullable>(struct_type);
}

// Non-nullable variant.
static DataTypePtr build_iceberg_rowid_type_non_nullable() {
    DataTypes field_types = {
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
            std::make_shared<DataTypeInt32>(),
            std::make_shared<DataTypeString>(),
    };
    Strings field_names = {"file_path", "pos", "partition_spec_id", "partition_data"};
    return std::make_shared<DataTypeStruct>(field_types, field_names);
}

// Expose protected static mixin helpers through a test-only derived type.
class IcebergReaderMixinTestAccessor : public IcebergReaderMixin<ParquetReader> {
public:
    using IcebergReaderMixin<ParquetReader>::_build_iceberg_rowid_column;
    using IcebergReaderMixin<ParquetReader>::_sort_delete_rows;

    void set_delete_rows() override {}

protected:
    Status on_before_init_reader(ReaderInitContext* ctx) override { return Status::OK(); }

private:
    using DeleteFile = typename IcebergReaderMixin<ParquetReader>::DeleteFile;

    Status _read_position_delete_file(const TFileRangeDesc*, DeleteFile*) override {
        return Status::OK();
    }

    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& delete_desc) override {
        return nullptr;
    }
};

// ============================================================================
// Test: _build_iceberg_rowid_column with nullable struct type
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNullable) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids = {0, 5, 10, 15};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/data/file1.parquet", row_ids, 3, "{\"region\":\"us-east-1\"}", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), 4);

    // Verify it's a nullable struct.
    auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    ASSERT_NE(nullable, nullptr);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_FALSE(nullable->is_null_at(i)) << "Row " << i << " should not be NULL";
    }

    auto* struct_col = check_and_get_column<ColumnStruct>(nullable->get_nested_column_ptr().get());
    ASSERT_NE(struct_col, nullptr);
    ASSERT_GE(struct_col->tuple_size(), 4);

    // Verify file_path (field 0).
    auto& file_path_col = struct_col->get_column(0);
    EXPECT_EQ(file_path_col.size(), 4);
    for (size_t i = 0; i < 4; i++) {
        auto val = file_path_col.get_data_at(i);
        EXPECT_EQ(std::string(val.data, val.size), "/data/file1.parquet");
    }

    // Verify row positions (field 1).
    auto& row_pos_col = struct_col->get_column(1);
    EXPECT_EQ(row_pos_col.size(), 4);
    EXPECT_EQ(row_pos_col.get_int(0), 0);
    EXPECT_EQ(row_pos_col.get_int(1), 5);
    EXPECT_EQ(row_pos_col.get_int(2), 10);
    EXPECT_EQ(row_pos_col.get_int(3), 15);

    // Verify partition_spec_id (field 2).
    auto& spec_id_col = struct_col->get_column(2);
    EXPECT_EQ(spec_id_col.size(), 4);
    for (size_t i = 0; i < 4; i++) {
        EXPECT_EQ(spec_id_col.get_int(i), 3);
    }

    // Verify partition_data (field 3).
    auto& partition_data_col = struct_col->get_column(3);
    EXPECT_EQ(partition_data_col.size(), 4);
    for (size_t i = 0; i < 4; i++) {
        auto val = partition_data_col.get_data_at(i);
        EXPECT_EQ(std::string(val.data, val.size), "{\"region\":\"us-east-1\"}");
    }
}

// ============================================================================
// Test: _build_iceberg_rowid_column with non-nullable struct type
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNonNullable) {
    auto type = build_iceberg_rowid_type_non_nullable();
    std::vector<segment_v2::rowid_t> row_ids = {100, 200};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/file2.orc",
                                                                          row_ids, 7, "", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), 2);

    auto* struct_col = check_and_get_column<ColumnStruct>(result.get());
    ASSERT_NE(struct_col, nullptr);

    // Verify row positions.
    auto& row_pos_col = struct_col->get_column(1);
    EXPECT_EQ(row_pos_col.get_int(0), 100);
    EXPECT_EQ(row_pos_col.get_int(1), 200);

    // Verify spec id.
    auto& spec_id_col = struct_col->get_column(2);
    EXPECT_EQ(spec_id_col.get_int(0), 7);
    EXPECT_EQ(spec_id_col.get_int(1), 7);
}

// ============================================================================
// Test: _build_iceberg_rowid_column with empty row_ids
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnEmptyRows) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids;
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/data/empty.parquet", row_ids, 0, "", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), 0);
}

// ============================================================================
// Test: _build_iceberg_rowid_column with large row count
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnLargeBatch) {
    auto type = build_iceberg_rowid_type();
    const size_t num_rows = 10000;
    std::vector<segment_v2::rowid_t> row_ids(num_rows);
    for (size_t i = 0; i < num_rows; i++) {
        row_ids[i] = static_cast<segment_v2::rowid_t>(i * 3);
    }
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/data/large.parquet", row_ids, 1, "{}", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_TRUE(static_cast<bool>(result));
    EXPECT_EQ(result->size(), num_rows);

    auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    auto* struct_col = check_and_get_column<ColumnStruct>(nullable->get_nested_column_ptr().get());
    auto& row_pos_col = struct_col->get_column(1);
    // Spot check a few positions.
    EXPECT_EQ(row_pos_col.get_int(0), 0);
    EXPECT_EQ(row_pos_col.get_int(1), 3);
    EXPECT_EQ(row_pos_col.get_int(9999), 29997);
}

// ============================================================================
// Test: _build_iceberg_rowid_column null type returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNullTypeError) {
    std::vector<segment_v2::rowid_t> row_ids = {0};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            nullptr, "/data/f.parquet", row_ids, 0, "", &result);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
}

// ============================================================================
// Test: _build_iceberg_rowid_column null output column returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnNullOutputError) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids = {0};

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/f.parquet",
                                                                          row_ids, 0, "", nullptr);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INVALID_ARGUMENT>());
}

// ============================================================================
// Test: _build_iceberg_rowid_column with wrong type (not struct) returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnWrongTypeError) {
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    std::vector<segment_v2::rowid_t> row_ids = {0};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/f.parquet",
                                                                          row_ids, 0, "", &result);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: _build_iceberg_rowid_column with struct having < 4 fields returns error
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnTooFewFieldsError) {
    DataTypes field_types = {
            std::make_shared<DataTypeString>(),
            std::make_shared<DataTypeInt64>(),
    };
    Strings field_names = {"file_path", "pos"};
    auto struct_type = std::make_shared<DataTypeStruct>(field_types, field_names);
    auto type = std::make_shared<DataTypeNullable>(struct_type);

    std::vector<segment_v2::rowid_t> row_ids = {0};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(type, "/data/f.parquet",
                                                                          row_ids, 0, "", &result);
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: _build_iceberg_rowid_column partition_spec_id 0 and empty partition_data
// ============================================================================
TEST(IcebergRowIdTest, BuildRowIdColumnZeroSpecIdEmptyPartition) {
    auto type = build_iceberg_rowid_type();
    std::vector<segment_v2::rowid_t> row_ids = {42};
    MutableColumnPtr result;

    auto st = IcebergReaderMixinTestAccessor::_build_iceberg_rowid_column(
            type, "/warehouse/table/data/00000-0.parquet", row_ids, 0, "", &result);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(result->size(), 1);

    auto* nullable = check_and_get_column<ColumnNullable>(result.get());
    auto* struct_col = check_and_get_column<ColumnStruct>(nullable->get_nested_column_ptr().get());

    auto& spec_id_col = struct_col->get_column(2);
    EXPECT_EQ(spec_id_col.get_int(0), 0);

    auto& partition_data_col = struct_col->get_column(3);
    auto val = partition_data_col.get_data_at(0);
    EXPECT_EQ(std::string(val.data, val.size), "");
}

// ============================================================================
// Test: _sort_delete_rows merges multiple pre-sorted delete row arrays
// ============================================================================
TEST(IcebergSortDeleteTest, SingleArray) {
    std::vector<int64_t> arr1 = {1, 2, 3, 4, 5};
    std::vector<std::vector<int64_t>*> arrays = {&arr1};
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, arr1.size(), result);
    ASSERT_EQ(result.size(), 5);
    EXPECT_EQ(result[0], 1);
    EXPECT_EQ(result[1], 2);
    EXPECT_EQ(result[2], 3);
    EXPECT_EQ(result[3], 4);
    EXPECT_EQ(result[4], 5);
}

TEST(IcebergSortDeleteTest, TwoArraysMerged) {
    // Both inputs must be pre-sorted.
    std::vector<int64_t> arr1 = {1, 5, 10};
    std::vector<int64_t> arr2 = {3, 7, 12};
    std::vector<std::vector<int64_t>*> arrays = {&arr1, &arr2};
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, arr1.size() + arr2.size(), result);
    // Merged: 1, 3, 5, 7, 10, 12
    ASSERT_EQ(result.size(), 6);
    EXPECT_EQ(result[0], 1);
    EXPECT_EQ(result[1], 3);
    EXPECT_EQ(result[2], 5);
    EXPECT_EQ(result[3], 7);
    EXPECT_EQ(result[4], 10);
    EXPECT_EQ(result[5], 12);
}

TEST(IcebergSortDeleteTest, EmptyInput) {
    std::vector<std::vector<int64_t>*> arrays;
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, 0, result);
    EXPECT_TRUE(result.empty());
}

TEST(IcebergSortDeleteTest, SingleElementArrays) {
    std::vector<int64_t> arr1 = {100};
    std::vector<int64_t> arr2 = {50};
    std::vector<std::vector<int64_t>*> arrays = {&arr1, &arr2};
    std::vector<int64_t> result;

    IcebergReaderMixinTestAccessor::_sort_delete_rows(arrays, 2, result);
    ASSERT_EQ(result.size(), 2);
    EXPECT_EQ(result[0], 50);
    EXPECT_EQ(result[1], 100);
}

class FakeEqualityDeleteOrcReader : public OrcReader {
public:
    FakeEqualityDeleteOrcReader(std::vector<std::string> schema_names,
                                std::vector<DataTypePtr> schema_types, std::vector<Block> batches)
            : OrcReader(TFileScanRangeParams {}, TFileRangeDesc {}, "UTC", nullptr, nullptr, false),
              _schema_names(std::move(schema_names)),
              _schema_types(std::move(schema_types)),
              _batches(std::move(batches)) {}

    Status init_schema_reader() override { return Status::OK(); }

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override {
        *col_names = _schema_names;
        *col_types = _schema_types;
        return Status::OK();
    }

protected:
    Status _open_file_reader(ReaderInitContext* /*ctx*/) override { return Status::OK(); }

    Status _do_init_reader(ReaderInitContext* /*ctx*/) override { return Status::OK(); }

    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        if (_next_batch >= _batches.size()) {
            *read_rows = 0;
            *eof = true;
            return Status::OK();
        }

        MutableBlock mutable_block(block);
        RETURN_IF_ERROR(mutable_block.merge(_batches[_next_batch]));
        *block = mutable_block.to_block();
        *read_rows = block->rows();
        ++_next_batch;
        *eof = _next_batch >= _batches.size();
        return Status::OK();
    }

private:
    std::vector<std::string> _schema_names;
    std::vector<DataTypePtr> _schema_types;
    std::vector<Block> _batches;
    size_t _next_batch = 0;
};

class IcebergReaderMixinOrcTestAccessor : public IcebergReaderMixin<OrcReader> {
public:
    IcebergReaderMixinOrcTestAccessor(RuntimeProfile* profile, const TFileScanRangeParams& params,
                                      const TFileRangeDesc& range)
            : IcebergReaderMixin<OrcReader>(nullptr, profile, nullptr, params, range, 1024, "UTC",
                                            nullptr, nullptr, false) {}

    using IcebergReaderMixin<OrcReader>::_init_row_filters;
    using IcebergReaderMixin<OrcReader>::_equality_delete_base;
    using IcebergReaderMixin<OrcReader>::on_after_read_block;
    using IcebergReaderMixin<OrcReader>::on_before_read_block;

    void set_delete_rows() override {}

    void set_block_index_map(std::unordered_map<std::string, uint32_t>* col_name_to_idx) {
        this->col_name_to_block_idx_ref() = col_name_to_idx;
    }

    const std::vector<std::string>& expand_col_names() const { return _expand_col_names; }

    const std::unordered_map<int, std::string>& id_to_block_column_name() const {
        return _id_to_block_column_name;
    }

    size_t equality_delete_impl_count() const { return _equality_delete_impls.size(); }

    void enqueue_delete_reader(std::unique_ptr<GenericReader> delete_reader) {
        _delete_readers.push(std::move(delete_reader));
    }

protected:
    Status on_before_init_reader(ReaderInitContext* /*ctx*/) override { return Status::OK(); }

private:
    using DeleteFile = typename IcebergReaderMixin<OrcReader>::DeleteFile;

    Status _read_position_delete_file(const TFileRangeDesc*, DeleteFile*) override {
        return Status::OK();
    }

    std::unique_ptr<GenericReader> _create_equality_reader(
            const TFileRangeDesc& /*delete_desc*/) override {
        EXPECT_FALSE(_delete_readers.empty());
        auto delete_reader = std::move(_delete_readers.front());
        _delete_readers.pop();
        return delete_reader;
    }

    std::queue<std::unique_ptr<GenericReader>> _delete_readers;
};

class UnsupportedDeleteReader : public GenericReader {
public:
    Status init_schema_reader() override { return Status::OK(); }

protected:
    Status _do_get_next_block(Block* /*block*/, size_t* read_rows, bool* eof) override {
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }
};

static MutableColumnPtr make_nullable_string_column(const std::vector<std::string>& values) {
    auto column = make_nullable(std::make_shared<DataTypeString>())->create_column();
    for (const auto& value : values) {
        column->insert_data(value.data(), value.size());
    }
    return column;
}

static MutableColumnPtr make_nullable_int32_column(const std::vector<Int32>& values) {
    auto column = make_nullable(std::make_shared<DataTypeInt32>())->create_column();
    for (const auto value : values) {
        column->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    return column;
}

static std::vector<Int32> get_int32_values(const IColumn& column) {
    const auto* int_column = check_and_get_column<ColumnInt32>(&column);
    EXPECT_NE(int_column, nullptr);
    if (int_column == nullptr) {
        return {};
    }
    const auto& data = int_column->get_data();
    return std::vector<Int32>(data.begin(), data.end());
}

TEST(IcebergEqualityDeleteTest, ExpandFilterAndShrinkUsingRealHooks) {
    RuntimeProfile profile("iceberg_equality_delete_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);

    Block delete_batch;
    delete_batch.insert({make_nullable_string_column({"us", "jp"}),
                         make_nullable(std::make_shared<DataTypeString>()), "region"});
    reader.enqueue_delete_reader(std::make_unique<FakeEqualityDeleteOrcReader>(
            std::vector<std::string> {"region"},
            std::vector<DataTypePtr> {std::make_shared<DataTypeString>()},
            std::vector<Block> {delete_batch}));

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "memory://delete-region.orc";
    delete_file.content = IcebergReaderMixinOrcTestAccessor::EQUALITY_DELETE;
    delete_file.__set_field_ids({10});

    auto st = reader._equality_delete_base({delete_file});
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(reader.expand_col_names(), std::vector<std::string>({"region"}));
    ASSERT_EQ(reader.equality_delete_impl_count(), 1);
    ASSERT_EQ(reader.id_to_block_column_name().at(10), "region");

    Block block;
    auto id_column = ColumnInt32::create();
    for (Int32 value : {1, 2, 3}) {
        id_column->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    block.insert({std::move(id_column), std::make_shared<DataTypeInt32>(), "id"});

    auto block_idx = block.get_name_to_pos_map();
    reader.set_block_index_map(&block_idx);

    st = reader.on_before_read_block(&block);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(block.columns(), 2);
    ASSERT_EQ(block_idx["region"], 1);

    block.replace_by_position(block_idx["region"], make_nullable_string_column({"cn", "us", "jp"}));

    size_t read_rows = block.rows();
    st = reader.on_after_read_block(&block, &read_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(read_rows, 1);
    EXPECT_EQ(block.columns(), 1);
    EXPECT_FALSE(block_idx.contains("region"));
    EXPECT_EQ(get_int32_values(*block.get_by_position(0).column), std::vector<Int32>({1}));
}

TEST(IcebergEqualityDeleteTest, RejectDuplicateExpandedColumnNames) {
    RuntimeProfile profile("iceberg_duplicate_expand_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);

    Block delete_batch;
    delete_batch.insert({make_nullable_int32_column({7}),
                         make_nullable(std::make_shared<DataTypeInt32>()), "id"});
    reader.enqueue_delete_reader(std::make_unique<FakeEqualityDeleteOrcReader>(
            std::vector<std::string> {"id"},
            std::vector<DataTypePtr> {std::make_shared<DataTypeInt32>()},
            std::vector<Block> {delete_batch}));

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "memory://delete-id.orc";
    delete_file.content = IcebergReaderMixinOrcTestAccessor::EQUALITY_DELETE;
    delete_file.__set_field_ids({1});

    auto st = reader._equality_delete_base({delete_file});
    ASSERT_TRUE(st.ok()) << st.to_string();

    Block block;
    auto id_column = ColumnInt32::create();
    Int32 value = 7;
    id_column->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    block.insert({std::move(id_column), std::make_shared<DataTypeInt32>(), "id"});

    auto block_idx = block.get_name_to_pos_map();
    reader.set_block_index_map(&block_idx);

    st = reader.on_before_read_block(&block);
    ASSERT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("Wrong expand column 'id'"), std::string::npos);
}

TEST(IcebergEqualityDeleteTest, BuildCompositeEqualityDeleteAndFilterRows) {
    RuntimeProfile profile("iceberg_composite_delete_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);

    Block delete_batch;
    delete_batch.insert({make_nullable_string_column({"us", "cn"}),
                         make_nullable(std::make_shared<DataTypeString>()), "region"});
    delete_batch.insert({make_nullable_int32_column({7, 9}),
                         make_nullable(std::make_shared<DataTypeInt32>()), "bucket"});
    reader.enqueue_delete_reader(std::make_unique<FakeEqualityDeleteOrcReader>(
            std::vector<std::string> {"region", "bucket"},
            std::vector<DataTypePtr> {std::make_shared<DataTypeString>(),
                                      std::make_shared<DataTypeInt32>()},
            std::vector<Block> {delete_batch}));

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "memory://delete-composite.orc";
    delete_file.content = IcebergReaderMixinOrcTestAccessor::EQUALITY_DELETE;
    delete_file.__set_field_ids({10, 20});

    auto st = reader._equality_delete_base({delete_file});
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(reader.expand_col_names(), std::vector<std::string>({"region", "bucket"}));
    ASSERT_EQ(reader.equality_delete_impl_count(), 1);
    ASSERT_EQ(reader.id_to_block_column_name().at(10), "region");
    ASSERT_EQ(reader.id_to_block_column_name().at(20), "bucket");

    Block block;
    auto id_column = ColumnInt32::create();
    for (Int32 value : {1, 2, 3, 4}) {
        id_column->insert_data(reinterpret_cast<const char*>(&value), sizeof(value));
    }
    block.insert({std::move(id_column), std::make_shared<DataTypeInt32>(), "id"});

    auto block_idx = block.get_name_to_pos_map();
    reader.set_block_index_map(&block_idx);

    st = reader.on_before_read_block(&block);
    ASSERT_TRUE(st.ok()) << st.to_string();
    ASSERT_EQ(block.columns(), 3);

    block.replace_by_position(block_idx["region"],
                              make_nullable_string_column({"us", "us", "cn", "jp"}));
    block.replace_by_position(block_idx["bucket"], make_nullable_int32_column({7, 8, 9, 7}));

    size_t read_rows = block.rows();
    st = reader.on_after_read_block(&block, &read_rows);
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(read_rows, 2);
    EXPECT_EQ(block.columns(), 1);
    EXPECT_EQ(get_int32_values(*block.get_by_position(0).column), std::vector<Int32>({2, 4}));
}

TEST(IcebergInitRowFiltersTest, CountShortCircuitSkipsDeleteProcessing) {
    RuntimeProfile profile("iceberg_count_short_circuit_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.__set_table_level_row_count(10);
    scan_range.table_format_params.iceberg_params.__set_format_version(2);

    TIcebergDeleteFileDesc dv1;
    dv1.path = "memory://dv-1.bin";
    dv1.content = IcebergReaderMixinOrcTestAccessor::DELETION_VECTOR;
    TIcebergDeleteFileDesc dv2;
    dv2.path = "memory://dv-2.bin";
    dv2.content = IcebergReaderMixinOrcTestAccessor::DELETION_VECTOR;
    scan_range.table_format_params.iceberg_params.__set_delete_files({dv1, dv2});

    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);
    reader.set_push_down_agg_type(TPushAggOp::COUNT);

    auto st = reader._init_row_filters();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::COUNT);
    EXPECT_EQ(reader.equality_delete_impl_count(), 0);
}

TEST(IcebergInitRowFiltersTest, OldFormatSkipsDeleteProcessing) {
    RuntimeProfile profile("iceberg_old_format_skip_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.iceberg_params.__set_format_version(1);

    TIcebergDeleteFileDesc dv1;
    dv1.path = "memory://dv-1.bin";
    dv1.content = IcebergReaderMixinOrcTestAccessor::DELETION_VECTOR;
    TIcebergDeleteFileDesc dv2;
    dv2.path = "memory://dv-2.bin";
    dv2.content = IcebergReaderMixinOrcTestAccessor::DELETION_VECTOR;
    scan_range.table_format_params.iceberg_params.__set_delete_files({dv1, dv2});

    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);

    auto st = reader._init_row_filters();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::NONE);
    EXPECT_EQ(reader.equality_delete_impl_count(), 0);
}

TEST(IcebergInitRowFiltersTest, EqualityDeleteDisablesCountPushdown) {
    RuntimeProfile profile("iceberg_eq_delete_count_disable_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    scan_range.__isset.table_format_params = true;
    scan_range.table_format_params.iceberg_params.__set_format_version(2);

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "memory://delete-region.orc";
    delete_file.content = IcebergReaderMixinOrcTestAccessor::EQUALITY_DELETE;
    delete_file.__set_field_ids({10});
    scan_range.table_format_params.iceberg_params.__set_delete_files({delete_file});

    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);
    reader.set_push_down_agg_type(TPushAggOp::COUNT);

    Block delete_batch;
    delete_batch.insert({make_nullable_string_column({"us"}),
                         make_nullable(std::make_shared<DataTypeString>()), "region"});
    reader.enqueue_delete_reader(std::make_unique<FakeEqualityDeleteOrcReader>(
            std::vector<std::string> {"region"},
            std::vector<DataTypePtr> {std::make_shared<DataTypeString>()},
            std::vector<Block> {delete_batch}));

    auto st = reader._init_row_filters();
    ASSERT_TRUE(st.ok()) << st.to_string();
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::NONE);
    EXPECT_EQ(reader.equality_delete_impl_count(), 1);
    ASSERT_EQ(reader.expand_col_names(), std::vector<std::string>({"region"}));
}

TEST(IcebergEqualityDeleteTest, RejectDeleteFileWithoutFieldIds) {
    RuntimeProfile profile("iceberg_missing_field_ids_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "memory://delete-missing-field-ids.orc";
    delete_file.content = IcebergReaderMixinOrcTestAccessor::EQUALITY_DELETE;

    auto st = reader._equality_delete_base({delete_file});
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_NE(st.to_string().find("missing delete field ids"), std::string::npos);
}

TEST(IcebergEqualityDeleteTest, RejectUnsupportedDeleteReaderFormat) {
    RuntimeProfile profile("iceberg_unsupported_delete_reader_test");
    TFileScanRangeParams scan_params;
    TFileRangeDesc scan_range;
    IcebergReaderMixinOrcTestAccessor reader(&profile, scan_params, scan_range);
    reader.enqueue_delete_reader(std::make_unique<UnsupportedDeleteReader>());

    TIcebergDeleteFileDesc delete_file;
    delete_file.path = "memory://unsupported-delete-reader";
    delete_file.content = IcebergReaderMixinOrcTestAccessor::EQUALITY_DELETE;
    delete_file.__set_field_ids({10});

    auto st = reader._equality_delete_base({delete_file});
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::INTERNAL_ERROR>());
    EXPECT_NE(st.to_string().find("Unsupported format of delete file"), std::string::npos);
}

} // namespace doris
