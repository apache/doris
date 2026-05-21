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

#include "format/table/iceberg_reader_v2.h"

#include <gtest/gtest.h>

#include <deque>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_array.h"
#include "core/column/column_map.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_struct.h"
#include "core/field.h"
#include "format/reader/expr/literal.h"
#include "runtime/runtime_profile.h"
#include "testutil/column_helper.h"

namespace doris::iceberg {
namespace {

constexpr const char* kRowIdColumn = "_row_id";
constexpr const char* kLastUpdatedSequenceNumberColumn = "_last_updated_sequence_number";

struct FakeBatch {
    Block block;
    std::vector<int64_t> row_positions;
};

struct FakeReaderState {
    std::vector<reader::SchemaField> schema;
    std::vector<FakeBatch> batches;
    reader::FileScanRequest last_request;
    int init_count = 0;
    int close_count = 0;
    size_t next_batch = 0;
    std::vector<int64_t> current_row_positions;
    bool provide_row_positions = true;
};

class FakeFileReader final : public reader::FileReader {
public:
    explicit FakeFileReader(std::shared_ptr<FakeReaderState> state) : _state(std::move(state)) {}

    Status get_schema(std::vector<reader::SchemaField>* file_schema) const override {
        *file_schema = _state->schema;
        return Status::OK();
    }

    Status init(const reader::FileScanRequest& request) override {
        RETURN_IF_ERROR(reader::FileReader::init(request));
        _state->last_request = request;
        ++_state->init_count;
        return Status::OK();
    }

    Status next(Block* file_block, size_t* rows, bool* eof) override {
        if (_state->next_batch >= _state->batches.size()) {
            if (rows != nullptr) {
                *rows = 0;
            }
            if (eof != nullptr) {
                *eof = true;
            }
            _state->current_row_positions.clear();
            return Status::OK();
        }

        const auto& batch = _state->batches[_state->next_batch++];
        *file_block = batch.block;
        if (rows != nullptr) {
            *rows = file_block->rows();
        }
        _state->current_row_positions =
                _state->provide_row_positions ? batch.row_positions : std::vector<int64_t> {};
        if (eof != nullptr) {
            *eof = _state->next_batch >= _state->batches.size();
        }
        return Status::OK();
    }

    const std::vector<int64_t>& current_batch_row_positions() const override {
        return _state->current_row_positions;
    }

    Status close() override {
        ++_state->close_count;
        return reader::FileReader::close();
    }

private:
    std::shared_ptr<FakeReaderState> _state;
};

class FakeDataReaderFactory final : public IcebergDataReaderFactory {
public:
    Status create(const IcebergScanTask& task,
                  std::unique_ptr<reader::FileReader>* reader) override {
        created_paths.push_back(task.data_file->path);
        if (states.empty()) {
            return Status::InternalError("missing fake reader");
        }
        *reader = std::make_unique<FakeFileReader>(states.front());
        states.pop_front();
        return Status::OK();
    }

    std::deque<std::shared_ptr<FakeReaderState>> states;
    std::vector<std::string> created_paths;
};

class FakeDeleteFileLoader final : public IcebergDeleteFileLoader {
public:
    Status load_position_deletes(const IcebergDeleteFile& delete_file,
                                 const IcebergDataFile& data_file,
                                 std::vector<int64_t>* rows) override {
        position_delete_data_files.push_back(data_file.path);
        *rows = position_deletes[delete_file.path];
        return Status::OK();
    }

    Status load_deletion_vector(const IcebergDeleteFile& delete_file,
                                const IcebergDataFile& data_file,
                                std::vector<int64_t>* rows) override {
        deletion_vector_data_files.push_back(data_file.path);
        *rows = deletion_vectors[delete_file.path];
        return Status::OK();
    }

    Status load_equality_deletes(const IcebergDeleteFile& delete_file,
                                 const std::vector<reader::SchemaField>& data_file_schema,
                                 IcebergEqualityDeleteData* delete_data) override {
        equality_delete_schema_sizes.push_back(data_file_schema.size());
        *delete_data = equality_deletes[delete_file.path];
        return Status::OK();
    }

    std::map<std::string, std::vector<int64_t>> position_deletes;
    std::map<std::string, std::vector<int64_t>> deletion_vectors;
    std::map<std::string, IcebergEqualityDeleteData> equality_deletes;
    std::vector<std::string> position_delete_data_files;
    std::vector<std::string> deletion_vector_data_files;
    std::vector<size_t> equality_delete_schema_sizes;
};

DataTypePtr int64_type() {
    return std::make_shared<DataTypeInt64>();
}

DataTypePtr nullable_int64_type() {
    return std::make_shared<DataTypeNullable>(int64_type());
}

reader::SchemaField schema_field(int32_t id, std::string name, DataTypePtr type,
                                 std::optional<int32_t> field_id = std::nullopt) {
    reader::SchemaField field;
    field.id = id;
    field.name = std::move(name);
    field.type = std::move(type);
    field.field_id = field_id;
    return field;
}

reader::TableColumn table_column(int32_t id, std::string name, DataTypePtr type) {
    reader::TableColumn column;
    column.id = id;
    column.name = std::move(name);
    column.type = std::move(type);
    return column;
}

DataTypePtr struct_type(const std::vector<DataTypePtr>& types, const Strings& names) {
    return std::make_shared<DataTypeStruct>(types, names);
}

Block int64_block(const std::vector<std::pair<std::string, std::vector<int64_t>>>& columns,
                  const DataTypePtr& type) {
    Block block;
    for (const auto& [name, values] : columns) {
        block.insert({ColumnHelper::create_column<DataTypeInt64>(values), type, name});
    }
    return block;
}

Block struct_block(const std::string& name, const DataTypePtr& type,
                   const std::vector<std::vector<int64_t>>& child_values) {
    Columns child_columns;
    child_columns.reserve(child_values.size());
    for (const auto& values : child_values) {
        child_columns.push_back(ColumnHelper::create_column<DataTypeInt64>(values));
    }

    Block block;
    block.insert({ColumnStruct::create(child_columns), type, name});
    return block;
}

MutableColumnPtr offsets_column(const std::vector<uint64_t>& offsets) {
    auto column = ColumnArray::ColumnOffsets::create();
    for (auto offset : offsets) {
        column->insert_value(offset);
    }
    return column;
}

Block array_struct_block(const std::string& name, const DataTypePtr& type,
                         const std::vector<std::vector<int64_t>>& child_values,
                         const std::vector<uint64_t>& offsets) {
    Columns child_columns;
    child_columns.reserve(child_values.size());
    for (const auto& values : child_values) {
        child_columns.push_back(ColumnHelper::create_column<DataTypeInt64>(values));
    }

    auto array_column =
            ColumnArray::create(ColumnStruct::create(child_columns), offsets_column(offsets));
    Block block;
    block.insert({ColumnPtr(std::move(array_column)), type, name});
    return block;
}

Block map_struct_value_block(const std::string& name, const DataTypePtr& type,
                             const std::vector<int64_t>& keys,
                             const std::vector<std::vector<int64_t>>& value_child_values,
                             const std::vector<uint64_t>& offsets) {
    Columns value_child_columns;
    value_child_columns.reserve(value_child_values.size());
    for (const auto& values : value_child_values) {
        value_child_columns.push_back(ColumnHelper::create_column<DataTypeInt64>(values));
    }

    auto map_column =
            ColumnMap::create(ColumnHelper::create_column<DataTypeInt64>(keys),
                              ColumnStruct::create(value_child_columns), offsets_column(offsets));
    Block block;
    block.insert({ColumnPtr(std::move(map_column)), type, name});
    return block;
}

Block primitive_array_map_block(const DataTypePtr& array_type, const DataTypePtr& map_type) {
    auto array_column = ColumnArray::create(
            ColumnHelper::create_column<DataTypeInt64>({10, 11, 12}), offsets_column({2, 3}));
    auto map_column = ColumnMap::create(ColumnHelper::create_column<DataTypeInt64>({1, 2, 3}),
                                        ColumnHelper::create_column<DataTypeInt64>({100, 200, 300}),
                                        offsets_column({1, 3}));

    Block block;
    block.insert({ColumnPtr(std::move(array_column)), array_type, "items"});
    block.insert({ColumnPtr(std::move(map_column)), map_type, "props"});
    return block;
}

std::unique_ptr<IcebergScanTask> make_task(std::string path, int64_t first_row_id = -1,
                                           int64_t last_updated_sequence_number = -1) {
    auto task = std::make_unique<IcebergScanTask>();
    auto data_file = std::make_unique<IcebergDataFile>();
    data_file->path = std::move(path);
    data_file->format = "parquet";
    data_file->first_row_id = first_row_id;
    data_file->last_updated_sequence_number = last_updated_sequence_number;
    task->data_file = std::move(data_file);
    return task;
}

IcebergDeleteFile delete_file(std::string path, IcebergDeleteContent content) {
    IcebergDeleteFile file;
    file.path = std::move(path);
    file.format = "parquet";
    file.content = content;
    return file;
}

class KeepRowsExpr final : public VExpr {
public:
    explicit KeepRowsExpr(std::vector<UInt8> keep) : _keep(std::move(keep)) {
        _data_type = std::make_shared<DataTypeUInt8>();
    }

    const std::string& expr_name() const override {
        static const std::string name = "KeepRowsExpr";
        return name;
    }

    Status execute_column_impl(VExprContext* context, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        auto column = ColumnUInt8::create();
        for (size_t i = 0; i < count; ++i) {
            column->insert_value(i < _keep.size() ? _keep[i] : 0);
        }
        result_column = std::move(column);
        return Status::OK();
    }

    Status execute_filter(VExprContext* context, const Block* block,
                          uint8_t* __restrict result_filter_data, size_t rows, bool accept_null,
                          bool* can_filter_all) const override {
        bool has_surviving_row = false;
        for (size_t i = 0; i < rows; ++i) {
            result_filter_data[i] &= i < _keep.size() ? _keep[i] : 0;
            has_surviving_row = has_surviving_row || result_filter_data[i] != 0;
        }
        if (can_filter_all != nullptr) {
            *can_filter_all = !has_surviving_row;
        }
        return Status::OK();
    }

private:
    std::vector<UInt8> _keep;
};

class TestableIcebergTableReader final : public IcebergTableReader {
public:
    void set_table_filter(int32_t column_id, reader::TableFilter filter) {
        _table_filters[column_id] = std::move(filter);
    }
};

} // namespace

TEST(IcebergTableReaderV2Test, PositionDeletesFilterRowsAndUpdateRowLineage) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20, 30, 40}}}, type), {0, 1, 2, 3}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    loader->position_deletes["delete-a.parquet"] = {0, 1, 1, 3, 4};

    auto task = make_task("data-a.parquet", 1000, 77);
    auto position_delete = delete_file("delete-a.parquet", IcebergDeleteContent::POSITION_DELETE);
    position_delete.position_lower_bound = 1;
    position_delete.position_upper_bound = 3;
    task->positional_deletes.push_back(std::move(position_delete));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.table_options.projected_columns = {
            table_column(1, "id", type), table_column(100, kRowIdColumn, type),
            table_column(101, kLastUpdatedSequenceNumberColumn, type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 3);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 30);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 1000);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 1002);
    EXPECT_EQ(block.get_by_position(2).column->get_int(0), 77);
    EXPECT_EQ(block.get_by_position(2).column->get_int(1), 77);
    EXPECT_TRUE(state->last_request.need_row_positions);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1}));
    EXPECT_EQ(loader->position_delete_data_files, std::vector<std::string>({"data-a.parquet"}));
    EXPECT_EQ(state->init_count, 1);
    EXPECT_EQ(state->close_count, 1);
}

TEST(IcebergTableReaderV2Test, EqualityDeleteReadsHelperColumnAndRemovesItFromOutput) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "value", type), schema_field(2, "delete_key", type)};
    state->batches.push_back(
            {int64_block({{"value", {10, 20, 30}}, {"__iceberg_eq_delete_field_2", {1, 2, 3}}},
                         type),
             {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    IcebergEqualityDeleteData equality_delete_data;
    equality_delete_data.field_ids = {2};
    equality_delete_data.delete_block = int64_block({{"delete_key", {2}}}, type);
    loader->equality_deletes["eq-delete.parquet"] = std::move(equality_delete_data);

    auto task = make_task("data-a.parquet");
    auto equality_delete = delete_file("eq-delete.parquet", IcebergDeleteContent::EQUALITY_DELETE);
    equality_delete.equality_field_ids = {2};
    task->equality_deletes.push_back(std::move(equality_delete));

    RuntimeProfile profile("IcebergTableReaderV2Test");
    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.profile = &profile;
    params.table_options.projected_columns = {table_column(1, "value", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 1);
    EXPECT_EQ(block.get_by_position(0).name, "value");
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 30);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1, 2}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({1, 2}));
    EXPECT_FALSE(state->last_request.need_row_positions);
    ASSERT_EQ(loader->equality_delete_schema_sizes.size(), 1);
    EXPECT_EQ(loader->equality_delete_schema_sizes[0], 2);
}

TEST(IcebergTableReaderV2Test, EqualityDeleteUsesProjectedKeyWithoutHelperColumn) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "value", type), schema_field(2, "delete_key", type)};
    state->batches.push_back(
            {int64_block({{"value", {10, 20, 30}}, {"delete_key", {1, 2, 3}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    IcebergEqualityDeleteData equality_delete_data;
    equality_delete_data.field_ids = {2};
    equality_delete_data.delete_block = int64_block({{"delete_key", {2}}}, type);
    loader->equality_deletes["eq-delete.parquet"] = std::move(equality_delete_data);

    auto task = make_task("data-a.parquet");
    auto equality_delete = delete_file("eq-delete.parquet", IcebergDeleteContent::EQUALITY_DELETE);
    equality_delete.equality_field_ids = {2};
    task->equality_deletes.push_back(std::move(equality_delete));

    RuntimeProfile profile("IcebergTableReaderV2Test");
    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.profile = &profile;
    params.table_options.projected_columns = {table_column(1, "value", type),
                                              table_column(2, "delete_key", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 30);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 1);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 3);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1, 2}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({1, 2}));
}

TEST(IcebergTableReaderV2Test, EqualityDeleteMissingDataFileColumnReturnsNotSupported) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "value", type)};
    state->batches.push_back({int64_block({{"value", {10, 20, 30}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    IcebergEqualityDeleteData equality_delete_data;
    equality_delete_data.field_ids = {2};
    equality_delete_data.delete_block = int64_block({{"delete_key", {2}}}, type);
    loader->equality_deletes["eq-delete.parquet"] = std::move(equality_delete_data);

    auto task = make_task("data-a.parquet");
    auto equality_delete = delete_file("eq-delete.parquet", IcebergDeleteContent::EQUALITY_DELETE);
    equality_delete.equality_field_ids = {2};
    task->equality_deletes.push_back(std::move(equality_delete));

    RuntimeProfile profile("IcebergTableReaderV2Test");
    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.profile = &profile;
    params.table_options.projected_columns = {table_column(1, "value", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("not present in current data file"), std::string::npos);
    EXPECT_EQ(state->init_count, 0);
}

TEST(IcebergTableReaderV2Test, EqualityDeleteUsesNameMappingForMigratedHiveDataFile) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(10, "legacy_value", type),
                     schema_field(20, "legacy_delete_key", type)};
    state->batches.push_back(
            {int64_block({{"value", {10, 20, 30}}, {"__iceberg_eq_delete_field_2", {1, 2, 3}}},
                         type),
             {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    IcebergEqualityDeleteData equality_delete_data;
    equality_delete_data.field_ids = {2};
    equality_delete_data.delete_block = int64_block({{"delete_key", {2}}}, type);
    loader->equality_deletes["eq-delete.parquet"] = std::move(equality_delete_data);

    auto task = make_task("data-a.parquet");
    auto equality_delete = delete_file("eq-delete.parquet", IcebergDeleteContent::EQUALITY_DELETE);
    equality_delete.equality_field_ids = {2};
    task->equality_deletes.push_back(std::move(equality_delete));

    RuntimeProfile profile("IcebergTableReaderV2Test");
    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.profile = &profile;
    params.name_mapping = {{"legacy_value", 1}, {"legacy_delete_key", 2}};
    params.table_options.projected_columns = {table_column(1, "value", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 1);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 30);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({10, 20}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({10, 20}));
}

TEST(IcebergTableReaderV2Test, DeletionVectorRowsFilterDataAndRequestPositions) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20, 30, 40, 50}}}, type), {0, 1, 2, 3, 4}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    loader->position_deletes["delete-a.parquet"] = {1, 3, 3};
    loader->deletion_vectors["dv-a"] = {0, 3};

    auto task = make_task("data-a.parquet");
    task->positional_deletes.push_back(
            delete_file("delete-a.parquet", IcebergDeleteContent::POSITION_DELETE));
    auto dv = delete_file("dv-a", IcebergDeleteContent::DELETION_VECTOR);
    dv.content_offset = 7;
    dv.content_size_in_bytes = 11;
    task->deletion_vectors.push_back(std::move(dv));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.table_options.projected_columns = {table_column(1, "id", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 30);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 50);
    EXPECT_TRUE(state->last_request.need_row_positions);
    EXPECT_EQ(loader->position_delete_data_files, std::vector<std::string>({"data-a.parquet"}));
    EXPECT_EQ(loader->deletion_vector_data_files, std::vector<std::string>({"data-a.parquet"}));
}

TEST(IcebergTableReaderV2Test, MultipleDeletionVectorsReturnDataQualityError) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10}}}, type), {0}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");
    auto first_dv = delete_file("dv-a", IcebergDeleteContent::DELETION_VECTOR);
    first_dv.content_offset = 0;
    first_dv.content_size_in_bytes = 10;
    auto second_dv = delete_file("dv-b", IcebergDeleteContent::DELETION_VECTOR);
    second_dv.content_offset = 10;
    second_dv.content_size_in_bytes = 10;
    task->deletion_vectors.push_back(std::move(first_dv));
    task->deletion_vectors.push_back(std::move(second_dv));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = std::make_shared<FakeDeleteFileLoader>();
    params.table_options.projected_columns = {table_column(1, "id", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("multiple DVs"), std::string::npos);
    EXPECT_EQ(state->init_count, 0);
}

TEST(IcebergTableReaderV2Test, NegativePositionDeleteReturnsDataQualityError) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10}}}, type), {0}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    loader->position_deletes["delete-a.parquet"] = {-1};

    auto task = make_task("data-a.parquet");
    task->positional_deletes.push_back(
            delete_file("delete-a.parquet", IcebergDeleteContent::POSITION_DELETE));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.table_options.projected_columns = {table_column(1, "id", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("non-negative"), std::string::npos);
    EXPECT_EQ(state->init_count, 0);
}

TEST(IcebergTableReaderV2Test, PositionDeleteRequiresReaderRowPositions) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20}}}, type), {0, 1}});
    state->provide_row_positions = false;

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    loader->position_deletes["delete-a.parquet"] = {1};

    auto task = make_task("data-a.parquet");
    task->positional_deletes.push_back(
            delete_file("delete-a.parquet", IcebergDeleteContent::POSITION_DELETE));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.table_options.projected_columns = {table_column(1, "id", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    EXPECT_FALSE(st.ok());
    EXPECT_NE(st.to_string().find("requires row positions"), std::string::npos);
    EXPECT_TRUE(state->last_request.need_row_positions);
}

TEST(IcebergTableReaderV2Test, TableFilterOnProjectedColumnIsLocalizedToFileRequest) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20, 30}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {table_column(1, "id", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    TestableIcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    reader::TableFilter filter;
    filter.conjunct = VExprContext::create_shared(
            std::make_shared<KeepRowsExpr>(std::vector<UInt8> {1, 0, 1}));
    table_reader.set_table_filter(1, std::move(filter));

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(state->last_request.local_filters.size(), 1);
    EXPECT_EQ(state->last_request.local_filters[0].file_column_id, 1);
    EXPECT_NE(state->last_request.local_filters[0].conjunct, nullptr);
    EXPECT_EQ(state->last_request.predicate_columns, std::vector<reader::ColumnId>({1}));
    EXPECT_TRUE(state->last_request.non_predicate_columns.empty());
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1}));
    EXPECT_TRUE(state->last_request.reader_expression_map.empty());
}

TEST(IcebergTableReaderV2Test, ProjectedFileColumnsKeepTableBlockOrderWithPredicateColumn) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type), schema_field(2, "value", type)};
    state->batches.push_back({int64_block({{"id", {10, 20}}, {"value", {100, 200}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {table_column(1, "id", type),
                                              table_column(2, "value", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    TestableIcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    reader::TableFilter filter;
    filter.conjunct =
            VExprContext::create_shared(std::make_shared<KeepRowsExpr>(std::vector<UInt8> {1, 1}));
    table_reader.set_table_filter(1, std::move(filter));

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    EXPECT_EQ(state->last_request.predicate_columns, std::vector<reader::ColumnId>({1}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({2}));
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1, 2}));
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(0).name, "id");
    EXPECT_EQ(block.get_by_position(1).name, "value");
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 100);
}

TEST(IcebergTableReaderV2Test, ResidualExprFiltersAfterProjectionAndBeforeDeletes) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20, 30, 40}}}, type), {0, 1, 2, 3}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    loader->position_deletes["delete-a.parquet"] = {2};

    auto task = make_task("data-a.parquet", 1000);
    task->positional_deletes.push_back(
            delete_file("delete-a.parquet", IcebergDeleteContent::POSITION_DELETE));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.table_options.projected_columns = {table_column(1, "id", type),
                                              table_column(100, kRowIdColumn, type)};
    params.table_options.conjuncts = {VExprContext::create_shared(
            std::make_shared<KeepRowsExpr>(std::vector<UInt8> {1, 0, 1, 1}))};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 40);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 1000);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 1003);
    EXPECT_TRUE(state->last_request.local_filters.empty());
    EXPECT_TRUE(state->last_request.reader_expression_map.empty());
    EXPECT_TRUE(state->last_request.need_row_positions);
}

TEST(IcebergTableReaderV2Test, RowLineageColumnsMaterializeNullsWhenMetadataIsMissing) {
    auto type = int64_type();
    auto lineage_type = nullable_int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20}}}, type), {0, 1}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {
            table_column(1, "id", type), table_column(100, kRowIdColumn, lineage_type),
            table_column(101, kLastUpdatedSequenceNumberColumn, lineage_type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 3);
    EXPECT_TRUE(block.get_by_position(1).column->is_null_at(0));
    EXPECT_TRUE(block.get_by_position(1).column->is_null_at(1));
    EXPECT_TRUE(block.get_by_position(2).column->is_null_at(0));
    EXPECT_TRUE(block.get_by_position(2).column->is_null_at(1));
    EXPECT_TRUE(state->last_request.need_row_positions);
}

TEST(IcebergTableReaderV2Test, RowLineageColumnsReadPhysicalColumnsWhenPresent) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type), schema_field(10, kRowIdColumn, type, 100),
                     schema_field(11, kLastUpdatedSequenceNumberColumn, type, 101)};
    state->batches.push_back({int64_block({{"id", {10, 20}},
                                           {kRowIdColumn, {700, 701}},
                                           {kLastUpdatedSequenceNumberColumn, {900, 901}}},
                                          type),
                              {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet", 1000, 77);

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {
            table_column(1, "id", type), table_column(100, kRowIdColumn, type),
            table_column(101, kLastUpdatedSequenceNumberColumn, type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 700);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 701);
    EXPECT_EQ(block.get_by_position(2).column->get_int(0), 900);
    EXPECT_EQ(block.get_by_position(2).column->get_int(1), 901);
    EXPECT_FALSE(state->last_request.need_row_positions);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1, 10, 11}));
    EXPECT_EQ(state->last_request.non_predicate_columns,
              std::vector<reader::ColumnId>({1, 10, 11}));
}

TEST(IcebergTableReaderV2Test, RowLineageFiltersDoNotPushDownToFilePredicates) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(10, kRowIdColumn, type, 100)};
    state->batches.push_back({int64_block({{kRowIdColumn, {700, 701}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {table_column(100, kRowIdColumn, type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    TestableIcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    reader::TableFilter filter;
    filter.conjunct =
            VExprContext::create_shared(std::make_shared<KeepRowsExpr>(std::vector<UInt8> {1, 1}));
    table_reader.set_table_filter(100, std::move(filter));

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    EXPECT_TRUE(state->last_request.local_filters.empty());
    EXPECT_TRUE(state->last_request.predicate_columns.empty());
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({10}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({10}));
    EXPECT_FALSE(state->last_request.need_row_positions);
    ASSERT_EQ(block.rows(), 2);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 700);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 701);
}

TEST(IcebergTableReaderV2Test, SchemaEvolutionUsesNameMappingForMigratedHiveDataFile) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(10, "legacy_id", type), schema_field(20, "legacy_value", type)};
    state->batches.push_back(
            {int64_block({{"renamed_value", {200, 201}}, {"renamed_id", {100, 101}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.name_mapping = {{"legacy_id", 1}, {"legacy_value", 2}};
    params.table_options.projected_columns = {table_column(2, "renamed_value", type),
                                              table_column(1, "renamed_id", type)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(0).name, "renamed_value");
    EXPECT_EQ(block.get_by_position(1).name, "renamed_id");
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 200);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 201);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 100);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 101);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({20, 10}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({20, 10}));
}

TEST(IcebergTableReaderV2Test,
     SchemaEvolutionUsesFieldIdsAndMaterializesMissingAndPartitionColumns) {
    auto type = int64_type();
    auto missing_type = nullable_int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "old_first_name", type),
                     schema_field(2, "old_renamed_name", type)};
    state->batches.push_back(
            {int64_block({{"renamed", {200, 201}}, {"first", {100, 101}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn partition_column = table_column(4, "part", type);
    partition_column.is_partition_key = true;

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {
            table_column(2, "renamed", type), table_column(1, "first", type),
            table_column(3, "missing", missing_type), std::move(partition_column)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    reader::SplitReadOptions split_options;
    split_options.partition_values["part"] =
            Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(9000));
    ASSERT_TRUE(table_reader.prepare_split(std::move(split_options)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 4);
    EXPECT_EQ(block.get_by_position(0).name, "renamed");
    EXPECT_EQ(block.get_by_position(1).name, "first");
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 200);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 201);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 100);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 101);
    EXPECT_TRUE(block.get_by_position(2).column->is_null_at(0));
    EXPECT_TRUE(block.get_by_position(2).column->is_null_at(1));
    EXPECT_EQ(block.get_by_position(3).column->get_int(0), 9000);
    EXPECT_EQ(block.get_by_position(3).column->get_int(1), 9000);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({2, 1}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({2, 1}));
}

TEST(IcebergTableReaderV2Test, StructSchemaEvolutionRemapsChildrenByFieldIds) {
    auto type = int64_type();
    auto nullable_type = nullable_int64_type();
    auto file_struct_type = struct_type({type, type}, {"legacy_id", "legacy_score"});
    auto table_struct_type = struct_type({type, type, nullable_type}, {"score", "id", "missing"});

    auto state = std::make_shared<FakeReaderState>();
    auto profile_field = schema_field(7, "legacy_profile", file_struct_type, 10);
    profile_field.children = {schema_field(0, "legacy_id", type, 101),
                              schema_field(1, "legacy_score", type, 102)};
    state->schema = {std::move(profile_field)};
    state->batches.push_back(
            {struct_block("profile", file_struct_type, {{100, 101}, {200, 201}}), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn profile = table_column(10, "profile", table_struct_type);
    profile.children = {table_column(102, "score", type), table_column(101, "id", type),
                        table_column(103, "missing", nullable_type)};

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {std::move(profile)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    ASSERT_EQ(block.columns(), 1);
    const auto& profile_column = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    ASSERT_EQ(profile_column.tuple_size(), 3);
    EXPECT_EQ(profile_column.get_column(0).get_int(0), 200);
    EXPECT_EQ(profile_column.get_column(0).get_int(1), 201);
    EXPECT_EQ(profile_column.get_column(1).get_int(0), 100);
    EXPECT_EQ(profile_column.get_column(1).get_int(1), 101);
    EXPECT_TRUE(profile_column.get_column(2).is_null_at(0));
    EXPECT_TRUE(profile_column.get_column(2).is_null_at(1));
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({7}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({7}));
}

TEST(IcebergTableReaderV2Test, StructSchemaEvolutionUsesNestedNameMappingForMigratedHiveFile) {
    auto type = int64_type();
    auto file_struct_type = struct_type({type, type}, {"legacy_id", "legacy_score"});
    auto table_struct_type = struct_type({type, type}, {"score", "id"});

    auto state = std::make_shared<FakeReaderState>();
    auto profile_field = schema_field(17, "legacy_profile", file_struct_type);
    profile_field.children = {schema_field(0, "legacy_id", type),
                              schema_field(1, "legacy_score", type)};
    state->schema = {std::move(profile_field)};
    state->batches.push_back(
            {struct_block("profile", file_struct_type, {{300, 301}, {400, 401}}), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn profile = table_column(10, "profile", table_struct_type);
    profile.children = {table_column(102, "score", type), table_column(101, "id", type)};

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.name_mapping = {{"legacy_profile", 10},
                           {"legacy_profile.legacy_id", 101},
                           {"legacy_profile.legacy_score", 102}};
    params.table_options.projected_columns = {std::move(profile)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    const auto& profile_column = assert_cast<const ColumnStruct&>(*block.get_by_position(0).column);
    ASSERT_EQ(profile_column.tuple_size(), 2);
    EXPECT_EQ(profile_column.get_column(0).get_int(0), 400);
    EXPECT_EQ(profile_column.get_column(0).get_int(1), 401);
    EXPECT_EQ(profile_column.get_column(1).get_int(0), 300);
    EXPECT_EQ(profile_column.get_column(1).get_int(1), 301);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({17}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({17}));
}

TEST(IcebergTableReaderV2Test, ArrayOfStructSchemaEvolutionRemapsElementStruct) {
    auto type = int64_type();
    auto nullable_type = nullable_int64_type();
    auto file_struct_type = struct_type({type, type}, {"legacy_id", "legacy_score"});
    auto table_struct_type = struct_type({type, type, nullable_type}, {"score", "id", "missing"});
    auto file_array_type = std::make_shared<DataTypeArray>(file_struct_type);
    auto table_array_type = std::make_shared<DataTypeArray>(table_struct_type);

    auto state = std::make_shared<FakeReaderState>();
    auto items_field = schema_field(8, "items", file_array_type, 10);
    auto element_field = schema_field(0, "element", file_struct_type, 11);
    element_field.children = {schema_field(0, "legacy_id", type, 101),
                              schema_field(1, "legacy_score", type, 102)};
    items_field.children = {std::move(element_field)};
    state->schema = {std::move(items_field)};
    state->batches.push_back({array_struct_block("items", file_array_type,
                                                 {{100, 101, 102}, {200, 201, 202}}, {2, 3}),
                              {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn element = table_column(11, "element", table_struct_type);
    element.children = {table_column(102, "score", type), table_column(101, "id", type),
                        table_column(103, "missing", nullable_type)};
    reader::TableColumn items = table_column(10, "items", table_array_type);
    items.children = {std::move(element)};

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {std::move(items)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    const auto& items_column = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    ASSERT_EQ(items_column.get_offsets().size(), 2);
    EXPECT_EQ(items_column.get_offsets()[0], 2);
    EXPECT_EQ(items_column.get_offsets()[1], 3);
    const auto& element_struct = assert_cast<const ColumnStruct&>(items_column.get_data());
    ASSERT_EQ(element_struct.tuple_size(), 3);
    EXPECT_EQ(element_struct.get_column(0).get_int(0), 200);
    EXPECT_EQ(element_struct.get_column(0).get_int(2), 202);
    EXPECT_EQ(element_struct.get_column(1).get_int(0), 100);
    EXPECT_EQ(element_struct.get_column(1).get_int(2), 102);
    EXPECT_TRUE(element_struct.get_column(2).is_null_at(0));
    EXPECT_TRUE(element_struct.get_column(2).is_null_at(2));
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({8}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({8}));
}

TEST(IcebergTableReaderV2Test, MapValueStructSchemaEvolutionRemapsValueStruct) {
    auto type = int64_type();
    auto nullable_type = nullable_int64_type();
    auto file_struct_type = struct_type({type, type}, {"legacy_id", "legacy_score"});
    auto table_struct_type = struct_type({type, type, nullable_type}, {"score", "id", "missing"});
    auto file_map_type = std::make_shared<DataTypeMap>(type, file_struct_type);
    auto table_map_type = std::make_shared<DataTypeMap>(type, table_struct_type);

    auto state = std::make_shared<FakeReaderState>();
    auto props_field = schema_field(9, "props", file_map_type, 20);
    auto key_field = schema_field(0, "key", type, 21);
    auto value_field = schema_field(1, "value", file_struct_type, 22);
    value_field.children = {schema_field(0, "legacy_id", type, 101),
                            schema_field(1, "legacy_score", type, 102)};
    props_field.children = {std::move(key_field), std::move(value_field)};
    state->schema = {std::move(props_field)};
    state->batches.push_back({map_struct_value_block("props", file_map_type, {1, 2, 3},
                                                     {{300, 301, 302}, {400, 401, 402}}, {2, 3}),
                              {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn value = table_column(22, "value", table_struct_type);
    value.children = {table_column(102, "score", type), table_column(101, "id", type),
                      table_column(103, "missing", nullable_type)};
    reader::TableColumn props = table_column(20, "props", table_map_type);
    props.children = {table_column(21, "key", type), std::move(value)};

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {std::move(props)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    const auto& props_column = assert_cast<const ColumnMap&>(*block.get_by_position(0).column);
    ASSERT_EQ(props_column.get_offsets().size(), 2);
    EXPECT_EQ(props_column.get_offsets()[0], 2);
    EXPECT_EQ(props_column.get_offsets()[1], 3);
    EXPECT_EQ(props_column.get_keys().get_int(0), 1);
    EXPECT_EQ(props_column.get_keys().get_int(2), 3);
    const auto& value_struct = assert_cast<const ColumnStruct&>(props_column.get_values());
    ASSERT_EQ(value_struct.tuple_size(), 3);
    EXPECT_EQ(value_struct.get_column(0).get_int(0), 400);
    EXPECT_EQ(value_struct.get_column(0).get_int(2), 402);
    EXPECT_EQ(value_struct.get_column(1).get_int(0), 300);
    EXPECT_EQ(value_struct.get_column(1).get_int(2), 302);
    EXPECT_TRUE(value_struct.get_column(2).is_null_at(0));
    EXPECT_TRUE(value_struct.get_column(2).is_null_at(2));
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({9}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({9}));
}

TEST(IcebergTableReaderV2Test, PrimitiveArrayAndMapChildrenStayTrivial) {
    auto type = int64_type();
    auto array_type = std::make_shared<DataTypeArray>(type);
    auto map_type = std::make_shared<DataTypeMap>(type, type);

    auto state = std::make_shared<FakeReaderState>();
    auto items_field = schema_field(8, "items", array_type, 10);
    items_field.children = {schema_field(0, "element", type)};
    auto props_field = schema_field(9, "props", map_type, 20);
    props_field.children = {schema_field(0, "key", type), schema_field(1, "value", type)};
    state->schema = {std::move(items_field), std::move(props_field)};
    state->batches.push_back({primitive_array_map_block(array_type, map_type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn items = table_column(10, "items", array_type);
    items.children = {table_column(11, "element", type)};
    reader::TableColumn props = table_column(20, "props", map_type);
    props.children = {table_column(21, "key", type), table_column(22, "value", type)};

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {std::move(items), std::move(props)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 2);
    const auto& items_column = assert_cast<const ColumnArray&>(*block.get_by_position(0).column);
    EXPECT_EQ(items_column.get_offsets()[0], 2);
    EXPECT_EQ(items_column.get_offsets()[1], 3);
    EXPECT_EQ(items_column.get_data().get_int(0), 10);
    EXPECT_EQ(items_column.get_data().get_int(2), 12);

    const auto& props_column = assert_cast<const ColumnMap&>(*block.get_by_position(1).column);
    EXPECT_EQ(props_column.get_offsets()[0], 1);
    EXPECT_EQ(props_column.get_offsets()[1], 3);
    EXPECT_EQ(props_column.get_keys().get_int(0), 1);
    EXPECT_EQ(props_column.get_values().get_int(2), 300);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({8, 9}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({8, 9}));
}

TEST(IcebergTableReaderV2Test, SchemaEvolutionMaterializesIcebergDefaultValues) {
    auto type = int64_type();
    auto state = std::make_shared<FakeReaderState>();
    state->schema = {schema_field(1, "id", type)};
    state->batches.push_back({int64_block({{"id", {10, 20, 30}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(state);

    auto task = make_task("data-a.parquet");

    reader::TableColumn default_column = table_column(3, "with_default", type);
    default_column.default_expr = VExprContext::create_shared(TableLiteral::create_shared(
            type, Field::create_field<TYPE_BIGINT>(static_cast<int64_t>(42))));

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.table_options.projected_columns = {table_column(1, "id", type),
                                              std::move(default_column)};
    params.table_options.scan_tasks.push_back(std::move(task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block block;
    bool eos = false;
    auto st = table_reader.get_block(&block, &eos);
    ASSERT_TRUE(st.ok()) << st;

    ASSERT_EQ(block.rows(), 3);
    ASSERT_EQ(block.columns(), 2);
    EXPECT_EQ(block.get_by_position(0).column->get_int(0), 10);
    EXPECT_EQ(block.get_by_position(0).column->get_int(1), 20);
    EXPECT_EQ(block.get_by_position(0).column->get_int(2), 30);
    EXPECT_EQ(block.get_by_position(1).column->get_int(0), 42);
    EXPECT_EQ(block.get_by_position(1).column->get_int(1), 42);
    EXPECT_EQ(block.get_by_position(1).column->get_int(2), 42);
    EXPECT_EQ(state->last_request.projected_columns, std::vector<reader::ColumnId>({1}));
    EXPECT_EQ(state->last_request.non_predicate_columns, std::vector<reader::ColumnId>({1}));
}

TEST(IcebergTableReaderV2Test, MultipleFilesResetRowPositionAndDeleteState) {
    auto type = int64_type();
    auto first_state = std::make_shared<FakeReaderState>();
    first_state->schema = {schema_field(1, "id", type)};
    first_state->batches.push_back({int64_block({{"id", {10, 20}}}, type), {0, 1}});

    auto second_state = std::make_shared<FakeReaderState>();
    second_state->schema = {schema_field(1, "id", type)};
    second_state->batches.push_back({int64_block({{"id", {30, 40}}}, type), {}});

    auto factory = std::make_shared<FakeDataReaderFactory>();
    factory->states.push_back(first_state);
    factory->states.push_back(second_state);

    auto loader = std::make_shared<FakeDeleteFileLoader>();
    loader->position_deletes["delete-a.parquet"] = {0};

    auto first_task = make_task("data-a.parquet");
    first_task->positional_deletes.push_back(
            delete_file("delete-a.parquet", IcebergDeleteContent::POSITION_DELETE));
    auto second_task = make_task("data-b.parquet");

    IcebergTableReadParams params;
    params.data_reader_factory = factory;
    params.delete_file_loader = loader;
    params.table_options.projected_columns = {table_column(1, "id", type)};
    params.table_options.scan_tasks.push_back(std::move(first_task));
    params.table_options.scan_tasks.push_back(std::move(second_task));

    IcebergTableReader table_reader;
    ASSERT_TRUE(table_reader.init(std::move(params)).ok());

    Block first_block;
    bool eos = false;
    auto st = table_reader.get_block(&first_block, &eos);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(first_block.rows(), 1);
    EXPECT_EQ(first_block.get_by_position(0).column->get_int(0), 20);
    EXPECT_TRUE(first_state->last_request.need_row_positions);
    EXPECT_EQ(first_state->close_count, 1);
    EXPECT_FALSE(eos);

    Block second_block;
    st = table_reader.get_block(&second_block, &eos);
    ASSERT_TRUE(st.ok()) << st;
    ASSERT_EQ(second_block.rows(), 2);
    EXPECT_EQ(second_block.get_by_position(0).column->get_int(0), 30);
    EXPECT_EQ(second_block.get_by_position(0).column->get_int(1), 40);
    EXPECT_FALSE(second_state->last_request.need_row_positions);
    EXPECT_EQ(second_state->close_count, 1);
    EXPECT_EQ(factory->created_paths,
              std::vector<std::string>({"data-a.parquet", "data-b.parquet"}));
}

} // namespace doris::iceberg
