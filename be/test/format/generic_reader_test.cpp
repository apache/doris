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

#include "format/generic_reader.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/data_type/data_type_number.h"
#include "format/column_descriptor.h"

namespace doris {

// ============================================================================
// Mock Reader: Records hook call order for verifying NVI template sequence.
// ============================================================================
class MockReader : public GenericReader {
public:
    // Accumulated log of method calls for sequence verification.
    std::vector<std::string> call_log;

    // Configurable return statuses for each hook.
    Status open_file_status = Status::OK();
    Status before_init_status = Status::OK();
    Status do_init_status = Status::OK();
    Status after_init_status = Status::OK();
    Status before_read_status = Status::OK();
    Status do_read_status = Status::OK();
    Status after_read_status = Status::OK();

    // _do_get_next_block behavior.
    int batches_to_emit = 1;
    size_t rows_per_batch = 10;

protected:
    Status _open_file_reader(ReaderInitContext* ctx) override {
        call_log.push_back("_open_file_reader");
        return open_file_status;
    }

    Status on_before_init_reader(ReaderInitContext* ctx) override {
        call_log.push_back("on_before_init_reader");
        _column_descs = ctx->column_descs;
        return before_init_status;
    }

    Status _do_init_reader(ReaderInitContext* ctx) override {
        call_log.push_back("_do_init_reader");
        return do_init_status;
    }

    Status on_after_init_reader(ReaderInitContext* ctx) override {
        call_log.push_back("on_after_init_reader");
        return after_init_status;
    }

    Status on_before_read_block(Block* block) override {
        call_log.push_back("on_before_read_block");
        return before_read_status;
    }

    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        call_log.push_back("_do_get_next_block");
        if (batches_to_emit <= 0) {
            *read_rows = 0;
            *eof = true;
            return do_read_status;
        }
        batches_to_emit--;
        *read_rows = rows_per_batch;
        *eof = (batches_to_emit == 0);
        // Resize columns to match read_rows.
        auto mutate_columns = block->mutate_columns();
        for (auto& col : mutate_columns) {
            col->resize(*read_rows);
        }
        block->set_columns(std::move(mutate_columns));
        return do_read_status;
    }

    Status on_after_read_block(Block* block, size_t* read_rows) override {
        call_log.push_back("on_after_read_block");
        return after_read_status;
    }
};

// A custom ReaderInitContext subclass for testing checked_context_cast.
struct TestInitContext : public ReaderInitContext {
    int custom_field = 42;
};

// ============================================================================
// Helpers
// ============================================================================
static Block make_block() {
    Block block;
    auto type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
    block.insert({type->create_column(), type, "col0"});
    return block;
}

// ============================================================================
// Test: init_reader hook sequence with column_descs (normal path)
// ============================================================================
TEST(GenericReaderNVITest, InitReaderNormalSequence) {
    MockReader reader;
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;

    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Verify exact call order: open → before → do → after
    ASSERT_EQ(reader.call_log.size(), 4);
    EXPECT_EQ(reader.call_log[0], "_open_file_reader");
    EXPECT_EQ(reader.call_log[1], "on_before_init_reader");
    EXPECT_EQ(reader.call_log[2], "_do_init_reader");
    EXPECT_EQ(reader.call_log[3], "on_after_init_reader");
}

// ============================================================================
// Test: init_reader standalone mode (column_descs == nullptr skips hooks)
// ============================================================================
TEST(GenericReaderNVITest, InitReaderStandaloneSkipsHooks) {
    MockReader reader;
    ReaderInitContext ctx;
    ctx.column_descs = nullptr;
    ctx.column_names = {"col_a", "col_b"};

    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok()) << st.to_string();

    // Standalone: open → do_init only (no before/after hooks)
    ASSERT_EQ(reader.call_log.size(), 2);
    EXPECT_EQ(reader.call_log[0], "_open_file_reader");
    EXPECT_EQ(reader.call_log[1], "_do_init_reader");
}

// ============================================================================
// Test: init_reader propagates push_down_agg_type from context
// ============================================================================
TEST(GenericReaderNVITest, InitReaderSetsPushDownAggType) {
    MockReader reader;
    ReaderInitContext ctx;
    ctx.push_down_agg_type = TPushAggOp::type::COUNT;
    ctx.column_descs = nullptr;

    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::COUNT);
}

// ============================================================================
// Test: init_reader error in _open_file_reader aborts early
// ============================================================================
TEST(GenericReaderNVITest, InitReaderOpenFileError) {
    MockReader reader;
    reader.open_file_status = Status::IOError("disk failure");
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;

    auto st = reader.init_reader(&ctx);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(st.is<ErrorCode::IO_ERROR>());

    // Only _open_file_reader should have been called.
    ASSERT_EQ(reader.call_log.size(), 1);
    EXPECT_EQ(reader.call_log[0], "_open_file_reader");
}

// ============================================================================
// Test: init_reader error in on_before_init_reader aborts before _do_init
// ============================================================================
TEST(GenericReaderNVITest, InitReaderBeforeInitError) {
    MockReader reader;
    reader.before_init_status = Status::InternalError("schema mismatch");
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;

    auto st = reader.init_reader(&ctx);
    ASSERT_FALSE(st.ok());

    ASSERT_EQ(reader.call_log.size(), 2);
    EXPECT_EQ(reader.call_log[0], "_open_file_reader");
    EXPECT_EQ(reader.call_log[1], "on_before_init_reader");
}

// ============================================================================
// Test: init_reader error in _do_init_reader aborts before on_after
// ============================================================================
TEST(GenericReaderNVITest, InitReaderDoInitError) {
    MockReader reader;
    reader.do_init_status = Status::Corruption("bad footer");
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;

    auto st = reader.init_reader(&ctx);
    ASSERT_FALSE(st.ok());

    ASSERT_EQ(reader.call_log.size(), 3);
    EXPECT_EQ(reader.call_log[2], "_do_init_reader");
}

// ============================================================================
// Test: init_reader error in on_after_init_reader returns error
// ============================================================================
TEST(GenericReaderNVITest, InitReaderAfterInitError) {
    MockReader reader;
    reader.after_init_status = Status::InternalError("delete file failed");
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;

    auto st = reader.init_reader(&ctx);
    ASSERT_FALSE(st.ok());

    // All 4 hooks called, but the last one returned error.
    ASSERT_EQ(reader.call_log.size(), 4);
    EXPECT_EQ(reader.call_log[3], "on_after_init_reader");
}

// ============================================================================
// Test: get_next_block hook sequence
// ============================================================================
TEST(GenericReaderNVITest, GetNextBlockSequence) {
    MockReader reader;
    reader.batches_to_emit = 1;
    reader.rows_per_batch = 5;

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st.to_string();

    ASSERT_EQ(reader.call_log.size(), 3);
    EXPECT_EQ(reader.call_log[0], "on_before_read_block");
    EXPECT_EQ(reader.call_log[1], "_do_get_next_block");
    EXPECT_EQ(reader.call_log[2], "on_after_read_block");
    EXPECT_EQ(read_rows, 5);
    EXPECT_TRUE(eof);
}

// ============================================================================
// Test: get_next_block error in on_before_read_block
// ============================================================================
TEST(GenericReaderNVITest, GetNextBlockBeforeReadError) {
    MockReader reader;
    reader.before_read_status = Status::InternalError("expand failed");

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_FALSE(st.ok());

    ASSERT_EQ(reader.call_log.size(), 1);
    EXPECT_EQ(reader.call_log[0], "on_before_read_block");
}

// ============================================================================
// Test: get_next_block error in _do_get_next_block
// ============================================================================
TEST(GenericReaderNVITest, GetNextBlockDoReadError) {
    MockReader reader;
    reader.do_read_status = Status::IOError("read error");
    reader.batches_to_emit = 1;

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_FALSE(st.ok());

    ASSERT_EQ(reader.call_log.size(), 2);
    EXPECT_EQ(reader.call_log[1], "_do_get_next_block");
}

// ============================================================================
// Test: get_next_block error in on_after_read_block
// ============================================================================
TEST(GenericReaderNVITest, GetNextBlockAfterReadError) {
    MockReader reader;
    reader.after_read_status = Status::InternalError("shrink failed");
    reader.batches_to_emit = 1;

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_FALSE(st.ok());

    ASSERT_EQ(reader.call_log.size(), 3);
    EXPECT_EQ(reader.call_log[2], "on_after_read_block");
}

// ============================================================================
// Test: get_next_block multi-batch reading
// ============================================================================
TEST(GenericReaderNVITest, GetNextBlockMultiBatch) {
    MockReader reader;
    reader.batches_to_emit = 3;
    reader.rows_per_batch = 100;

    size_t total_rows = 0;
    int batch_count = 0;
    bool eof = false;
    while (!eof) {
        Block block = make_block();
        size_t read_rows = 0;
        auto st = reader.get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok()) << st.to_string();
        total_rows += read_rows;
        batch_count++;
    }
    EXPECT_EQ(total_rows, 300);
    EXPECT_EQ(batch_count, 3);
    // Each batch triggers 3 hooks → 9 total calls.
    EXPECT_EQ(reader.call_log.size(), 9);
}

// ============================================================================
// Test: get_next_block with zero batches (immediate EOF)
// ============================================================================
TEST(GenericReaderNVITest, GetNextBlockImmediateEof) {
    MockReader reader;
    reader.batches_to_emit = 0;

    Block block = make_block();
    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(read_rows, 0);
    EXPECT_TRUE(eof);
}

// ============================================================================
// Test: checked_context_cast succeeds for correct type
// ============================================================================
TEST(GenericReaderNVITest, CheckedContextCastSuccess) {
    TestInitContext test_ctx;
    test_ctx.custom_field = 99;

    auto* casted = checked_context_cast<TestInitContext>(&test_ctx);
    ASSERT_NE(casted, nullptr);
    EXPECT_EQ(casted->custom_field, 99);
}

// ============================================================================
// Test: checked_context_cast from base pointer
// ============================================================================
TEST(GenericReaderNVITest, CheckedContextCastFromBase) {
    TestInitContext test_ctx;
    test_ctx.custom_field = 77;
    ReaderInitContext* base_ptr = &test_ctx;

    auto* casted = checked_context_cast<TestInitContext>(base_ptr);
    ASSERT_NE(casted, nullptr);
    EXPECT_EQ(casted->custom_field, 77);
}

// ============================================================================
// Test: push_down_agg_type getter/setter
// ============================================================================
TEST(GenericReaderNVITest, PushDownAggType) {
    MockReader reader;
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::NONE);

    reader.set_push_down_agg_type(TPushAggOp::type::COUNT);
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::COUNT);

    reader.set_push_down_agg_type(TPushAggOp::type::NONE);
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::NONE);
}

// ============================================================================
// Test: has_column_descs tracks state from on_before_init_reader
// ============================================================================
TEST(GenericReaderNVITest, HasColumnDescs) {
    MockReader reader;
    EXPECT_FALSE(reader.has_column_descs());

    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    EXPECT_TRUE(reader.has_column_descs());
}

// ============================================================================
// Test: has_column_descs remains false for standalone reader
// ============================================================================
TEST(GenericReaderNVITest, HasColumnDescsStandalone) {
    MockReader reader;
    ReaderInitContext ctx;
    ctx.column_descs = nullptr;

    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    // Standalone path skips on_before_init_reader → _column_descs stays null.
    EXPECT_FALSE(reader.has_column_descs());
}

// ============================================================================
// Test: get_columns caching behavior
// ============================================================================
class MockReaderWithColumns : public MockReader {
public:
    int get_columns_call_count = 0;

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override {
        get_columns_call_count++;
        (*name_to_type)["col0"] = std::make_shared<DataTypeInt64>();
        (*name_to_type)["col1"] = std::make_shared<DataTypeInt64>();
        return Status::OK();
    }
};

TEST(GenericReaderNVITest, GetColumnsCaching) {
    MockReaderWithColumns reader;

    std::unordered_map<std::string, DataTypePtr> result1;
    auto st = reader.get_columns(&result1);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(result1.size(), 2);
    EXPECT_EQ(reader.get_columns_call_count, 1);

    // Second call should use cache.
    std::unordered_map<std::string, DataTypePtr> result2;
    st = reader.get_columns(&result2);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(result2.size(), 2);
    EXPECT_EQ(reader.get_columns_call_count, 1); // Still 1, cached.
}

// ============================================================================
// Test: default virtual method behaviors
// ============================================================================
TEST(GenericReaderNVITest, DefaultVirtualMethods) {
    MockReader reader;

    EXPECT_FALSE(reader.count_read_rows());
    EXPECT_FALSE(reader.supports_count_pushdown());
    EXPECT_EQ(reader.get_total_rows(), 0);
    EXPECT_FALSE(reader.has_delete_operations());
    EXPECT_TRUE(reader.close().ok());
}

// ============================================================================
// Test: Hook that modifies block during on_before_read_block
// ============================================================================
class BlockModifyingReader : public MockReader {
protected:
    Status on_before_read_block(Block* block) override {
        call_log.push_back("on_before_read_block");
        // Add an extra column to the block (like Iceberg equality delete expansion).
        auto extra_type = std::make_shared<DataTypeNullable>(std::make_shared<DataTypeInt64>());
        block->insert({extra_type->create_column(), extra_type, "extra_col"});
        return Status::OK();
    }

    Status on_after_read_block(Block* block, size_t* read_rows) override {
        call_log.push_back("on_after_read_block");
        // Remove the extra column (like Iceberg shrink).
        int pos = block->get_position_by_name("extra_col");
        if (pos >= 0) {
            block->erase(static_cast<size_t>(pos));
        }
        return Status::OK();
    }
};

TEST(GenericReaderNVITest, HooksModifyBlockStructure) {
    BlockModifyingReader reader;
    reader.batches_to_emit = 1;
    reader.rows_per_batch = 3;

    Block block = make_block();
    ASSERT_EQ(block.columns(), 1);

    size_t read_rows = 0;
    bool eof = false;
    auto st = reader.get_next_block(&block, &read_rows, &eof);
    ASSERT_TRUE(st.ok()) << st.to_string();
    // After the full cycle, extra_col should have been added then removed.
    EXPECT_EQ(block.columns(), 1);
    EXPECT_EQ(block.get_by_position(0).name, "col0");
}

// ============================================================================
// Test: init_reader + get_next_block full lifecycle
// ============================================================================
TEST(GenericReaderNVITest, FullLifecycle) {
    MockReader reader;
    reader.batches_to_emit = 2;
    reader.rows_per_batch = 50;

    // 1) Init.
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.push_down_agg_type = TPushAggOp::type::NONE;

    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    EXPECT_TRUE(reader.has_column_descs());

    // 2) Read all batches.
    size_t total_rows = 0;
    bool eof = false;
    while (!eof) {
        Block block = make_block();
        size_t read_rows = 0;
        st = reader.get_next_block(&block, &read_rows, &eof);
        ASSERT_TRUE(st.ok());
        total_rows += read_rows;
    }
    EXPECT_EQ(total_rows, 100);

    // 3) Close.
    EXPECT_TRUE(reader.close().ok());

    // 4) Verify full call sequence.
    // Init: 4 calls. Read: 2 batches × 3 hooks = 6 calls. Total: 10.
    EXPECT_EQ(reader.call_log.size(), 10);
}

// ============================================================================
// Test: ReaderInitContext default values
// ============================================================================
TEST(GenericReaderNVITest, ReaderInitContextDefaults) {
    ReaderInitContext ctx;
    EXPECT_EQ(ctx.column_descs, nullptr);
    EXPECT_EQ(ctx.col_name_to_block_idx, nullptr);
    EXPECT_EQ(ctx.state, nullptr);
    EXPECT_EQ(ctx.tuple_descriptor, nullptr);
    EXPECT_EQ(ctx.row_descriptor, nullptr);
    EXPECT_EQ(ctx.params, nullptr);
    EXPECT_EQ(ctx.range, nullptr);
    EXPECT_EQ(ctx.push_down_agg_type, TPushAggOp::type::NONE);
    EXPECT_TRUE(ctx.column_names.empty());
    EXPECT_NE(ctx.table_info_node, nullptr); // ConstNode singleton
    EXPECT_TRUE(ctx.column_ids.empty());
    EXPECT_TRUE(ctx.filter_column_ids.empty());
}

// ============================================================================
// Test: Multiple init_reader calls (re-init scenario)
// ============================================================================
TEST(GenericReaderNVITest, MultipleInitReaderCalls) {
    MockReader reader;
    std::vector<ColumnDescriptor> col_descs1 = {
            {"col_a", nullptr, ColumnCategory::REGULAR, nullptr}};
    std::vector<ColumnDescriptor> col_descs2 = {
            {"col_b", nullptr, ColumnCategory::REGULAR, nullptr},
            {"col_c", nullptr, ColumnCategory::PARTITION_KEY, nullptr}};

    ReaderInitContext ctx1;
    ctx1.column_descs = &col_descs1;
    ASSERT_TRUE(reader.init_reader(&ctx1).ok());
    EXPECT_EQ(reader.call_log.size(), 4);

    // Second init (some readers may be re-inited for different file ranges).
    ReaderInitContext ctx2;
    ctx2.column_descs = &col_descs2;
    ASSERT_TRUE(reader.init_reader(&ctx2).ok());
    EXPECT_EQ(reader.call_log.size(), 8);
}

// ============================================================================
// Test: get_columns error propagation
// ============================================================================
class ErrorColumnsReader : public MockReader {
public:
    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override {
        return Status::InternalError("schema unavailable");
    }
};

TEST(GenericReaderNVITest, GetColumnsError) {
    ErrorColumnsReader reader;
    std::unordered_map<std::string, DataTypePtr> result;
    auto st = reader.get_columns(&result);
    ASSERT_FALSE(st.ok());
    EXPECT_TRUE(result.empty());
}

// ============================================================================
// Test: get_columns error is NOT cached (retry after fix)
// ============================================================================
class TransientErrorColumnsReader : public MockReader {
public:
    int call_count = 0;

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override {
        call_count++;
        if (call_count == 1) {
            return Status::InternalError("transient error");
        }
        (*name_to_type)["col0"] = std::make_shared<DataTypeInt64>();
        return Status::OK();
    }
};

TEST(GenericReaderNVITest, GetColumnsErrorNotCached) {
    TransientErrorColumnsReader reader;
    std::unordered_map<std::string, DataTypePtr> result;

    // First call fails.
    auto st = reader.get_columns(&result);
    ASSERT_FALSE(st.ok());
    EXPECT_EQ(reader.call_count, 1);

    // Second call succeeds (error was not cached).
    st = reader.get_columns(&result);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(result.size(), 1);
    EXPECT_EQ(reader.call_count, 2);
}

// ============================================================================
// Test: init_reader with all TPushAggOp types
// ============================================================================
TEST(GenericReaderNVITest, PushDownAggTypeAllValues) {
    MockReader reader;
    ReaderInitContext ctx;
    ctx.column_descs = nullptr;

    ctx.push_down_agg_type = TPushAggOp::type::COUNT;
    ASSERT_TRUE(reader.init_reader(&ctx).ok());
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::COUNT);

    ctx.push_down_agg_type = TPushAggOp::type::MINMAX;
    ASSERT_TRUE(reader.init_reader(&ctx).ok());
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::MINMAX);

    ctx.push_down_agg_type = TPushAggOp::type::NONE;
    ASSERT_TRUE(reader.init_reader(&ctx).ok());
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::NONE);
}

// ============================================================================
// Test: on_after_init_reader can override push_down_agg_type
// ============================================================================
class CountResetReader : public MockReader {
protected:
    Status on_after_init_reader(ReaderInitContext* ctx) override {
        call_log.push_back("on_after_init_reader");
        // Simulate Iceberg resetting COUNT to NONE due to equality deletes.
        this->set_push_down_agg_type(TPushAggOp::type::NONE);
        return Status::OK();
    }
};

TEST(GenericReaderNVITest, AfterInitResetsCountPushdown) {
    CountResetReader reader;
    std::vector<ColumnDescriptor> col_descs = {{"col0", nullptr, ColumnCategory::REGULAR, nullptr}};
    ReaderInitContext ctx;
    ctx.column_descs = &col_descs;
    ctx.push_down_agg_type = TPushAggOp::type::COUNT;

    auto st = reader.init_reader(&ctx);
    ASSERT_TRUE(st.ok());
    // COUNT was set before hooks, but on_after_init_reader reset it to NONE.
    EXPECT_EQ(reader.get_push_down_agg_type(), TPushAggOp::type::NONE);
}

// ============================================================================
// Test: read_by_rows default returns NotSupported
// ============================================================================
TEST(GenericReaderNVITest, ReadByRowsDefault) {
    MockReader reader;
    auto st = reader.read_by_rows({1, 2, 3});
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: init_schema_reader default returns NotSupported
// ============================================================================
TEST(GenericReaderNVITest, InitSchemaReaderDefault) {
    MockReader reader;
    auto st = reader.init_schema_reader();
    ASSERT_FALSE(st.ok());
}

// ============================================================================
// Test: get_parsed_schema default returns NotSupported
// ============================================================================
TEST(GenericReaderNVITest, GetParsedSchemaDefault) {
    MockReader reader;
    std::vector<std::string> col_names;
    std::vector<DataTypePtr> col_types;
    auto st = reader.get_parsed_schema(&col_names, &col_types);
    ASSERT_FALSE(st.ok());
}

} // namespace doris
