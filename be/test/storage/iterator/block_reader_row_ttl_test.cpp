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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include "cloud/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vcollect_iterator.h"
#include "storage/iterator/vertical_block_reader.h"
#include "storage/iterator/vertical_merge_iterator.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"
#include "util/defer_op.h"

namespace doris {
namespace {

TabletSchemaSPtr make_row_ttl_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);

    auto* key = schema_pb.add_column();
    key->set_unique_id(0);
    key->set_name("k");
    key->set_type("INT");
    key->set_is_key(true);
    key->set_is_nullable(false);

    auto* ttl = schema_pb.add_column();
    ttl->set_unique_id(1);
    ttl->set_name(TTL_COL);
    ttl->set_type("BIGINT");
    ttl->set_is_key(false);
    ttl->set_is_nullable(true);
    ttl->set_aggregation("NONE");
    ttl->set_default_value("NULL");
    schema_pb.set_ttl_col_idx(1);

    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

class FakeRowTtlIterator final : public VCollectIterator::LevelIterator {
public:
    FakeRowTtlIterator(TabletReader* reader, bool fail) : LevelIterator(reader), _fail(fail) {}

    Status init(bool /*get_data_by_ref*/) override { return Status::OK(); }

    int64_t version() const override { return 0; }

    Status next(IteratorRowRef* /*ref*/) override {
        return Status::Error<ErrorCode::END_OF_FILE>("");
    }

    Status next(Block* block) override {
        if (_fail) {
            return Status::InternalError("injected row ttl read failure");
        }
        if (_emitted) {
            return Status::Error<ErrorCode::END_OF_FILE>("");
        }

        auto keys = ColumnInt32::create();
        keys->get_data().assign({1, 2, 3});
        block->replace_by_position(0, std::move(keys));

        auto ttl_values = ColumnInt64::create();
        ttl_values->get_data().assign({100, 101, 0});
        auto null_map = ColumnUInt8::create();
        null_map->get_data().assign({0, 0, 1});
        block->replace_by_position(
                1, ColumnNullable::create(std::move(ttl_values), std::move(null_map)));
        const auto rowid_ordinal = block->get_position_by_name(BeConsts::COMPACTION_ROWID_COL);
        if (rowid_ordinal >= 0) {
            auto rowids = ColumnString::create();
            // Physical segment ids need not match their positions in a cloud rowset.
            for (uint32_t row = 0; row < 3; ++row) {
                const RowLocation location(17, row);
                rowids->insert_data(reinterpret_cast<const char*>(&location), sizeof(location));
            }
            block->replace_by_position(rowid_ordinal, std::move(rowids));
        }
        _emitted = true;
        return Status::OK();
    }

    Status ensure_first_row_ref() override { return Status::OK(); }

    void update_profile(RuntimeProfile* /*profile*/) override {}

private:
    bool _fail = false;
    bool _emitted = false;
};

// Model a key batch with many merged inputs and only three surviving keys. A single
// append is larger than the previous buffer even when that buffer has just spilled.
class SpillingRowTtlIterator final : public RowwiseIterator {
public:
    SpillingRowTtlIterator(const ReadSchema& schema, RowSourcesBuffer* buffer)
            : _schema(schema), _buffer(buffer) {}

    const ReadSchema& schema() const override { return _schema; }
    uint64_t merged_rows() const override { return _merged_rows; }

    Status next_batch(Block* block) override {
        constexpr size_t batch_size = 600000;
        std::vector<RowSource> sources(batch_size, RowSource(0, true));
        sources[0].set_agg_flag(false);
        sources[batch_size / 2].set_agg_flag(false);
        sources[batch_size - 1].set_agg_flag(false);
        RETURN_IF_ERROR(_buffer->append(sources));
        _merged_rows += batch_size - 3;

        auto keys = ColumnInt32::create();
        keys->get_data().assign({1, 2, 3});
        block->replace_by_position(0, std::move(keys));
        auto ttl_values = ColumnInt64::create();
        ttl_values->get_data().assign({100, 101, 0});
        auto null_map = ColumnUInt8::create();
        null_map->get_data().assign({0, 0, 1});
        block->replace_by_position(
                1, ColumnNullable::create(std::move(ttl_values), std::move(null_map)));
        return Status::OK();
    }

private:
    const ReadSchema& _schema;
    RowSourcesBuffer* _buffer;
    uint64_t _merged_rows = 0;
};

void configure_reader(BlockReader* reader, const TabletSchemaSPtr& schema, bool fail,
                      bool convert_rowids) {
    reader->_tablet_schema = schema;
    reader->_filter_row_ttl = true;
    reader->_row_ttl_now_us = 100;
    reader->_read_schema = std::make_shared<ReadSchema>(schema->columns());
    if (convert_rowids) {
        reader->_read_schema->append_compaction_rowid_column();
    }
    reader->_next_block_func = &BlockReader::_direct_next_block;
    reader->_vcollect_iter._inner_iter = std::make_unique<FakeRowTtlIterator>(reader, fail);
}

void check_surviving_keys(const Block& block) {
    ASSERT_EQ(block.rows(), 2);
    const auto& keys = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(keys.get_data(), (ColumnInt32::Container {2, 3}));
}

void check_surviving_rowids(const Block& block) {
    const auto rowid_ordinal = block.get_position_by_name(BeConsts::COMPACTION_ROWID_COL);
    const auto& rowids =
            assert_cast<const ColumnString&>(*block.get_by_position(rowid_ordinal).column);
    ASSERT_EQ(rowids.size(), 2);
    for (size_t row = 0; row < rowids.size(); ++row) {
        auto encoded = rowids.get_data_at(row);
        ASSERT_EQ(encoded.size, sizeof(RowLocation));
        RowLocation location;
        std::memcpy(&location, encoded.data, sizeof(location));
        EXPECT_EQ(location, RowLocation(17, row + 1));
    }
}

void check_replayed_source_rows(RowSourcesBuffer& buffer) {
    ASSERT_TRUE(buffer.flush().ok());
    ASSERT_TRUE(buffer.seek_to_begin().ok());
    size_t replayed_rows = 0;
    Status status;
    while ((status = buffer.has_remaining()).ok()) {
        replayed_rows += !buffer.current().agg_flag();
        buffer.advance();
    }
    EXPECT_TRUE(status.is<ErrorCode::END_OF_FILE>());
    EXPECT_EQ(replayed_rows, 5); // Three previous rows and two surviving TTL rows.
}

void check_vertical_spilled_batch(bool unique_keys) {
    // The temporary spill file is unlinked by RowSourcesBuffer immediately after creation.
    RowSourcesBuffer buffer(1, ".", ReaderType::READER_CUMULATIVE_COMPACTION);
    ASSERT_TRUE(buffer.append(std::vector<RowSource>(3, RowSource(0, false))).ok());
    VerticalBlockReader reader(&buffer);
    reader._tablet_schema = make_row_ttl_schema();
    reader._read_schema = std::make_shared<ReadSchema>(reader._tablet_schema->columns());
    reader._filter_row_ttl = true;
    reader._row_ttl_now_us = 100;
    reader._reader_context.is_key_column_group = true;
    reader._next_block_func = unique_keys ? &VerticalBlockReader::_unique_key_next_block
                                          : &VerticalBlockReader::_direct_next_block;
    reader._vcollect_iter = std::make_shared<SpillingRowTtlIterator>(*reader._read_schema, &buffer);
    Block block = reader._read_schema->create_read_block();
    bool eof = false;
    ASSERT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
    EXPECT_FALSE(eof);
    check_surviving_keys(block);
    EXPECT_EQ(buffer.total_size(), 600003);
    ASSERT_EQ(buffer.buffered_size(), 600000);
    EXPECT_TRUE(buffer.get_agg_flag(0));
    EXPECT_FALSE(buffer.get_agg_flag(300000));
    EXPECT_FALSE(buffer.get_agg_flag(599999));
    EXPECT_EQ(reader._stats.rows_del_filtered, 1);
    check_replayed_source_rows(buffer);
}

} // namespace

class BlockReaderRowTtlTest : public testing::Test {
protected:
    void SetUp() override { _original_cloud_unique_id = config::cloud_unique_id; }

    void TearDown() override { config::cloud_unique_id = _original_cloud_unique_id; }

    std::string _original_cloud_unique_id;
};

TEST_F(BlockReaderRowTtlTest, FiltersRowsAfterMerge) {
    BlockReader reader;
    configure_reader(&reader, make_row_ttl_schema(), false, true);
    Block block = reader._read_schema->create_read_block();
    bool eof = false;
    RowIdConversion rowid_conversion;
    rowid_conversion.enable_visibility_filter_tracking();
    reader._reader_context.rowid_conversion = &rowid_conversion;

    ASSERT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(block.columns(), 3);
    check_surviving_keys(block);
    check_surviving_rowids(block);
    EXPECT_EQ(reader._stats.rows_del_filtered, 1);
    ASSERT_EQ(rowid_conversion.visibility_filtered_rows()->size(), 1);
    EXPECT_TRUE(rowid_conversion.is_visibility_filtered(RowLocation(17, 0)));
    EXPECT_FALSE(rowid_conversion.is_visibility_filtered(RowLocation(17, 1)));
}

TEST_F(BlockReaderRowTtlTest, DoesNotFilterAfterReadFailure) {
    config::cloud_unique_id = "block_reader_row_ttl_test";
    BlockReader reader;
    configure_reader(&reader, make_row_ttl_schema(), true, false);
    Block block = reader._read_schema->create_read_block();
    bool eof = false;

    Status status = reader.next_block_with_aggregation(&block, &eof);
    EXPECT_FALSE(status.ok());
    EXPECT_THAT(status.to_string(), testing::HasSubstr("injected row ttl read failure"));
    EXPECT_EQ(block.columns(), 2);
    EXPECT_EQ(block.rows(), 0);
}

TEST_F(BlockReaderRowTtlTest, VerticalVisibilityMaskSurvivesLargerSpilledBatch) {
    config::cloud_unique_id.clear();
    const auto original_limit = config::vertical_compaction_max_row_source_memory_mb;
    Defer restore_limit(
            [&] { config::vertical_compaction_max_row_source_memory_mb = original_limit; });
    config::vertical_compaction_max_row_source_memory_mb = 1;
    for (bool unique_keys : {false, true}) {
        SCOPED_TRACE(unique_keys);
        check_vertical_spilled_batch(unique_keys);
    }
}

} // namespace doris
