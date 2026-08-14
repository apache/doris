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

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#define private public
#define protected public
#include "storage/iterator/block_reader.h"
#include "storage/iterator/vcollect_iterator.h"
#undef private
#undef protected
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "cloud/config.h"
#include "common/consts.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/utils.h"

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
        _emitted = true;
        return Status::OK();
    }

    RowLocation current_row_location() override { return {}; }

    Status current_block_row_locations(std::vector<RowLocation>* locations) override {
        locations->clear();
        locations->emplace_back(0, 0);
        locations->emplace_back(0, 1);
        locations->emplace_back(0, 2);
        return Status::OK();
    }

    Status ensure_first_row_ref() override { return Status::OK(); }

    void update_profile(RuntimeProfile* /*profile*/) override {}

private:
    bool _fail = false;
    bool _emitted = false;
};

Block make_output_block() {
    Block block;
    block.insert({ColumnInt32::create(), std::make_shared<DataTypeInt32>(), "k"});
    return block;
}

void configure_reader(BlockReader* reader, const TabletSchemaSPtr& schema, bool fail,
                      bool record_rowids) {
    reader->_tablet_schema = schema;
    reader->_filter_row_ttl = true;
    reader->_row_ttl_now_us = 100;
    reader->_remove_row_ttl_output = true;
    reader->_row_ttl_output_pos = 1;
    reader->_reader_context.record_rowids = record_rowids;
    reader->_next_block_func = &BlockReader::_direct_next_block;
    reader->_vcollect_iter._inner_iter = std::make_unique<FakeRowTtlIterator>(reader, fail);
}

} // namespace

class BlockReaderRowTtlTest : public testing::Test {
protected:
    void SetUp() override { _original_cloud_unique_id = config::cloud_unique_id; }

    void TearDown() override { config::cloud_unique_id = _original_cloud_unique_id; }

    std::string _original_cloud_unique_id;
};

TEST_F(BlockReaderRowTtlTest, FiltersRowsAndRemovesInternalTtlColumn) {
    BlockReader reader;
    configure_reader(&reader, make_row_ttl_schema(), false, true);
    Block block = make_output_block();
    bool eof = false;

    ASSERT_TRUE(reader.next_block_with_aggregation(&block, &eof).ok());
    EXPECT_FALSE(eof);
    ASSERT_EQ(block.columns(), 1);
    ASSERT_EQ(block.rows(), 2);
    const auto& keys = assert_cast<const ColumnInt32&>(*block.get_by_position(0).column);
    EXPECT_EQ(keys.get_data(), (ColumnInt32::Container {2, 3}));
    ASSERT_EQ(reader._block_row_locations.size(), 3);
    EXPECT_EQ(reader._block_row_locations[0].row_id, std::numeric_limits<uint32_t>::max());
    EXPECT_EQ(reader._block_row_locations[1].row_id, 1);
    EXPECT_EQ(reader._block_row_locations[2].row_id, 2);
    EXPECT_EQ(reader._stats.rows_del_filtered, 1);
}

TEST_F(BlockReaderRowTtlTest, RemovesInternalTtlColumnAfterReadFailure) {
    config::cloud_unique_id = "block_reader_row_ttl_test";
    BlockReader reader;
    configure_reader(&reader, make_row_ttl_schema(), true, false);
    Block block = make_output_block();
    bool eof = false;

    Status status = reader.next_block_with_aggregation(&block, &eof);
    EXPECT_FALSE(status.ok());
    EXPECT_THAT(status.to_string(), testing::HasSubstr("injected row ttl read failure"));
    EXPECT_EQ(block.columns(), 1);
    EXPECT_EQ(block.rows(), 0);
}

} // namespace doris
