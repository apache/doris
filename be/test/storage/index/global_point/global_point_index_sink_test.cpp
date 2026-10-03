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

#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "common/config.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/global_point/global_point_index_writer.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/rowset/beta_rowset_writer_v2.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#include "util/hash_util.hpp"

namespace doris {

using segment_v2::BloomFilter;

namespace {

constexpr std::string_view kTestDir = "./ut_dir/global_point_index_sink_test";
constexpr int32_t kColUniqueId = 5;
constexpr int64_t kIndexId = 1001;

TabletSchemaSPtr create_schema_with_global_point() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(DUP_KEYS);
    schema_pb.set_num_short_key_columns(1);
    ColumnPB* key_col = schema_pb.add_column();
    key_col->set_unique_id(1);
    key_col->set_name("id");
    key_col->set_type("BIGINT");
    key_col->set_is_key(true);
    key_col->set_length(8);
    ColumnPB* ev_col = schema_pb.add_column();
    ev_col->set_unique_id(kColUniqueId);
    ev_col->set_name("ev");
    ev_col->set_type("INT");
    ev_col->set_length(4);
    ev_col->set_is_nullable(true);
    TabletIndexPB* index_pb = schema_pb.add_index();
    index_pb->set_index_id(kIndexId);
    index_pb->set_index_name("idx_ev");
    index_pb->set_index_type(IndexType::GLOBAL_POINT);
    index_pb->add_col_unique_id(kColUniqueId);
    (*index_pb->mutable_properties())["fpp"] = "0.01";
    auto schema = std::make_shared<TabletSchema>();
    schema->init_from_pb(schema_pb);
    return schema;
}

std::unique_ptr<BloomFilter> make_bloom(uint64_t bytes = 1024) {
    std::unique_ptr<BloomFilter> bf;
    EXPECT_TRUE(BloomFilter::create(BLOCK_BLOOM_FILTER, &bf).ok());
    EXPECT_TRUE(bf->init(bytes, HASH_MURMUR3_X64_64).ok());
    return bf;
}

PGlobalPointIndexPart make_part(const BloomFilter& bloom, int64_t index_id, int64_t total_rows) {
    PGlobalPointIndexPart part;
    part.set_column_unique_id(kColUniqueId);
    part.set_index_id(index_id);
    part.set_fpp(0.002);
    part.set_hash_strategy(static_cast<int32_t>(HASH_MURMUR3_X64_64));
    part.set_num_bits(static_cast<uint64_t>(bloom.num_bytes()) * 8);
    part.set_body_size(static_cast<int64_t>(bloom.size()));
    part.set_total_rows(total_rows);
    part.set_body_crc32(
            HashUtil::zlib_crc_hash(bloom.data(), static_cast<uint32_t>(bloom.size()), 0));
    return part;
}

std::string_view body_of(const BloomFilter& bloom) {
    return {bloom.data(), bloom.size()};
}

RowsetWriterContext make_context(bool from_sender) {
    static int64_t next_rowset_id = 40000;
    RowsetWriterContext context;
    RowsetId rowset_id;
    rowset_id.init(next_rowset_id++);
    context.rowset_id = rowset_id;
    context.tablet_id = 54321;
    context.partition_id = 10;
    context.rowset_type = BETA_ROWSET;
    context.tablet_path = std::string(kTestDir);
    context.rowset_state = VISIBLE;
    context.tablet_schema = create_schema_with_global_point();
    context.version = Version(10, 10);
    context.point_query_index_from_sender = from_sender;
    return context;
}

} // namespace

// Receiver of a memtable-on-sink-node load: merges the parts of all senders.
class GlobalPointIndexReceiverTest : public testing::Test {
protected:
    void SetUp() override {
        _engine = std::make_unique<StorageEngine>(EngineOptions {});
        _writer = std::make_unique<BetaRowsetWriter>(*_engine);
        ASSERT_TRUE(_writer->init(make_context(true)).ok());
    }

    const auto& acc() { return _writer->received_point_query_indexes().at(kColUniqueId); }

    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<BetaRowsetWriter> _writer;
};

TEST_F(GlobalPointIndexReceiverTest, ReceiverBuildsNoLocalBloom) {
    EXPECT_TRUE(_writer->context().global_point_index_builders.empty());
    BetaRowsetWriter local_writer(*_engine);
    ASSERT_TRUE(local_writer.init(make_context(false)).ok());
    EXPECT_FALSE(local_writer.context().global_point_index_builders.empty());
}

TEST_F(GlobalPointIndexReceiverTest, PartsAreOredTogether) {
    auto a = make_bloom();
    auto b = make_bloom();
    int32_t va = 1;
    int32_t vb = 2;
    a->add_bytes(reinterpret_cast<const char*>(&va), sizeof(va));
    b->add_bytes(reinterpret_cast<const char*>(&vb), sizeof(vb));
    b->set_has_null(true);
    ASSERT_TRUE(_writer->add_point_query_index(make_part(*a, kIndexId, 3), body_of(*a)).ok());
    ASSERT_TRUE(_writer->add_point_query_index(make_part(*b, kIndexId, 4), body_of(*b)).ok());

    EXPECT_FALSE(acc().poisoned);
    EXPECT_EQ(acc().total_rows, 7);
    std::unique_ptr<BloomFilter> merged;
    ASSERT_TRUE(BloomFilter::create(BLOCK_BLOOM_FILTER, &merged).ok());
    ASSERT_TRUE(merged->init(acc().body.data(), acc().body.size(), HASH_MURMUR3_X64_64).ok());
    EXPECT_TRUE(merged->test_bytes(reinterpret_cast<const char*>(&va), sizeof(va)));
    EXPECT_TRUE(merged->test_bytes(reinterpret_cast<const char*>(&vb), sizeof(vb)));
    // The has-null byte is ORed like the bitmap.
    EXPECT_TRUE(merged->has_null());
}

// Every bad part removes the column's index instead of failing the load.
TEST_F(GlobalPointIndexReceiverTest, BadPartsPoisonTheColumn) {
    auto bloom = make_bloom();

    auto bad_crc = make_part(*bloom, kIndexId, 1);
    bad_crc.set_body_crc32(bad_crc.body_crc32() + 1);
    EXPECT_TRUE(_writer->add_point_query_index(bad_crc, body_of(*bloom)).ok());
    EXPECT_TRUE(acc().poisoned);
    // A good part afterwards does not bring it back.
    EXPECT_TRUE(
            _writer->add_point_query_index(make_part(*bloom, kIndexId, 1), body_of(*bloom)).ok());
    EXPECT_TRUE(acc().poisoned);
}

TEST_F(GlobalPointIndexReceiverTest, InconsistentPartsPoisonTheColumn) {
    auto bloom = make_bloom();
    auto bigger = make_bloom(2048);
    for (int i = 0; i < 4; ++i) {
        BetaRowsetWriter writer(*_engine);
        ASSERT_TRUE(writer.init(make_context(true)).ok());
        ASSERT_TRUE(
                writer.add_point_query_index(make_part(*bloom, kIndexId, 1), body_of(*bloom)).ok());
        Status st;
        switch (i) {
        case 0: // attachment shorter than declared
            st = writer.add_point_query_index(make_part(*bloom, kIndexId, 1),
                                              body_of(*bloom).substr(1));
            break;
        case 1: { // num_bits does not match body_size
            auto part = make_part(*bloom, kIndexId, 1);
            part.set_num_bits(part.num_bits() + 8);
            st = writer.add_point_query_index(part, body_of(*bloom));
            break;
        }
        case 2: // senders disagree on the bloom size
            st = writer.add_point_query_index(make_part(*bigger, kIndexId, 1), body_of(*bigger));
            break;
        default: // senders disagree on the index id (index dropped and re-created)
            st = writer.add_point_query_index(make_part(*bloom, kIndexId + 1, 1), body_of(*bloom));
            break;
        }
        EXPECT_TRUE(st.ok()) << "case " << i;
        EXPECT_TRUE(writer.received_point_query_indexes().at(kColUniqueId).poisoned)
                << "case " << i;
    }
}

TEST_F(GlobalPointIndexReceiverTest, DropDiscardsEverything) {
    auto bloom = make_bloom();
    ASSERT_TRUE(
            _writer->add_point_query_index(make_part(*bloom, kIndexId, 1), body_of(*bloom)).ok());
    ASSERT_FALSE(_writer->received_point_query_indexes().empty());
    _writer->drop_point_query_indexes();
    EXPECT_TRUE(_writer->received_point_query_indexes().empty());
    // Parts that arrive after the drop are ignored.
    ASSERT_TRUE(
            _writer->add_point_query_index(make_part(*bloom, kIndexId, 1), body_of(*bloom)).ok());
    EXPECT_TRUE(_writer->received_point_query_indexes().empty());
}

// Sender of a memtable-on-sink-node load: builds the blooms and frees them once sent.
class GlobalPointIndexSenderTest : public testing::Test {
protected:
    void SetUp() override { _saved = config::enable_global_point_index_sink_build; }
    void TearDown() override { config::enable_global_point_index_sink_build = _saved; }
    bool _saved = true;
};

TEST_F(GlobalPointIndexSenderTest, BuildersFollowTheSwitchAndTheSchema) {
    config::enable_global_point_index_sink_build = false;
    {
        BetaRowsetWriterV2 writer({});
        ASSERT_TRUE(writer.init(make_context(false)).ok());
        EXPECT_TRUE(writer.context().global_point_index_builders.empty());
    }
    config::enable_global_point_index_sink_build = true;
    {
        BetaRowsetWriterV2 writer({});
        ASSERT_TRUE(writer.init(make_context(false)).ok());
        EXPECT_TRUE(writer.context().global_point_index_builders.contains(kColUniqueId));
    }
    {
        RowsetWriterContext context = make_context(false);
        context.tablet_schema = std::make_shared<TabletSchema>();
        BetaRowsetWriterV2 writer({});
        ASSERT_TRUE(writer.init(context).ok());
        EXPECT_TRUE(writer.context().global_point_index_builders.empty());
    }
}

TEST_F(GlobalPointIndexSenderTest, SendFreesBuildersAndIsIdempotent) {
    config::enable_global_point_index_sink_build = true;
    BetaRowsetWriterV2 writer({});
    ASSERT_TRUE(writer.init(make_context(false)).ok());
    ASSERT_FALSE(writer.global_point_index_builders().empty());
    ASSERT_TRUE(writer.send_point_query_indexes().ok());
    EXPECT_TRUE(writer.global_point_index_builders().empty());
    EXPECT_TRUE(writer.context().global_point_index_builders.empty());
    ASSERT_TRUE(writer.send_point_query_indexes().ok());
}

} // namespace doris
