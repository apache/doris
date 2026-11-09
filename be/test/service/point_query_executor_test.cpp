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

#include "service/point_query_executor.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "cpp/sync_point.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_context.h"
#include "storage/compaction/cumulative_compaction_policy.h"
#include "storage/data_dir.h"
#include "storage/options.h"
#include "storage/row_cursor.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet.h"
#include "util/countdown_latch.h"

namespace doris {

class PointQuerySnapshotTest : public testing::Test {
protected:
    void SetUp() override {
        // Exercise the cache-miss path, including the real PK index and delete bitmap.
        _saved_disable_row_cache = config::disable_storage_row_cache;
        config::disable_storage_row_cache = true;
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_test_dir).ok());
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        auto engine = std::make_unique<StorageEngine>(options);
        _engine = engine.get();
        ExecEnv::GetInstance()->set_storage_engine(std::move(engine));
        _data_dir = std::make_unique<DataDir>(*_engine, _test_dir);
        ASSERT_TRUE(_data_dir->update_capacity().ok());

        TabletSchemaPB schema_pb;
        schema_pb.set_keys_type(UNIQUE_KEYS);
        schema_pb.set_num_short_key_columns(1);
        schema_pb.set_sequence_col_idx(1);
        for (int i = 0; i < 2; ++i) {
            auto* column = schema_pb.add_column();
            column->set_unique_id(i);
            column->set_name(i == 0 ? "k" : SEQUENCE_COL);
            column->set_type("INT");
            column->set_is_key(i == 0);
            column->set_is_nullable(false);
            column->set_length(4);
            column->set_index_length(4);
            if (i != 0) {
                column->set_aggregation("REPLACE");
            }
        }
        auto meta = std::make_shared<TabletMeta>();
        meta->_schema->init_from_pb(schema_pb);
        meta->_enable_unique_key_merge_on_write = true;
        _tablet = std::make_shared<Tablet>(*_engine, meta, _data_dir.get(),
                                           CUMULATIVE_SIZE_BASED_POLICY);
        ASSERT_TRUE(_tablet->init().ok());
    }

    void TearDown() override {
        auto* sync_point = SyncPoint::get_instance();
        sync_point->disable_processing();
        sync_point->clear_call_back("SegmentLoader::load_segments");
        _tablet.reset();
        _data_dir.reset();
        ExecEnv::GetInstance()->set_storage_engine(nullptr);
        _engine = nullptr;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_test_dir).ok());
        config::disable_storage_row_cache = _saved_disable_row_cache;
    }

    void create_rowset(int64_t version, int32_t sequence, RowsetSharedPtr* rowset) {
        RowsetWriterContext context;
        context.rowset_id = _engine->next_rowset_id();
        context.rowset_state = VISIBLE;
        context.tablet_path = _test_dir;
        context.tablet = _tablet;
        context.tablet_schema = _tablet->tablet_schema();
        context.version = {version, version};
        context.segments_overlap = NONOVERLAPPING;
        context.enable_unique_key_merge_on_write = true;
        context.enable_segcompaction = false;
        auto writer_result = RowsetFactory::create_rowset_writer(*_engine, context, false);
        ASSERT_TRUE(writer_result.has_value()) << writer_result.error();
        auto writer = std::move(writer_result).value();
        auto block = context.tablet_schema->create_storage_block();
        {
            auto guard = block.mutate_columns_scoped();
            auto& columns = guard.mutable_columns();
            int32_t key = 1;
            columns[0]->insert_data(reinterpret_cast<const char*>(&key), sizeof(key));
            columns[1]->insert_data(reinterpret_cast<const char*>(&sequence), sizeof(sequence));
        }
        ASSERT_TRUE(writer->add_block(&block).ok());
        ASSERT_TRUE(writer->flush().ok());
        ASSERT_TRUE(writer->build(*rowset).ok());
        ASSERT_EQ(1, (*rowset)->num_segments());
    }

    void merge_delete_bitmap(const RowsetSharedPtr& deleted_rowset, int64_t version) {
        DeleteBitmap bitmap(_tablet->tablet_id());
        bitmap.add({deleted_rowset->rowset_id(), 0, version}, 0);
        _tablet->tablet_meta()->delete_bitmap().merge(bitmap);
    }

    void expect_lookup(const RowsetSharedPtr& expected_rowset, size_t key_count = 1) {
        RowCursor key_cursor;
        ASSERT_TRUE(
                key_cursor
                        .init_scan_key(_tablet->tablet_schema(), {Field::create_field<TYPE_INT>(1)})
                        .ok());
        std::string encoded_key;
        key_cursor.encode_key_with_padding<true>(&encoded_key, 1, true);

        PointQueryExecutor executor;
        executor._tablet = _tablet;
        executor._row_read_ctxs.resize(key_count);
        for (auto& ctx : executor._row_read_ctxs) {
            ctx._primary_key = encoded_key;
        }
        auto status = executor._lookup_row_key();
        ASSERT_TRUE(status.ok()) << status;
        EXPECT_EQ(0, executor._profile_metrics.row_cache_hits);
        for (auto& ctx : executor._row_read_ctxs) {
            ASSERT_EQ(expected_rowset != nullptr, ctx._row_location.has_value());
            if (expected_rowset == nullptr) {
                continue;
            }
            EXPECT_EQ(expected_rowset->rowset_id(), ctx._row_location->rowset_id);
            ASSERT_NE(nullptr, ctx._rowset_ptr);
            EXPECT_EQ(expected_rowset, *ctx._rowset_ptr);
            EXPECT_NE(nullptr, ctx._segment);
        }
    }

    const std::string _test_dir = "./ut_dir/point_query_snapshot_test";
    bool _saved_disable_row_cache;
    StorageEngine* _engine = nullptr;
    std::unique_ptr<DataDir> _data_dir;
    TabletSharedPtr _tablet;
};

TEST_F(PointQuerySnapshotTest, BitmapMergedBeforeReplacementRowsetIsVisible) {
    RowsetSharedPtr original;
    RowsetSharedPtr replacement;
    ASSERT_NO_FATAL_FAILURE(create_rowset(2, 10, &original));
    ASSERT_NO_FATAL_FAILURE(create_rowset(3, 20, &replacement));
    ASSERT_TRUE(_tablet->add_inc_rowset(original).ok());
    ASSERT_NO_FATAL_FAILURE(expect_lookup(original));

    // Stop publication between merging the version-3 bitmap and add_inc_rowset().
    merge_delete_bitmap(original, 3);
    ASSERT_EQ(2, _tablet->max_version().second);
    ASSERT_NO_FATAL_FAILURE(expect_lookup(original));

    ASSERT_TRUE(_tablet->add_inc_rowset(replacement).ok());
    ASSERT_NO_FATAL_FAILURE(expect_lookup(replacement));
}

TEST_F(PointQuerySnapshotTest, PublicationAfterSnapshotKeepsAllKeysAtCapturedVersion) {
    RowsetSharedPtr original;
    RowsetSharedPtr replacement;
    ASSERT_NO_FATAL_FAILURE(create_rowset(2, 10, &original));
    ASSERT_NO_FATAL_FAILURE(create_rowset(3, 20, &replacement));
    ASSERT_TRUE(_tablet->add_inc_rowset(original).ok());

    // Segment loading happens after the rowset snapshot. Finish publication here
    // deterministically, before the first key's bitmap check. Even though the live
    // tablet is now at version 3, every key in this lookup must use version 2.
    bool published = false;
    auto* sync_point = SyncPoint::get_instance();
    sync_point->set_call_back("SegmentLoader::load_segments", [&](auto&& args) {
        if (published || try_any_cast<BetaRowset*>(args[0]) != original.get()) {
            return;
        }
        published = true;
        merge_delete_bitmap(original, 3);
        ASSERT_TRUE(_tablet->add_inc_rowset(replacement).ok());
    });
    sync_point->enable_processing();
    ASSERT_NO_FATAL_FAILURE(expect_lookup(original, 2));
    ASSERT_TRUE(published);
    ASSERT_EQ(3, _tablet->max_version().second);
    sync_point->disable_processing();
    ASSERT_NO_FATAL_FAILURE(expect_lookup(replacement));
}

TEST_F(PointQuerySnapshotTest, VisibleBitmapStillFiltersLowerSequenceRowset) {
    RowsetSharedPtr original;
    RowsetSharedPtr lower_sequence;
    ASSERT_NO_FATAL_FAILURE(create_rowset(2, 20, &original));
    ASSERT_NO_FATAL_FAILURE(create_rowset(3, 10, &lower_sequence));
    ASSERT_TRUE(_tablet->add_inc_rowset(original).ok());
    merge_delete_bitmap(lower_sequence, 3);
    ASSERT_TRUE(_tablet->add_inc_rowset(lower_sequence).ok());
    ASSERT_NO_FATAL_FAILURE(expect_lookup(original));
}

TEST_F(PointQuerySnapshotTest, EmptySnapshotReturnsNoRow) {
    ASSERT_NO_FATAL_FAILURE(expect_lookup(nullptr));
}

TEST_F(PointQuerySnapshotTest, DeleteBitmapUsesFull64BitVisibleVersion) {
    constexpr int64_t version = (int64_t {1} << 32) + 2;
    RowsetSharedPtr rowset;
    ASSERT_NO_FATAL_FAILURE(create_rowset(version, 10, &rowset));
    ASSERT_TRUE(_tablet->add_inc_rowset(rowset).ok());
    merge_delete_bitmap(rowset, version);
    ASSERT_NO_FATAL_FAILURE(expect_lookup(nullptr));

    // Reproduce the old 32-bit boundary: the same deleted row incorrectly appears visible.
    RowCursor cursor;
    ASSERT_TRUE(cursor.init_scan_key(_tablet->tablet_schema(), {Field::create_field<TYPE_INT>(1)})
                        .ok());
    std::string key;
    cursor.encode_key_with_padding<true>(&key, 1, true);
    auto rowsets = _tablet->get_rowset_by_ids(nullptr);
    std::vector<std::unique_ptr<SegmentCacheHandle>> caches(rowsets.size());
    RowLocation location;
    EXPECT_TRUE(_tablet->lookup_row_key(key, nullptr, false, rowsets, &location,
                                        static_cast<uint32_t>(version), caches, nullptr, false)
                        .ok());
}

class PointQueryReusableTest : public testing::Test {
protected:
    int32_t saved_pool_size = config::lookup_connection_cache_block_pool_size;

    void TearDown() override { config::lookup_connection_cache_block_pool_size = saved_pool_size; }

    void SetUp() override {
        config::lookup_connection_cache_block_pool_size = 32;
        TDescriptorTableBuilder table_builder;
        TTupleDescriptorBuilder tuple_builder;
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).nullable(false).column_name("id").build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(128)
                                       .nullable(false)
                                       .column_name("value")
                                       .build());
        tuple_builder.build(&table_builder);
        ASSERT_TRUE(
                DescriptorTbl::create(&_pool, table_builder.desc_tbl(), &_reusable._desc_tbl).ok());
    }

    static void append_row(Block& block, int32_t id, const std::string& value) {
        auto guard = block.mutate_columns_scoped();
        auto& columns = guard.mutable_columns();
        columns[0]->insert_data(reinterpret_cast<const char*>(&id), sizeof(id));
        columns[1]->insert_data(value.data(), value.size());
    }

    static void expect_empty_block(const Block& block) {
        ASSERT_EQ(2, block.columns());
        EXPECT_EQ(0, block.rows());
        for (size_t i = 0; i < block.columns(); ++i) {
            EXPECT_EQ(0, block.get_by_position(i).column->size());
        }
    }

    ObjectPool _pool;
    Reusable _reusable;
};

TEST_F(PointQueryReusableTest, EmptyPoolAllocatesBlock) {
    auto block = _reusable.get_block();
    ASSERT_NE(nullptr, block);
    expect_empty_block(*block);
    EXPECT_TRUE(_reusable._block_pool.empty());
}

TEST_F(PointQueryReusableTest, ReturnClearsDataAndTransfersOwnership) {
    auto block = _reusable.get_block();
    auto* original = block.get();
    append_row(*block, 1, "first row");
    append_row(*block, 2, "second row");
    ASSERT_EQ(2, block->rows());

    _reusable.return_block(block);
    EXPECT_EQ(nullptr, block);
    ASSERT_EQ(1, _reusable._block_pool.size());

    auto reused = _reusable.get_block();
    ASSERT_EQ(original, reused.get());
    expect_empty_block(*reused);
    append_row(*reused, 3, "replacement row");
    EXPECT_EQ(1, reused->rows());
    EXPECT_EQ("replacement row", reused->get_by_position(1).column->get_data_at(0).to_string());
}

TEST_F(PointQueryReusableTest, ReturnNullDoesNotChangePool) {
    std::unique_ptr<Block> null_block;
    _reusable.return_block(null_block);
    EXPECT_TRUE(_reusable._block_pool.empty());

    auto block = _reusable.get_block();
    _reusable.return_block(block);
    _reusable.return_block(null_block);
    EXPECT_EQ(1, _reusable._block_pool.size());
    EXPECT_EQ(nullptr, null_block);
}

TEST_F(PointQueryReusableTest, ExhaustedPoolKeepsCheckedOutBlocksIndependent) {
    auto first = _reusable.get_block();
    _reusable.return_block(first);
    first = _reusable.get_block();
    append_row(*first, 1, "checked out");

    auto second = _reusable.get_block();
    ASSERT_NE(first.get(), second.get());
    expect_empty_block(*second);
    append_row(*second, 2, "new block");
    EXPECT_EQ("checked out", first->get_by_position(1).column->get_data_at(0).to_string());

    _reusable.return_block(second);
    EXPECT_EQ(nullptr, second);
    EXPECT_EQ(1, first->rows());
    _reusable.return_block(first);
    EXPECT_EQ(nullptr, first);
    EXPECT_EQ(2, _reusable._block_pool.size());
}

TEST_F(PointQueryReusableTest, PoolLimitDiscardsOverflowAndPreservesCachedBlocks) {
    constexpr size_t pool_capacity = 32;
    std::vector<std::unique_ptr<Block>> blocks;
    std::unordered_set<Block*> retained;
    for (size_t i = 0; i < pool_capacity + 8; ++i) {
        blocks.push_back(_reusable.get_block());
        append_row(*blocks.back(), i, "row");
        if (i < pool_capacity) {
            retained.insert(blocks.back().get());
        }
    }
    for (auto& block : blocks) {
        _reusable.return_block(block);
        EXPECT_EQ(nullptr, block);
        EXPECT_LE(_reusable._block_pool.size(), pool_capacity);
    }
    ASSERT_EQ(pool_capacity, _reusable._block_pool.size());
    for (size_t i = 0; i < pool_capacity; ++i) {
        blocks[i] = _reusable.get_block();
        EXPECT_EQ(1, retained.erase(blocks[i].get()));
        expect_empty_block(*blocks[i]);
    }
    EXPECT_TRUE(retained.empty());
    EXPECT_TRUE(_reusable._block_pool.empty());
}

TEST_F(PointQueryReusableTest, RuntimeCapacityReduction) {
    std::vector<std::unique_ptr<Block>> blocks;
    for (int i = 0; i < 4; ++i) {
        blocks.push_back(_reusable.get_block());
    }
    for (auto& block : blocks) {
        _reusable.return_block(block);
    }
    ASSERT_EQ(4, _reusable._block_pool.size());
    auto block = _reusable.get_block();
    config::lookup_connection_cache_block_pool_size = 1;
    _reusable.return_block(block);
    EXPECT_EQ(nullptr, block);
    EXPECT_EQ(1, _reusable._block_pool.size());
}

TEST_F(PointQueryReusableTest, ConcurrentCheckoutsHaveExclusiveOwnership) {
    constexpr size_t worker_count = 40;
    CountDownLatch checked_out(worker_count);
    CountDownLatch return_blocks(1);
    std::vector<Block*> borrowed(worker_count);
    std::vector<std::thread> workers;
    for (size_t i = 0; i < worker_count; ++i) {
        workers.emplace_back([&, i]() {
            ScopedInitThreadContext thread_context;
            auto block = _reusable.get_block();
            borrowed[i] = block.get();
            append_row(*block, i, "worker row");
            checked_out.count_down();
            return_blocks.wait();
            _reusable.return_block(block);
            EXPECT_EQ(nullptr, block);
        });
    }

    checked_out.wait();
    std::unordered_set<Block*> distinct;
    for (auto* block : borrowed) {
        EXPECT_NE(nullptr, block);
        EXPECT_TRUE(distinct.insert(block).second);
        EXPECT_EQ(1, block->rows());
    }
    return_blocks.count_down();
    for (auto& worker : workers) {
        worker.join();
    }

    ASSERT_EQ(32, _reusable._block_pool.size());
    distinct.clear();
    for (const auto& block : _reusable._block_pool) {
        EXPECT_TRUE(distinct.insert(block.get()).second);
        expect_empty_block(*block);
    }
}

} // namespace doris
