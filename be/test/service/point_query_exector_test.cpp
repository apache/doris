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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "core/block/block.h"
#include "exprs/vexpr.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/cache/remote_scan_cache_write_limiter.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/point_query_executor.h"
#include "storage/mow/mow_transform_test_base.h"
#include "storage/segment/segment.h"
#include "storage/segment/segment_loader.h"
#include "storage/tablet/tablet_schema.h"
#include "storage/tablet/tablet_schema_helper.h"
#include "util/defer_op.h"

namespace doris {

class PointQueryCommitTsoTest : public MowTransformTestBase {
protected:
    TabletSchemaSPtr tso_schema(bool partial_row_store = false, bool row_store = true) {
        auto schema = row_store ? create_row_store_schema() : create_mow_schema(false);
        TabletSchemaPB pb;
        schema->to_schema_pb(&pb);
        pb.set_delete_sign_idx(2);
        if (partial_row_store) {
            // Deliberately claim TSO is covered by row store; its logical value must still
            // come from the source rowset. The ordinary value column needs column-store I/O.
            for (int uid : {0, 2, 10}) {
                pb.add_row_store_column_unique_ids(uid);
            }
        }
        schema->init_from_pb(pb);
        schema->append_column(*create_commit_tso_column(10));
        return schema;
    }

    Status init_executor(PointQueryExecutor& executor, const TabletSchemaSPtr& schema,
                         const TabletSharedPtr& tablet, const RowsetSharedPtr& rowset,
                         const std::vector<int>& uids, bool allow_column_store = true) {
        TDescriptorTableBuilder descriptors;
        TTupleDescriptorBuilder tuple;
        std::vector<TExpr> exprs;
        for (size_t pos = 0; pos < uids.size(); ++pos) {
            const auto& column = schema->column_by_uid(uids[pos]);
            auto type = TYPE_INT;
            if (uids[pos] == 10) {
                type = TYPE_BIGINT;
            } else if (uids[pos] == 2) {
                type = TYPE_TINYINT;
            }
            auto slot = TSlotDescriptorBuilder()
                                .type(type)
                                .nullable(column.is_nullable())
                                .column_name(column.name())
                                .column_pos(pos)
                                .build();
            slot.__set_col_unique_id(uids[pos]);
            tuple.add_slot(slot);
            if (uids[pos] == 2) {
                continue; // Delete sign is required for filtering, not an output expression.
            }
            TExprNode node;
            node.__set_node_type(TExprNodeType::SLOT_REF);
            node.__set_type(create_type_desc(type));
            node.__set_is_nullable(column.is_nullable());
            TSlotRef ref;
            ref.__set_slot_id(pos);
            ref.__set_tuple_id(0);
            node.__set_slot_ref(ref);
            TExpr expr;
            expr.__set_nodes({node});
            exprs.push_back(expr);
        }
        tuple.build(&descriptors);
        TQueryOptions options;
        options.__set_enable_short_circuit_query_access_column_store(allow_column_store);
        executor._reusable = std::make_shared<Reusable>();
        RETURN_IF_ERROR(executor._reusable->init(descriptors.desc_tbl(), exprs, options, *schema));
        executor._tablet = tablet;
        executor._result_block = executor._reusable->get_block();
        executor._row_hits = 2;
        executor._row_read_ctxs.resize(2);
        for (uint32_t i = 0; i < 2; ++i) {
            auto& ctx = executor._row_read_ctxs[i];
            ctx._row_location = RowLocation(rowset->rowset_id(), 0, i);
            rowset->acquire();
            ctx._rowset_ptr.reset(new RowsetSharedPtr(rowset));
        }
        return Status::OK();
    }

    void expect_tso(const PointQueryExecutor& executor, int64_t first, int64_t second) {
        const int pos = executor._reusable->commit_tso_idx();
        const auto& result = assert_cast<const ColumnInt64&>(
                *executor._result_block->get_by_position(pos).column);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ(first, result.get_element(0));
        EXPECT_EQ(second, result.get_element(1));
        for (const auto& column : executor._result_block->get_columns()) {
            EXPECT_EQ(2, column->size());
        }
    }

    void expect_values(const PointQueryExecutor& executor) {
        const int pos = executor._reusable->get_col_uid_to_idx().at(1);
        const auto& result = assert_cast<const ColumnNullable&>(
                *executor._result_block->get_by_position(pos).column);
        const auto& values = assert_cast<const ColumnInt32&>(result.get_nested_column());
        ASSERT_EQ(2, values.size());
        EXPECT_EQ(11, values.get_element(0));
        EXPECT_EQ(22, values.get_element(1));
    }

    void check_projection(bool partial_row_store, const std::vector<int>& uids,
                          bool allow_column_store) {
        auto schema = tso_schema(partial_row_store);
        TabletSharedPtr tablet;
        auto rowset =
                write_rowset(schema, 6051, 2, {{.k = 1, .v = 11}, {.k = 2, .v = 22}}, &tablet);
        rowset->make_visible(Version(2, 2), 42);
        Defer erase_segments(
                [&] { SegmentLoader::instance()->erase_segments(*rowset->rowset_meta()); });
        PointQueryExecutor executor;
        ASSERT_TRUE(init_executor(executor, schema, tablet, rowset, uids, allow_column_store).ok());
        EXPECT_FALSE(executor._reusable->include_col_uids().contains(10));
        EXPECT_TRUE(executor._reusable->missing_col_uids().contains(10));
        ASSERT_TRUE(executor._lookup_row_data().ok());
        expect_tso(executor, 42, 42);
        if (executor._reusable->get_col_uid_to_idx().contains(1)) {
            expect_values(executor);
        } else {
            // TSO-only queries need no JSONB/data-page read for a published singleton.
            EXPECT_TRUE(executor._reusable->include_col_uids().empty());
            EXPECT_EQ(0, executor.read_stats().total_pages_num);
        }
    }

    RowsetSharedPtr write_multiversion_rowset(const TabletSchemaSPtr& schema,
                                              TabletSharedPtr* tablet) {
        auto rowset = write_rowset_block(
                schema, 6055, 2,
                [](Block& block) {
                    auto columns_guard = block.mutate_columns_scoped();
                    auto& columns = columns_guard.mutable_columns();
                    for (int32_t i = 1; i <= 2; ++i) {
                        const int32_t value = i * 11;
                        const int64_t tso = i * 100;
                        columns[0]->insert_data(reinterpret_cast<const char*>(&i), sizeof(i));
                        columns[1]->insert_data(reinterpret_cast<const char*>(&value),
                                                sizeof(value));
                        columns[2]->insert_default();
                        columns[3]->insert_default();
                        columns[4]->insert_data(reinterpret_cast<const char*>(&tso), sizeof(tso));
                    }
                },
                tablet);
        rowset->make_visible(Version(2, 3), 200);
        rowset->rowset_meta()->set_commit_tso(TsoRange {100, 200});
        return rowset;
    }

    void expect_remote_read_stats(const PointQueryExecutor& executor) {
        const auto& stats = executor._profile_metrics.read_stats;
        EXPECT_GT(stats.total_pages_num, 0);
        EXPECT_GT(stats.compressed_bytes_read, 0);
        EXPECT_GT(stats.io_ns, 0);
        EXPECT_GT(stats.file_cache_stats.bytes_read_from_remote, 0);
        EXPECT_GT(stats.file_cache_stats.remote_io_timer, 0);
    }

    void expect_cache_policy(const PointQueryExecutor& executor, io::BlockFileCache* cache,
                             const io::UInt128Wrapper& key, bool limited) {
        const auto& stats = executor._profile_metrics.read_stats;
        if (limited) {
            EXPECT_EQ(0, stats.file_cache_stats.bytes_write_into_cache);
            EXPECT_GT(stats.file_cache_stats.num_skip_cache_io_total, 0);
            EXPECT_TRUE(cache->get_blocks_by_key(key).empty());
        } else {
            // Control: the same cold physical read normally admits data into file cache.
            EXPECT_GT(stats.file_cache_stats.bytes_write_into_cache, 0);
            EXPECT_FALSE(cache->get_blocks_by_key(key).empty());
        }
    }

    void check_remote_tso_read(int64_t budget, bool exhaust_budget);
};

TEST_F(PointQueryCommitTsoTest, RowStoreResolvesCommitTsoForEveryRow) {
    check_projection(false, {10}, false);
}

TEST_F(PointQueryCommitTsoTest, FullRowStoreSkipsTsoPlaceholder) {
    check_projection(false, {10, 1, 2}, false);
}

TEST_F(PointQueryCommitTsoTest, FullRowStoreWithTsoAfterValue) {
    check_projection(false, {1, 10, 2}, true);
}

TEST_F(PointQueryCommitTsoTest, PartialRowStoreResolvesTsoAndMissingValue) {
    check_projection(true, {10, 1, 2}, true);
}

TEST_F(PointQueryCommitTsoTest, PartialRowStoreTsoOnlyWithColumnStoreDisabled) {
    check_projection(true, {10}, false);
}

TEST_F(PointQueryCommitTsoTest, DisabledColumnStoreStillRejectsOrdinaryMissingColumns) {
    auto schema = tso_schema(true);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 6052, 2, {{.k = 1, .v = 11}, {.k = 2, .v = 22}}, &tablet);
    rowset->make_visible(Version(2, 2), 42);
    Defer erase_segments(
            [&] { SegmentLoader::instance()->erase_segments(*rowset->rowset_meta()); });
    PointQueryExecutor executor;
    ASSERT_TRUE(init_executor(executor, schema, tablet, rowset, {10, 1, 2}, false).ok());
    const auto status = executor._lookup_row_data();
    EXPECT_FALSE(status.ok());
    EXPECT_NE(std::string::npos, status.to_string().find("missing columns: v,"));
}

TEST_F(PointQueryCommitTsoTest, ColumnStoreOnlyResolvesTsoAndValues) {
    auto schema = tso_schema(false, false);
    TabletSharedPtr tablet;
    auto rowset = write_rowset(schema, 6053, 2, {{.k = 1, .v = 11}, {.k = 2, .v = 22}}, &tablet);
    rowset->make_visible(Version(2, 2), 42);
    Defer erase_segments(
            [&] { SegmentLoader::instance()->erase_segments(*rowset->rowset_meta()); });
    PointQueryExecutor executor;
    ASSERT_TRUE(init_executor(executor, schema, tablet, rowset, {10, 1, 2}).ok());
    EXPECT_TRUE(executor._reusable->include_col_uids().empty());
    ASSERT_TRUE(executor._lookup_row_data().ok());
    expect_tso(executor, 42, 42);
    expect_values(executor);
    EXPECT_GT(executor.read_stats().total_pages_num, 0);
    EXPECT_EQ(&executor.read_stats(), &executor._profile_metrics.read_stats);
}

TEST_F(PointQueryCommitTsoTest, OrdinaryRowStoreQueryStillFiltersDeleteSign) {
    auto schema = tso_schema();
    TabletSharedPtr tablet;
    auto rowset = write_rowset(
            schema, 6054, 2,
            {{.k = 1, .v = 11, .delete_sign = 1}, {.k = 2, .v = 22, .delete_sign = 1}}, &tablet);
    Defer erase_segments(
            [&] { SegmentLoader::instance()->erase_segments(*rowset->rowset_meta()); });
    PointQueryExecutor executor;
    ASSERT_TRUE(init_executor(executor, schema, tablet, rowset, {1, 2}, false).ok());
    EXPECT_TRUE(executor._reusable->missing_col_uids().empty());
    ASSERT_TRUE(executor._lookup_row_data().ok());
    EXPECT_EQ(0, executor._result_block->rows());
}

void PointQueryCommitTsoTest::check_remote_tso_read(int64_t budget, bool exhaust_budget) {
    const bool old_disable_page_cache = config::disable_storage_page_cache;
    config::disable_storage_page_cache = true;
    Defer restore_config([&] { config::disable_storage_page_cache = old_disable_page_cache; });
    io::FileCacheFactory cache_factory;
    auto* old_factory = io::FileCacheFactory::instance();
    ExecEnv::GetInstance()->set_file_cache_factory(&cache_factory);
    Defer restore_factory([&] { ExecEnv::GetInstance()->set_file_cache_factory(old_factory); });
    auto settings = io::get_file_cache_settings(8 * 1024 * 1024, 0, 75, 12, 13, 0, "memory");
    ASSERT_TRUE(cache_factory.create_file_cache("memory", settings).ok());
    // The default UT loader has only 1000 bytes of capacity and would evict this segment,
    // causing the point query to reopen the local file instead of using our remote reader.
    SegmentLoader segment_loader(64 * 1024 * 1024, 1000);
    auto* old_loader = SegmentLoader::instance();
    ExecEnv::GetInstance()->set_segment_loader(&segment_loader);
    Defer restore_loader([&] { ExecEnv::GetInstance()->set_segment_loader(old_loader); });

    auto schema = tso_schema();
    TabletSharedPtr tablet;
    auto rowset = write_multiversion_rowset(schema, &tablet);
    Defer erase_segments(
            [&] { SegmentLoader::instance()->erase_segments(*rowset->rowset_meta()); });
    SegmentCacheHandle segments;
    ASSERT_TRUE(
            SegmentLoader::instance()
                    ->load_segments(std::static_pointer_cast<BetaRowset>(rowset), &segments, true)
                    .ok());
    ASSERT_EQ(1, segments.get_segments().size());
    auto segment = segments.get_segments()[0];
    // Use the real cloud file-cache reader with a local file standing in for object storage,
    // as in BlockFileCacheTest. Footer is loaded, but TSO readers and data pages are still cold.
    io::FileReaderOptions reader_options;
    reader_options.cache_type = io::FileCachePolicy::FILE_BLOCK_CACHE;
    reader_options.is_doris_table = true;
    reader_options.tablet_id = tablet->tablet_id();
    segment->_file_reader =
            std::make_shared<io::CachedRemoteFileReader>(segment->file_reader(), reader_options);
    SegmentCacheHandle cached_segments;
    ASSERT_TRUE(SegmentLoader::instance()
                        ->load_segments(std::static_pointer_cast<BetaRowset>(rowset),
                                        &cached_segments, true)
                        .ok());
    ASSERT_EQ(segment, cached_segments.get_segments()[0]);
    auto key = io::BlockFileCache::hash(segment->file_reader()->path().filename().string());
    auto* cache = cache_factory.get_by_path(key);
    EXPECT_TRUE(cache->get_blocks_by_key(key).empty());

    PointQueryExecutor executor;
    ASSERT_TRUE(init_executor(executor, schema, tablet, rowset, {10}, false).ok());
    if (budget >= 0) {
        executor._remote_scan_cache_write_limiter =
                std::make_unique<io::RemoteScanCacheWriteLimiter>(TUniqueId {}, budget);
    }
    if (exhaust_budget) {
        EXPECT_TRUE(executor._remote_scan_cache_write_limiter->try_admit_cache_write(budget));
        EXPECT_FALSE(executor._remote_scan_cache_write_limiter->try_admit_cache_write(1));
    }
    ASSERT_TRUE(executor._lookup_row_data().ok());
    expect_tso(executor, 100, 200);
    expect_remote_read_stats(executor);
    expect_cache_policy(executor, cache, key, budget >= 0);
}

TEST_F(PointQueryCommitTsoTest, PhysicalTsoHonorsZeroCacheWriteBudget) {
    check_remote_tso_read(0, false);
}

TEST_F(PointQueryCommitTsoTest, PhysicalTsoHonorsExhaustedCacheWriteBudget) {
    check_remote_tso_read(128, true);
}

TEST_F(PointQueryCommitTsoTest, PhysicalTsoPopulatesCacheWithoutLimiter) {
    check_remote_tso_read(-1, false);
}

// Helper class for setting up Reusable objects to test LookupConnectionCache
class ReusableTestHelper {
public:
    static TDescriptorTable create_descriptor_tablet() {
        TDescriptorTableBuilder dtb;
        TTupleDescriptorBuilder tuple_builder;

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("k1")
                                       .column_pos(0)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("k2")
                                       .column_pos(1)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("k3").column_pos(2).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("k4").column_pos(3).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("k5")
                                       .column_pos(4)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_DATE).column_name("k6").column_pos(5).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("k7")
                                       .column_pos(6)
                                       .build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(4).column_name("k8").column_pos(7).build());
        tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(65).column_name("k9").column_pos(8).build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("k10")
                                       .column_pos(9)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("k11")
                                       .column_pos(10)
                                       .build());

        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_TINYINT)
                                       .column_name("v1")
                                       .column_pos(11)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_SMALLINT)
                                       .column_name("v2")
                                       .column_pos(12)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_INT)
                                       .column_name("v3")
                                       .column_pos(13)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_BIGINT)
                                       .column_name("v4")
                                       .column_pos(14)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_LARGEINT)
                                       .column_name("v5")
                                       .column_pos(15)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATE)
                                       .column_name("v6")
                                       .column_pos(16)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATETIME)
                                       .column_name("v7")
                                       .column_pos(17)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(4)
                                       .column_name("v8")
                                       .column_pos(18)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .string_type(65)
                                       .column_name("v9")
                                       .column_pos(19)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .decimal_type(6, 3)
                                       .column_name("v10")
                                       .column_pos(20)
                                       .nullable(false)
                                       .build());
        tuple_builder.add_slot(TSlotDescriptorBuilder()
                                       .type(TYPE_DATEV2)
                                       .column_name("v11")
                                       .column_pos(21)
                                       .nullable(false)
                                       .build());
        tuple_builder.build(&dtb);

        return dtb.desc_tbl();
    }

    static std::shared_ptr<Reusable> create_reusable() {
        auto obj_pool = std::make_unique<ObjectPool>();
        auto runtime_state = RuntimeState::create_unique();
        auto reusable = std::make_shared<Reusable>();

        // Create a simple TupleDescriptor for testing
        TDescriptorTable t_desc_tbl = create_descriptor_tablet();

        // Initialize Reusable
        Status st = reusable->init(t_desc_tbl, output_exprs, query_options, *tablet_schema, 2);
        if (!st.ok()) {
            return nullptr;
        }

        return reusable;
    }

    static std::vector<TExpr> output_exprs;
    static TQueryOptions query_options;
    static std::shared_ptr<TabletSchema> tablet_schema;
};

std::vector<TExpr> ReusableTestHelper::output_exprs = []() {
    std::vector<TExpr> list;
    TExpr expr;
    expr.nodes.emplace_back(TExprNode());
    expr.nodes[0].node_type = TExprNodeType::SLOT_REF;
    auto type = TTypeDesc();
    type.types.emplace_back(TTypeNode());
    type.types[0].type = TTypeNodeType::SCALAR;
    type.types[0].__isset.scalar_type = true;
    type.types[0].scalar_type.type = TPrimitiveType::BIGINT;
    expr.nodes[0].type = type;
    expr.nodes[0].num_children = 0;
    list.push_back(expr);
    return list;
}();

TQueryOptions ReusableTestHelper::query_options = TQueryOptions();
std::shared_ptr<TabletSchema> ReusableTestHelper::tablet_schema = []() {
    auto schema = std::make_shared<TabletSchema>();
    for (int i = 0; i < 11; ++i) {
        schema->append_column(TabletColumn(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                                           FieldType::OLAP_FIELD_TYPE_BIGINT));
    }
    return schema;
}();

// RowCache test class
class RowCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create RowCache instance
        _row_cache = new RowCache(1024, 4); // 1KB cache, 4 shards
    }

    void TearDown() override { delete _row_cache; }

    RowCache* _row_cache = nullptr;
};

// Test basic RowCache functionality
TEST_F(RowCacheTest, PQTestBasicOperations) {
    // Create test data
    const int64_t tablet_id = 12345;
    const std::string key_data = "test_key";
    Slice key_slice(key_data.c_str(), key_data.size());

    const std::string value_data = "test_value";
    Slice value_slice(value_data.c_str(), value_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);

    // Test insertion
    _row_cache->insert(cache_key, value_slice);

    // Test lookup
    RowCache::CacheHandle handle;
    bool found = _row_cache->lookup(cache_key, &handle);
    ASSERT_TRUE(found) << "Cache entry should be found";
    ASSERT_TRUE(handle.valid()) << "Cache handle should be valid";

    // Verify data correctness
    Slice cached_data = handle.data();
    ASSERT_EQ(cached_data.size, value_slice.size) << "Cache data size should match";
    ASSERT_EQ(memcmp(cached_data.data, value_slice.data, value_slice.size), 0)
            << "Cache data content should match";

    // Test deletion
    _row_cache->erase(cache_key);
    RowCache::CacheHandle handle2;
    found = _row_cache->lookup(cache_key, &handle2);
    ASSERT_FALSE(found) << "Cache entry should be deleted";
    ASSERT_FALSE(handle2.valid()) << "Cache handle should be invalid";
}

// Test RowCache LRU mechanism
TEST_F(RowCacheTest, PQTestLRUEviction) {
    // Use small capacity cache to test LRU eviction
    RowCache small_cache(100, 1); // 100 bytes capacity, 1 shard

    // Create enough entries to trigger LRU eviction
    for (int i = 0; i < 10; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        // Create values large enough, about 20 bytes each
        std::string value_data = "value_" + std::to_string(i) + std::string(10, 'x');
        Slice value_slice(value_data.c_str(), value_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        small_cache.insert(cache_key, value_slice);
    }

    // Check if some earlier entries have been evicted
    bool at_least_one_evicted = false;
    for (int i = 0; i < 5; i++) {
        const int64_t tablet_id = 12345;
        std::string key_data = "key_" + std::to_string(i);
        Slice key_slice(key_data.c_str(), key_data.size());

        RowCache::RowCacheKey cache_key(tablet_id, key_slice);
        RowCache::CacheHandle handle;
        if (!small_cache.lookup(cache_key, &handle)) {
            at_least_one_evicted = true;
            break;
        }
    }

    ASSERT_TRUE(at_least_one_evicted) << "At least one cache entry should be evicted";

    // Ensure the most recently accessed entry is still in cache
    const int64_t tablet_id = 12345;
    std::string key_data = "key_9"; // Last inserted key
    Slice key_slice(key_data.c_str(), key_data.size());

    RowCache::RowCacheKey cache_key(tablet_id, key_slice);
    RowCache::CacheHandle handle;
    bool found = small_cache.lookup(cache_key, &handle);
    //evicted
    ASSERT_TRUE(!found);
}

// LookupConnectionCache test class
class LookupConnectionCacheTest : public testing::Test {
protected:
    void SetUp() override {
        // Create LookupConnectionCache instance
        _lookup_cache = new LookupConnectionCache(1024); // 1KB cache
    }

    void TearDown() override { delete _lookup_cache; }

    LookupConnectionCache* _lookup_cache = nullptr;
};

TEST_F(LookupConnectionCacheTest, PQTestLRUEvictionPolicy) {
    LookupConnectionCache cache(40);

    for (int i = 1; i <= 45; ++i) {
        auto reusable = std::make_shared<Reusable>();
        reusable->_block_pool.resize(10);
        cache.add(i, reusable);
    }
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
    auto entry2 = cache.get(10);

    auto reusable5 = std::make_shared<Reusable>();
    reusable5->_block_pool.resize(10);
    cache.add(41, reusable5);
    cache.add(42, reusable5);
    cache.add(43, reusable5);
    EXPECT_LT(cache.get_element_count(), 45) << "capacity " << cache.get_capacity();
}

// Test cache capacity boundary and LRU eviction policy
TEST_F(LookupConnectionCacheTest, PQTestCapacityBoundary) {
    LookupConnectionCache cache(40);
    const int num_entries = 45;

    // Insert 15 entries to exceed capacity
    for (int i = 0; i < num_entries; ++i) {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(i, reusable);
    }

    // Verify at least 5 entries are evicted
    int found_count = 0;
    for (int i = 0; i < num_entries; ++i) {
        if (cache.get(i) != nullptr) found_count++;
    }
    EXPECT_LT(found_count, 45) << "LRU eviction should maintain capacity limit";
}

// Test thread safety with concurrent operations
TEST_F(LookupConnectionCacheTest, PQTestConcurrentAccess) {
    const int num_threads = 4;
    const int num_ops_per_thread = 1000;
    LookupConnectionCache cache(1024 * 1024 * 100); // 100MB buffer

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&, i]() {
            // Mixed operations: 50% insert, 50% query
            for (int j = 0; j < num_ops_per_thread; ++j) {
                int64_t key = i * 1000 + j;
                auto reusable = ReusableTestHelper::create_reusable();

                if (j % 2 == 0) {
                    cache.add(key, reusable); // Insert
                } else {
                    auto entry = cache.get(key); // Query
                    EXPECT_TRUE(entry == nullptr || entry.use_count() == 2);
                }
            }
        });
    }

    for (auto& t : threads) t.join();
}

// Test exceptional input handling
TEST_F(LookupConnectionCacheTest, PQTestInvalidKeys) {
    LookupConnectionCache cache(1024 * 1024);

    // Null value test
    cache.add(0, nullptr);

    // Oversized entry test (1MB limit)
    auto large_reusable = std::make_shared<Reusable>();
    large_reusable->_block_pool.resize(1024 * 1024); // 1MB block
    cache.add(1, large_reusable);
}

// Test key collision handling
TEST_F(LookupConnectionCacheTest, PQTestDuplicateAdd) {
    LookupConnectionCache cache(1024 * 1024);
    auto reusable1 = ReusableTestHelper::create_reusable();
    auto reusable2 = ReusableTestHelper::create_reusable();

    // Overwrite existing key
    cache.add(123, reusable1);
    cache.add(123, reusable2);

    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.get(), reusable2.get()) << "Last write should win in key collision";
}

// Test reference counting mechanism
TEST_F(LookupConnectionCacheTest, PQTestEntryLifetime) {
    LookupConnectionCache cache(1024 * 1024);
    {
        auto reusable = ReusableTestHelper::create_reusable();
        cache.add(123, reusable);
        auto entry = cache.get(123);
        ASSERT_NE(entry, nullptr);
        EXPECT_EQ(entry.use_count(), 3); // Cache + local reference
    } // Local reference released

    // Verify cache maintains ownership
    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.use_count(), 2) << "Cache should maintain sole ownership after scope exit";
}

} // namespace doris
