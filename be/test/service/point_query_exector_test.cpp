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

#include <atomic>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "common/object_pool.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/field.h"
#include "cpp/sync_point.h"
#include "exprs/vexpr.h"
#include "io/fs/local_file_system.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/memory/cache_manager.h"
#include "runtime/runtime_state.h"
#include "service/point_query_executor.h"
#include "storage/options.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/beta_rowset_writer.h"
#include "storage/segment/segment_loader.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#include "util/thrift_util.h"

namespace doris {

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
    }                                    // Local reference released

    // Verify cache maintains ownership
    auto entry = cache.get(123);
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry.use_count(), 2) << "Cache should maintain sole ownership after scope exit";
}

// Exercise the executor's key/data stages with real MoW segments. The parameters cover
// local contiguous IDs and cloud segment-list IDs.
class PointQuerySegmentReuseTest : public testing::TestWithParam<bool> {
protected:
    void SetUp() override {
        _saved_disable_row_cache = config::disable_storage_row_cache;
        _saved_disable_segment_cache = config::disable_segment_cache;
        config::disable_storage_row_cache = true;
        config::disable_segment_cache = false;
        auto* env = ExecEnv::GetInstance();
        _saved_cache_manager = env->get_cache_manager();
        _cache_manager.reset(CacheManager::create_global_instance());
        env->set_cache_manager(_cache_manager.get());
        _saved_row_cache = env->get_row_cache();
        _saved_segment_loader = env->segment_loader();
        _row_cache.reset(new RowCache(1024 * 1024, 1));
        _segment_loader = std::make_unique<SegmentLoader>(64 * 1024 * 1024, 1024);
        env->_row_cache = _row_cache.get();
        env->set_segment_loader(_segment_loader.get());
        EngineOptions options;
        options.backend_uid = UniqueId::gen_uid();
        _engine = std::make_unique<StorageEngine>(options);
        ASSERT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(kTestDir).ok());
    }

    void TearDown() override {
        auto* sync_point = SyncPoint::get_instance();
        sync_point->clear_call_back("SegmentLoader::load_segments");
        sync_point->disable_processing();
        evict_segments();
        _tablet.reset();
        _rowsets.clear();
        _cloud_engine.reset();
        _engine.reset();
        auto* env = ExecEnv::GetInstance();
        env->_row_cache = _saved_row_cache;
        env->set_segment_loader(_saved_segment_loader);
        _row_cache.reset();
        _segment_loader.reset();
        env->set_cache_manager(_saved_cache_manager);
        _cache_manager.reset();
        config::disable_storage_row_cache = _saved_disable_row_cache;
        config::disable_segment_cache = _saved_disable_segment_cache;
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(kTestDir).ok());
    }

    Status create_tablet() {
        TabletMetaPB meta_pb;
        meta_pb.set_tablet_id(987654);
        meta_pb.set_enable_unique_key_merge_on_write(true);
        meta_pb.set_tablet_state(PB_RUNNING);
        auto* schema_pb = meta_pb.mutable_schema();
        schema_pb->set_keys_type(UNIQUE_KEYS);
        schema_pb->set_num_short_key_columns(1);
        schema_pb->set_num_rows_per_row_block(1024);
        schema_pb->set_next_column_unique_id(5);
        schema_pb->set_delete_sign_idx(3);
        schema_pb->set_store_row_column(true);
        auto add_column = [&](int uid, const std::string& name, const std::string& type, int length,
                              bool nullable) {
            auto* column = schema_pb->add_column();
            column->set_unique_id(uid);
            column->set_name(name);
            column->set_type(type);
            column->set_length(length);
            column->set_index_length(type == "STRING" ? 4 : length);
            column->set_is_key(uid == 0);
            column->set_is_nullable(nullable);
            column->set_aggregation("NONE");
        };
        add_column(0, "k", "INT", 4, false);
        add_column(1, "v1", "INT", 4, false);
        add_column(2, "v2", "INT", 4, true);
        add_column(3, DELETE_SIGN, "TINYINT", 1, false);
        add_column(4, BeConsts::ROW_STORE_COL, "STRING", 2147483643, false);
        auto meta = std::make_shared<TabletMeta>();
        meta->init_from_pb(meta_pb);
        if (GetParam()) {
            _cloud_engine = std::make_unique<CloudStorageEngine>(EngineOptions {});
            _tablet = std::make_shared<CloudTablet>(*_cloud_engine, meta);
        } else {
            _tablet = std::make_shared<Tablet>(*_engine, meta, nullptr);
        }
        RETURN_IF_ERROR(write_rowset(1, {{10, 11}, {20}, {30}}));
        RETURN_IF_ERROR(write_rowset(2, {{40}}));
        // Key 20 still has a primary-index entry but is invisible through the delete bitmap.
        const auto& rowset = _rowsets.front();
        meta->delete_bitmap().add({rowset->rowset_id(), rowset->segment(1).id(), 1}, 0);
        auto* sync_point = SyncPoint::get_instance();
        sync_point->set_call_back("SegmentLoader::load_segments", [&](auto&& args) {
            if (try_any_cast<BetaRowset*>(args[0])->rowset_meta()->tablet_id() ==
                _tablet->tablet_id()) {
                _segment_load_calls.fetch_add(1, std::memory_order_relaxed);
            }
        });
        sync_point->enable_processing();
        return Status::OK();
    }

    Status write_rowset(int64_t version, const std::vector<std::vector<int32_t>>& keys) {
        RowsetWriterContext context;
        context.rowset_id = _engine->next_rowset_id();
        context.tablet_id = _tablet->tablet_id();
        context.tablet_schema = _tablet->tablet_schema();
        context.tablet_path = kTestDir;
        context.rowset_state = VISIBLE;
        context.version = {version, version};
        context.enable_unique_key_merge_on_write = true;
        context.enable_segcompaction = false;
        context.write_type = DataWriteType::TYPE_DIRECT;
        BetaRowsetWriter writer(*_engine);
        RETURN_IF_ERROR(writer.init(context));
        for (const auto& segment_keys : keys) {
            Block block = context.tablet_schema->create_storage_block();
            {
                auto guard = block.mutate_columns_scoped();
                auto& columns = guard.mutable_columns();
                for (int32_t key : segment_keys) {
                    columns[0]->insert(Field::create_field<TYPE_INT>(key));
                    columns[1]->insert(Field::create_field<TYPE_INT>(key * 10));
                    if (key == 11) {
                        columns[2]->insert_default();
                    } else {
                        columns[2]->insert(Field::create_field<TYPE_INT>(key * 100));
                    }
                    columns[3]->insert_default();
                    columns[4]->insert_default();
                }
            }
            RETURN_IF_ERROR(writer.flush_single_block(&block));
        }
        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(writer.build(rowset));
        if (GetParam()) {
            // Model a cloud segment-list rowset using local files, without a remote service.
            // Each rowset can use the same physical IDs; its rowset ID distinguishes the files.
            std::vector<int64_t> segment_ids;
            for (auto segment : rowset->segments()) {
                auto path = DORIS_TRY(segment.path());
                int64_t physical_id = 7 + segment.pos() * 13;
                RETURN_IF_ERROR(io::global_local_filesystem()->rename(
                        path, std::string(kTestDir) + "/" + rowset->rowset_id().to_string() + "_" +
                                      std::to_string(physical_id) + ".dat"));
                segment_ids.push_back(physical_id);
            }
            rowset->rowset_meta()->set_segment_ids(segment_ids);
        }
        {
            std::unique_lock lock(_tablet->get_header_lock());
            _tablet->_rs_version_map.emplace(rowset->version(), rowset);
        }
        _rowsets.push_back(std::move(rowset));
        return Status::OK();
    }

    Status prepare_executor(PointQueryExecutor* executor, const std::vector<int32_t>& keys) {
        TDescriptorTableBuilder descriptor_builder;
        TTupleDescriptorBuilder tuple_builder;
        for (int pos = 0; pos < 4; ++pos) {
            const auto& column = _tablet->tablet_schema()->column(pos);
            auto slot = TSlotDescriptorBuilder()
                                .type(pos == 3 ? TYPE_TINYINT : TYPE_INT)
                                .column_name(column.name())
                                .column_pos(pos)
                                .nullable(column.is_nullable())
                                .build();
            slot.__set_col_unique_id(column.unique_id());
            tuple_builder.add_slot(slot);
        }
        tuple_builder.build(&descriptor_builder);
        auto descriptor = descriptor_builder.desc_tbl();
        std::vector<TExpr> output_exprs;
        for (int pos = 0; pos < 3; ++pos) {
            TExprNode node;
            node.__set_node_type(TExprNodeType::SLOT_REF);
            node.__set_type(descriptor.slotDescriptors[pos].slotType);
            node.__set_is_nullable(_tablet->tablet_schema()->column(pos).is_nullable());
            node.__set_num_children(0);
            TSlotRef slot_ref;
            slot_ref.__set_slot_id(descriptor.slotDescriptors[pos].id);
            slot_ref.__set_tuple_id(0);
            node.__set_slot_ref(slot_ref);
            TExpr expr;
            expr.nodes.push_back(node);
            output_exprs.push_back(expr);
        }
        TQueryOptions options;
        executor->_tablet = _tablet;
        executor->_reusable = std::make_shared<Reusable>();
        RETURN_IF_ERROR(executor->_reusable->init(descriptor, output_exprs, options,
                                                  *_tablet->tablet_schema(), 1));
        PTabletKeyLookupRequest request;
        ThriftSerializer serializer(false, 128);
        for (int32_t key : keys) {
            TExprNode literal;
            literal.__set_node_type(TExprNodeType::INT_LITERAL);
            literal.__set_type(descriptor.slotDescriptors[0].slotType);
            literal.__set_is_nullable(false);
            literal.__set_num_children(0);
            TIntLiteral int_literal;
            int_literal.__set_value(key);
            literal.__set_int_literal(int_literal);
            RETURN_IF_ERROR(serializer.serialize(
                    &literal, request.add_key_tuples()->add_key_column_literals()));
        }
        RETURN_IF_ERROR(executor->_init_keys(&request));
        executor->_result_block = executor->_reusable->get_block();
        return Status::OK();
    }

    void evict_segments() {
        for (const auto& rowset : _rowsets) {
            SegmentLoader::instance()->erase_segments(*rowset->rowset_meta());
        }
    }

    static void check_rows(const Block& block) {
        ASSERT_EQ(block.rows(), 4);
        const std::vector<int32_t> expected_keys {30, 10, 40, 11};
        const auto& v2 = assert_cast<const ColumnNullable&>(*block.get_by_position(2).column);
        for (size_t row = 0; row < expected_keys.size(); ++row) {
            EXPECT_EQ(block.get_by_position(0).column->get_int(row), expected_keys[row]);
            EXPECT_EQ(block.get_by_position(1).column->get_int(row), expected_keys[row] * 10);
            EXPECT_EQ(v2.is_null_at(row), expected_keys[row] == 11);
            if (expected_keys[row] != 11) {
                EXPECT_EQ(v2.get_nested_column().get_int(row), expected_keys[row] * 100);
            }
            EXPECT_EQ(block.get_by_position(3).column->get_int(row), 0);
        }
    }

    static constexpr const char* kTestDir = "./point_query_segment_reuse_test";
    std::unique_ptr<StorageEngine> _engine;
    std::unique_ptr<CloudStorageEngine> _cloud_engine;
    BaseTabletSPtr _tablet;
    std::vector<RowsetSharedPtr> _rowsets;
    std::atomic<int> _segment_load_calls = 0;
    std::unique_ptr<RowCache> _row_cache;
    std::unique_ptr<SegmentLoader> _segment_loader;
    std::unique_ptr<CacheManager> _cache_manager;
    CacheManager* _saved_cache_manager = nullptr;
    RowCache* _saved_row_cache = nullptr;
    SegmentLoader* _saved_segment_loader = nullptr;
    bool _saved_disable_row_cache = true;
    bool _saved_disable_segment_cache = false;
};

// NOLINTNEXTLINE(readability-function-cognitive-complexity): gtest assertions expand to branches; keep the key/data lifetime checks together.
TEST_P(PointQuerySegmentReuseTest, ReuseAfterKeyLookupAndCacheEviction) {
    ASSERT_EQ(Status::OK(), create_tablet());
    std::vector<std::weak_ptr<segment_v2::Segment>> retained_segments;
    {
        PointQueryExecutor executor;
        ASSERT_EQ(Status::OK(), prepare_executor(&executor, {30, 10, 40, 11, 99, 20}));
        ASSERT_EQ(Status::OK(), executor._lookup_row_key());
        ASSERT_EQ(executor._row_hits, 4);
        ASSERT_EQ(_segment_load_calls.load(), 2);
        const auto& contexts = executor._row_read_ctxs;
        for (size_t row = 0; row < 4; ++row) {
            const auto& context = contexts[row];
            ASSERT_TRUE(context._row_location.has_value());
            ASSERT_NE(context._segment, nullptr);
            ASSERT_NE(context._rowset_ptr, nullptr);
            EXPECT_EQ(context._segment->id(), context._row_location->segment_id);
            EXPECT_EQ((*context._rowset_ptr)->rowset_id(), context._row_location->rowset_id);
            retained_segments.emplace_back(context._segment);
        }
        EXPECT_EQ(contexts[0]._segment->id(), _rowsets[0]->segment(2).id());
        EXPECT_EQ(contexts[1]._segment.get(), contexts[3]._segment.get());
        EXPECT_NE(contexts[1]._segment.get(), contexts[2]._segment.get());
        EXPECT_EQ(contexts[3]._row_location->row_id, 1);
        for (size_t row : {4, 5}) {
            EXPECT_FALSE(contexts[row]._row_location.has_value());
            EXPECT_EQ(contexts[row]._segment, nullptr);
            EXPECT_EQ(contexts[row]._rowset_ptr, nullptr);
        }
        ASSERT_GT(_segment_loader->_segment_cache->get_element_count(), 0);
        evict_segments();
        ASSERT_EQ(_segment_loader->_segment_cache->get_element_count(), 0);
        for (const auto& segment : retained_segments) {
            EXPECT_FALSE(segment.expired());
        }
        ASSERT_EQ(Status::OK(), executor._lookup_row_data());
        check_rows(*executor._result_block);
        // This fails if the executor switches back to the overload that reloads segments.
        EXPECT_EQ(_segment_load_calls.load(), 2);
        EXPECT_EQ(_rowsets[0]->_refs_by_reader.load(), 3);
        EXPECT_EQ(_rowsets[1]->_refs_by_reader.load(), 1);
    }
    for (const auto& segment : retained_segments) {
        EXPECT_TRUE(segment.expired());
    }
    for (const auto& rowset : _rowsets) {
        EXPECT_EQ(rowset->_refs_by_reader.load(), 0);
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity): compare cold and cached execution using the same request and expected result.
TEST_P(PointQuerySegmentReuseTest, LookupUpAndRowCacheHit) {
    ASSERT_EQ(Status::OK(), create_tablet());
    config::disable_storage_row_cache = false;
    // MySQL text rows: length-prefixed cells, with 0xfb for SQL NULL.
    const std::vector<std::string> expected_rows {"\00230\003300\0043000", "\00210\003100\0041000",
                                                  "\00240\003400\0044000", "\00211\003110\373"};
    for (bool cached : {false, true}) {
        PointQueryExecutor executor;
        PTabletKeyLookupResponse response;
        // Only existing keys: the warm request must not need an index lookup for a missing key.
        ASSERT_EQ(Status::OK(), prepare_executor(&executor, {30, 10, 40, 11}));
        executor._response = &response;
        _segment_load_calls = 0;
        ASSERT_EQ(Status::OK(), executor.lookup_up());
        EXPECT_FALSE(response.empty_batch());
        TResultBatch batch;
        auto length = static_cast<uint32_t>(response.row_batch().size());
        ASSERT_TRUE(deserialize_thrift_msg(
                            reinterpret_cast<const uint8_t*>(response.row_batch().data()), &length,
                            false, &batch)
                            .ok());
        EXPECT_EQ(batch.rows, expected_rows);
        EXPECT_EQ(_segment_load_calls.load(), cached ? 0 : 2);
        EXPECT_EQ(executor._profile_metrics.row_cache_hits, cached ? 4 : 0);
        if (cached) {
            for (const auto& context : executor._row_read_ctxs) {
                EXPECT_EQ(context._segment, nullptr);
                EXPECT_EQ(context._rowset_ptr, nullptr);
            }
        }
        evict_segments();
    }
}

INSTANTIATE_TEST_SUITE_P(SegmentLayouts, PointQuerySegmentReuseTest, testing::Bool());

} // namespace doris
