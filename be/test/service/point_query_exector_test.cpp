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
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/point_query_executor.h"
#include "storage/tablet/tablet_schema.h"
#include "util/timezone_utils.h"

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

// Regression for timezone-dependent functions on the point-query
// short-circuit path: Reusable::init() must install the request's session
// timezone before opening the output expressions. A constant descendant
// (here the const CAST above least) is evaluated in VExpr::open() with the
// runtime state's timezone; opening under the default +08:00 would cache
// 2024-03-09 19:30 UTC and timezone_hour would return -5, while parsing the
// value in America/New_York yields 2024-03-10 07:30 UTC and must return -4.
TEST(ReusableTimezoneTest, ConstantDescendantUsesRequestTimezone) {
    TimezoneUtils::load_timezones_to_cache();

    auto str_literal = [](const char* value) {
        return create_texpr_node_from(Field::create_field<TYPE_STRING>(std::string(value)),
                                      TYPE_STRING, 0, 0);
    };

    TExprNode least_node;
    least_node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    least_node.__set_type(create_type_desc(TYPE_STRING));
    TFunction least_fn;
    least_fn.name.__set_function_name("least");
    least_fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    least_node.__set_fn(least_fn);
    least_node.__set_num_children(2);

    TExprNode cast_node;
    cast_node.__set_node_type(TExprNodeType::CAST_EXPR);
    cast_node.__set_type(create_type_desc(TYPE_TIMESTAMPTZ, 0, 6));
    cast_node.__set_num_children(1);

    TExprNode fn_node;
    fn_node.__set_node_type(TExprNodeType::FUNCTION_CALL);
    fn_node.__set_type(create_type_desc(TYPE_BIGINT));
    TFunction fn;
    fn.name.__set_function_name("timezone_hour");
    fn.__set_binary_type(TFunctionBinaryType::BUILTIN);
    fn_node.__set_fn(fn);
    fn_node.__set_num_children(1);

    // timezone_hour(CAST(least('2024-03-10 03:30:00','2024-03-11 03:30:00')
    // AS TIMESTAMPTZ))
    TExpr texpr;
    texpr.nodes.push_back(fn_node);
    texpr.nodes.push_back(cast_node);
    texpr.nodes.push_back(least_node);
    texpr.nodes.push_back(str_literal("2024-03-10 03:30:00"));
    texpr.nodes.push_back(str_literal("2024-03-11 03:30:00"));

    auto reusable = std::make_shared<Reusable>();
    TQueryOptions query_options;
    Status st = reusable->init(ReusableTestHelper::create_descriptor_tablet(), {texpr},
                               query_options, *ReusableTestHelper::tablet_schema, 2,
                               "America/New_York");
    ASSERT_TRUE(st.ok()) << st.to_string();

    // The expression tree is constant-only, so execute it over a block whose
    // row count is carried by a dummy column (the pooled Reusable blocks are
    // created empty).
    auto dummy = ColumnUInt8::create();
    dummy->insert_many_defaults(2);
    Block block;
    block.insert({std::move(dummy), std::make_shared<DataTypeUInt8>(), "dummy"});
    int result_column_id = -1;
    st = reusable->output_exprs()[0]->execute(&block, &result_column_id);
    ASSERT_TRUE(st.ok()) << st.to_string();

    auto result_col =
            block.get_by_position(result_column_id).column->convert_to_full_column_if_const();
    // The non-strict CAST above is nullable-typed, so the function result is
    // wrapped in a ColumnNullable; unwrap it before inspecting elements.
    result_col = remove_nullable(result_col);
    const auto& col = assert_cast<const ColumnInt64&>(*result_col);
    ASSERT_GE(col.size(), 1);
    EXPECT_EQ(col.get_element(0), -4);
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

} // namespace doris
