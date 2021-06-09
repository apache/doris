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

#include "olap/aggregate_func.h"

#include <gtest/gtest.h>

#include "common/object_pool.h"
#include "olap/decimal12.h"
#include "olap/uint24.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"

namespace doris {

class AggregateFuncTest : public testing::Test {
public:
    AggregateFuncTest() {}
    virtual ~AggregateFuncTest() {}
};

template <FieldType field_type>
void test_min() {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    static const size_t kValSize = sizeof(CppType) + 1; // '1' represent the leading bool flag.
    char buf[64];

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_MIN, field_type);

    RowCursorCell dst(buf);
    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->init(&dst, val_buf, true, mem_pool.get(), &agg_object_pool);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 200
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 200;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 50
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(50, val);
    }
    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        CppType val;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(50, val);
    }
    agg->finalize(&dst, mem_pool.get());
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(50, val);
}

TEST_F(AggregateFuncTest, min) {
    test_min<OLAP_FIELD_TYPE_INT>();
    test_min<OLAP_FIELD_TYPE_LARGEINT>();
}

template <FieldType field_type>
void test_max() {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    static const size_t kValSize = sizeof(CppType) + 1; // '1' represent the leading bool flag.

    char buf[64];

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_MAX, field_type);

    RowCursorCell dst(buf);
    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->init(&dst, val_buf, true, mem_pool.get(), &agg_object_pool);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 200
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 200;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(200, val);
    }
    // 50
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(200, val);
    }
    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        CppType val;
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(200, val);
    }
    agg->finalize(&dst, mem_pool.get());
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(200, val);
}

TEST_F(AggregateFuncTest, max) {
    test_max<OLAP_FIELD_TYPE_INT>();
    test_max<OLAP_FIELD_TYPE_LARGEINT>();
}

template <FieldType field_type>
void test_sum() {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    static const size_t kValSize = sizeof(CppType) + 1; // '1' represent the leading bool flag.

    char buf[64];
    RowCursorCell dst(buf);

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_SUM, field_type);

    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->init(&dst, val_buf, true, mem_pool.get(), &agg_object_pool);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // 200
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 200;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(300, val);
    }
    // 50
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(350, val);
    }
    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        CppType val;
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(350, val);
    }
    agg->finalize(&dst, mem_pool.get());
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(350, val);
}

TEST_F(AggregateFuncTest, sum) {
    test_sum<OLAP_FIELD_TYPE_INT>();
    test_sum<OLAP_FIELD_TYPE_LARGEINT>();
}

template <FieldType field_type>
void test_replace() {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    static const size_t kValSize = sizeof(CppType) + 1; // '1' represent the leading bool flag.

    char buf[64];
    RowCursorCell dst(buf);

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_REPLACE, field_type);

    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->init(&dst, val_buf, true, mem_pool.get(), &agg_object_pool);
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 100
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 100;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(100, val);
    }
    // null
    {
        char val_buf[kValSize];
        *(bool*)val_buf = true;
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_TRUE(*(bool*)(buf));
    }
    // 50
    {
        char val_buf[kValSize];
        *(bool*)val_buf = false;
        CppType val = 50;
        memcpy(val_buf + 1, &val, sizeof(CppType));
        agg->update(&dst, val_buf, mem_pool.get());
        ASSERT_FALSE(*(bool*)(buf));
        memcpy(&val, buf + 1, sizeof(CppType));
        ASSERT_EQ(50, val);
    }
    agg->finalize(&dst, mem_pool.get());
    ASSERT_FALSE(*(bool*)(buf));
    CppType val;
    memcpy(&val, buf + 1, sizeof(CppType));
    ASSERT_EQ(50, val);
}

template <FieldType field_type>
void test_replace_string() {
    using CppType = typename CppTypeTraits<field_type>::CppType;
    constexpr size_t string_field_size = sizeof(bool) + sizeof(Slice);

    char dst[string_field_size];
    RowCursorCell dst_cell(dst);
    auto dst_slice = reinterpret_cast<Slice*>(dst_cell.mutable_cell_ptr());
    dst_slice->data = nullptr;
    dst_slice->size = 0;

    std::shared_ptr<MemTracker> tracker(new MemTracker(-1));
    std::unique_ptr<MemPool> mem_pool(new MemPool(tracker.get()));
    ObjectPool agg_object_pool;
    const AggregateInfo* agg = get_aggregate_info(OLAP_FIELD_AGGREGATION_REPLACE, field_type);

    char src[string_field_size];
    RowCursorCell src_cell(src);
    auto src_slice = reinterpret_cast<Slice*>(src_cell.mutable_cell_ptr());
    // null
    {
        src_cell.set_null();
        agg->init(&dst_cell, (const char*)src_slice, true, mem_pool.get(), &agg_object_pool);
        ASSERT_TRUE(dst_cell.is_null());
    }
    // "12345"
    {
        src_cell.set_not_null();
        src_slice->data = (char*)"1234567890";
        src_slice->size = 10;
        agg->update(&dst_cell, src_cell, mem_pool.get());
        ASSERT_FALSE(dst_cell.is_null());
        ASSERT_EQ(10, dst_slice->size);
        ASSERT_STREQ("1234567890", dst_slice->to_string().c_str());
    }
    // abc
    {
        src_cell.set_not_null();
        src_slice->data = (char*)"abc";
        src_slice->size = 3;
        agg->update(&dst_cell, src_cell, mem_pool.get());
        ASSERT_FALSE(dst_cell.is_null());
        ASSERT_EQ(3, dst_slice->size);
        ASSERT_STREQ("abc", dst_slice->to_string().c_str());
    }
    // null
    {
        src_cell.set_null();
        agg->update(&dst_cell, src_cell, mem_pool.get());
        ASSERT_TRUE(dst_cell.is_null());
    }
    // "12345"
    {
        src_cell.set_not_null();
        src_slice->data = (char*)"12345";
        src_slice->size = 5;
        agg->update(&dst_cell, src_cell, mem_pool.get());
        ASSERT_FALSE(dst_cell.is_null());
        ASSERT_EQ(5, dst_slice->size);
        ASSERT_STREQ("12345", dst_slice->to_string().c_str());
    }

    agg->finalize(&dst_cell, mem_pool.get());
    ASSERT_FALSE(dst_cell.is_null());
    ASSERT_EQ(5, dst_slice->size);
    ASSERT_STREQ("12345", dst_slice->to_string().c_str());
}

TEST_F(AggregateFuncTest, replace) {
    test_replace<OLAP_FIELD_TYPE_INT>();
    test_replace<OLAP_FIELD_TYPE_LARGEINT>();
    test_replace_string<OLAP_FIELD_TYPE_CHAR>();
    test_replace_string<OLAP_FIELD_TYPE_VARCHAR>();
}

} // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
