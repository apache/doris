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

#include "exprs/bitmapfilter_predicate.h"

#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <memory>
#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "exprs/create_predicate_function.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/define_primitive_type.h"

namespace doris {
class BitmapFilterPredicateTest : public testing::Test {
public:
    BitmapFilterPredicateTest() = default;
    virtual void SetUp() {}
    virtual void TearDown() {}

    BitmapValue create_bitmap_value() {
        BitmapValue bitmap_value;
        bitmap_value.add(1);
        bitmap_value.add(2);
        bitmap_value.add(4);
        bitmap_value.add(8);
        bitmap_value.add(1024);
        bitmap_value.add(2048);

        return bitmap_value;
    }

    std::vector<int32_t> create_vector() { return {1, 2, 3, 4, 1024, 1025, 2047, 2048}; }

    void find_batch(BitmapFilterFuncBase* func) {
        const std::vector<int32_t> to_find = create_vector();
        std::vector<uint8_t> results(to_find.size());
        func->find_batch(reinterpret_cast<const char*>(to_find.data()), nullptr, to_find.size(),
                         results.data());
        EXPECT_EQ(results[0], 1);
        EXPECT_EQ(results[1], 1);
        EXPECT_EQ(results[2], 0);
        EXPECT_EQ(results[3], 1);
        EXPECT_EQ(results[4], 1);
        EXPECT_EQ(results[5], 0);
        EXPECT_EQ(results[6], 0);
        EXPECT_EQ(results[7], 1);
    }
};

TEST_F(BitmapFilterPredicateTest, insert) {
    std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(PrimitiveType::TYPE_INT));

    EXPECT_TRUE(func->empty());

    BitmapValue bitmap_value = create_bitmap_value();
    func->insert(&bitmap_value);

    find_batch(func.get());
}

TEST_F(BitmapFilterPredicateTest, insert_many) {
    std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(PrimitiveType::TYPE_INT));

    std::vector<const BitmapValue*> bitmaps;

    func->insert_many(bitmaps);
    EXPECT_TRUE(func->empty());

    auto bitmap = create_bitmap_value();
    bitmaps.emplace_back(&bitmap);

    func->insert_many(bitmaps);
    EXPECT_FALSE(func->empty());

    find_batch(func.get());
}

TEST_F(BitmapFilterPredicateTest, assign) {
    std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(PrimitiveType::TYPE_INT));

    BitmapValue bitmap_value = create_bitmap_value();
    EXPECT_EQ(func->assign(&bitmap_value), Status::OK());

    find_batch(func.get());
}

TEST_F(BitmapFilterPredicateTest, contains_any) {
    std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(PrimitiveType::TYPE_INT));

    BitmapValue bitmap_value = create_bitmap_value();
    EXPECT_EQ(func->assign(&bitmap_value), Status::OK());

    auto* filter = assert_cast<BitmapFilterFunc<PrimitiveType::TYPE_INT>*>(func.get());

    EXPECT_TRUE(filter->contains_any(1, 5));
    EXPECT_FALSE(filter->contains_any(5, 7));
    EXPECT_TRUE(filter->contains_any(5, 8));
    EXPECT_FALSE(filter->contains_any(1025, 2047));
    EXPECT_TRUE(filter->contains_any(1025, 2048));

    find_batch(func.get());
}

TEST_F(BitmapFilterPredicateTest, find_fixed_len_olap_engine) {
    std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(PrimitiveType::TYPE_INT));

    BitmapValue bitmap_value = create_bitmap_value();
    EXPECT_EQ(func->assign(&bitmap_value), Status::OK());

    auto* filter = assert_cast<BitmapFilterFunc<PrimitiveType::TYPE_INT>*>(func.get());

    auto data = create_vector();

    std::vector<uint16_t> results {0, 1, 3, 4, 7};
    auto selected = filter->find_fixed_len_olap_engine(reinterpret_cast<const char*>(data.data()),
                                                       nullptr, results.data(), results.size());

    EXPECT_EQ(selected, 5);
    EXPECT_EQ(results[0], 0);
    EXPECT_EQ(results[1], 1);
    EXPECT_EQ(results[2], 3);
    EXPECT_EQ(results[3], 4);
    EXPECT_EQ(results[4], 7);

    std::vector<uint16_t> results2 {1, 2, 4, 5, 7};
    selected = filter->find_fixed_len_olap_engine(reinterpret_cast<const char*>(data.data()),
                                                  nullptr, results2.data(), results2.size());

    EXPECT_EQ(selected, 3);
    EXPECT_EQ(results2[0], 1);
    EXPECT_EQ(results2[1], 4);
    EXPECT_EQ(results2[2], 7);
}

TEST_F(BitmapFilterPredicateTest, light_copy) {
    std::unique_ptr<BitmapFilterFuncBase> func(create_bitmap_filter(PrimitiveType::TYPE_INT));
    std::unique_ptr<BitmapFilterFuncBase> func2(create_bitmap_filter(PrimitiveType::TYPE_INT));

    BitmapValue bitmap_value = create_bitmap_value();
    EXPECT_EQ(func->assign(&bitmap_value), Status::OK());

    auto* filter = assert_cast<BitmapFilterFunc<PrimitiveType::TYPE_INT>*>(func.get());
    auto* filter2 = assert_cast<BitmapFilterFunc<PrimitiveType::TYPE_INT>*>(func2.get());

    filter2->light_copy(filter);

    find_batch(func2.get());
}

} // namespace doris
