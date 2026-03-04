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

#include "olap/accept_null_predicate.h"

#include <gtest/gtest.h>

#include <memory>
#include <roaring/roaring.hh>

#include "olap/comparison_predicate.h"
#include "olap/rowset/segment_v2/index_iterator.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "runtime/define_primitive_type.h"
#include "runtime/memory/lru_cache_policy.h"

namespace doris {

class MockLRUCachePolicy : public LRUCachePolicy {
public:
    MockLRUCachePolicy(std::shared_ptr<roaring::Roaring> bitmap)
            : LRUCachePolicy(CachePolicy::CacheType::INVERTEDINDEX_QUERY_CACHE, 1024,
                             LRUCacheType::SIZE, 3600, 1, 0, true, false) {
        _cache_value.bitmap = bitmap;
    }

    void* value(Cache::Handle* handle) override { return &_cache_value; }

    void release(Cache::Handle* handle) override {}

    segment_v2::InvertedIndexQueryCache::CacheValue _cache_value;
};

class MockIndexIterator : public segment_v2::IndexIterator {
public:
    MockIndexIterator(bool has_null, std::shared_ptr<roaring::Roaring> null_bitmap)
            : _has_null_value(has_null), _null_bitmap(std::move(null_bitmap)) {
        if (_null_bitmap) {
            _mock_cache = std::make_unique<MockLRUCachePolicy>(_null_bitmap);
        }
    }

    Result<bool> has_null() override { return _has_null_value; }

    Status read_null_bitmap(segment_v2::InvertedIndexQueryCacheHandle* cache_handle) override {
        if (_mock_cache && _null_bitmap) {
            *cache_handle = segment_v2::InvertedIndexQueryCacheHandle(
                    _mock_cache.get(), reinterpret_cast<Cache::Handle*>(1));
        }
        return Status::OK();
    }

    segment_v2::IndexReaderPtr get_reader(segment_v2::IndexReaderType) const override {
        return nullptr;
    }

    Status read_from_index(const segment_v2::IndexParam&) override { return Status::OK(); }

private:
    bool _has_null_value;
    std::shared_ptr<roaring::Roaring> _null_bitmap;
    std::unique_ptr<MockLRUCachePolicy> _mock_cache;
};

class MockNestedPredicate : public ColumnPredicate {
public:
    MockNestedPredicate(uint32_t column_id, std::shared_ptr<roaring::Roaring> result_bitmap)
            : ColumnPredicate(column_id, "mock_col", PrimitiveType::TYPE_INT, false),
              _result_bitmap(std::move(result_bitmap)) {}

    PredicateType type() const override { return PredicateType::EQ; }

    Status evaluate(const vectorized::IndexFieldNameAndTypePair& name_with_type,
                    segment_v2::IndexIterator* iterator, uint32_t num_rows,
                    roaring::Roaring* bitmap) const override {
        *bitmap = *_result_bitmap;
        return Status::OK();
    }

    std::shared_ptr<ColumnPredicate> clone(uint32_t col_id) const override {
        return std::make_shared<MockNestedPredicate>(col_id, _result_bitmap);
    }

private:
    uint16_t _evaluate_inner(const vectorized::IColumn& column, uint16_t* sel,
                             uint16_t size) const override {
        return size;
    }

    std::shared_ptr<roaring::Roaring> _result_bitmap;
};

class AcceptNullPredicateTest : public testing::Test {
public:
    AcceptNullPredicateTest() = default;
    ~AcceptNullPredicateTest() = default;
};

TEST_F(AcceptNullPredicateTest, EvaluateWithNullIterator) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    nested_result->add(2);
    nested_result->add(5);
    nested_result->add(8);

    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    roaring::Roaring bitmap;
    vectorized::IndexFieldNameAndTypePair name_with_type = {"test_col", nullptr};

    Status status = predicate.evaluate(name_with_type, nullptr, 10, &bitmap);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bitmap.cardinality(), 3);
    EXPECT_TRUE(bitmap.contains(2));
    EXPECT_TRUE(bitmap.contains(5));
    EXPECT_TRUE(bitmap.contains(8));
}

TEST_F(AcceptNullPredicateTest, EvaluateWithNoNull) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    nested_result->add(1);
    nested_result->add(3);
    nested_result->add(7);

    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    null_bitmap->add(0);
    MockIndexIterator iterator(false, null_bitmap);

    roaring::Roaring bitmap;
    vectorized::IndexFieldNameAndTypePair name_with_type = {"test_col", nullptr};

    Status status = predicate.evaluate(name_with_type, &iterator, 10, &bitmap);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bitmap.cardinality(), 3);
    EXPECT_TRUE(bitmap.contains(1));
    EXPECT_TRUE(bitmap.contains(3));
    EXPECT_TRUE(bitmap.contains(7));
    EXPECT_FALSE(bitmap.contains(0));
}

TEST_F(AcceptNullPredicateTest, EvaluateWithNullRows) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    nested_result->add(2);
    nested_result->add(5);
    nested_result->add(8);

    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    null_bitmap->add(0);
    null_bitmap->add(3);
    null_bitmap->add(9);
    MockIndexIterator iterator(true, null_bitmap);

    roaring::Roaring bitmap;
    bitmap.addRange(0, 10);
    vectorized::IndexFieldNameAndTypePair name_with_type = {"test_col", nullptr};

    Status status = predicate.evaluate(name_with_type, &iterator, 10, &bitmap);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bitmap.cardinality(), 6);
    EXPECT_TRUE(bitmap.contains(0));
    EXPECT_TRUE(bitmap.contains(2));
    EXPECT_TRUE(bitmap.contains(3));
    EXPECT_TRUE(bitmap.contains(5));
    EXPECT_TRUE(bitmap.contains(8));
    EXPECT_TRUE(bitmap.contains(9));
}

TEST_F(AcceptNullPredicateTest, DebugString) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    std::string debug_str = predicate.debug_string();
    EXPECT_TRUE(debug_str.find("AcceptNullPredicate") != std::string::npos);
}

TEST_F(AcceptNullPredicateTest, TypeDelegation) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    EXPECT_EQ(predicate.type(), PredicateType::EQ);
}

TEST_F(AcceptNullPredicateTest, Clone) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    auto cloned = predicate.clone(1);
    EXPECT_NE(cloned, nullptr);
    EXPECT_EQ(cloned->column_id(), 1);
}

TEST_F(AcceptNullPredicateTest, EvaluateWithPreFilteredBitmap) {
    auto nested_result = std::make_shared<roaring::Roaring>();
    nested_result->add(0);
    nested_result->add(1);
    nested_result->add(5);

    auto nested_pred = std::make_shared<MockNestedPredicate>(0, nested_result);
    AcceptNullPredicate predicate(nested_pred);

    auto null_bitmap = std::make_shared<roaring::Roaring>();
    null_bitmap->add(2);
    null_bitmap->add(3);
    null_bitmap->add(7);
    MockIndexIterator iterator(true, null_bitmap);

    roaring::Roaring bitmap;
    bitmap.add(0);
    bitmap.add(1);
    bitmap.add(2);
    bitmap.add(5);
    bitmap.add(6);

    vectorized::IndexFieldNameAndTypePair name_with_type = {"test_col", nullptr};

    Status status = predicate.evaluate(name_with_type, &iterator, 10, &bitmap);

    EXPECT_TRUE(status.ok());
    EXPECT_EQ(bitmap.cardinality(), 4);
    EXPECT_TRUE(bitmap.contains(0));
    EXPECT_TRUE(bitmap.contains(1));
    EXPECT_TRUE(bitmap.contains(2));
    EXPECT_TRUE(bitmap.contains(5));
    EXPECT_FALSE(bitmap.contains(3));
    EXPECT_FALSE(bitmap.contains(6));
    EXPECT_FALSE(bitmap.contains(7));
}

} // namespace doris
