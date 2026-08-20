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

#include <gtest/gtest.h>

#include <memory>
#include <vector>

#include "core/assert_cast.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "storage/olap_common.h"
#include "storage/predicate/null_predicate.h"
#include "storage/tablet/tablet_schema.h"

#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wkeyword-macro"
#endif
#include "storage/segment/segment_iterator.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif

namespace doris::segment_v2 {
namespace {

MutableColumnPtr make_nullable_int_column(const std::vector<int32_t>& values,
                                          const std::vector<uint8_t>& null_map) {
    auto nested = ColumnInt32::create();
    auto nulls = ColumnUInt8::create();
    for (auto value : values) {
        nested->insert_value(value);
    }
    for (auto is_null : null_map) {
        nulls->insert_value(is_null);
    }
    return ColumnNullable::create(std::move(nested), std::move(nulls));
}

TabletSchemaSPtr make_nullable_int_schema() {
    TabletSchemaPB schema_pb;
    schema_pb.set_keys_type(KeysType::DUP_KEYS);
    auto* column = schema_pb.add_column();
    column->set_unique_id(0);
    column->set_name("c0");
    column->set_type("INT");
    column->set_is_key(true);
    column->set_is_nullable(true);

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(schema_pb);
    return tablet_schema;
}

SchemaSPtr make_read_schema(const TabletSchemaSPtr& tablet_schema) {
    return std::make_shared<Schema>(tablet_schema->columns(), std::vector<ColumnId> {0});
}

void expect_nullable_int_column(const MutableColumnPtr& column,
                                const std::vector<int32_t>& expected_values,
                                const std::vector<uint8_t>& expected_null_map) {
    const auto& nullable = assert_cast<const ColumnNullable&>(*column);
    const auto& nested = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
    ASSERT_EQ(expected_values.size(), nested.size());
    ASSERT_EQ(expected_null_map.size(), nullable.get_null_map_data().size());
    for (size_t i = 0; i < expected_values.size(); ++i) {
        EXPECT_EQ(expected_values[i], nested.get_data()[i]);
        EXPECT_EQ(expected_null_map[i], nullable.get_null_map_data()[i]);
    }
}

} // namespace

class SegmentIteratorPredicateNullStateTest : public ::testing::Test {
protected:
    void SetUp() override {
        _tablet_schema = make_nullable_int_schema();
        _read_schema = make_read_schema(_tablet_schema);
    }

    std::unique_ptr<SegmentIterator> make_iter() {
        return std::make_unique<SegmentIterator>(nullptr, _read_schema);
    }

    TabletSchemaSPtr _tablet_schema;
    SchemaSPtr _read_schema;
};

TEST_F(SegmentIteratorPredicateNullStateTest, UsesNestedColumnOnlyWhenNoNullsAreKnown) {
    auto iter = make_iter();
    auto input = make_nullable_int_column({10, 20, 30}, {0, 0, 0});
    const auto* input_ptr = input.get();
    const auto* nested_ptr = &assert_cast<const ColumnNullable&>(*input).get_nested_column();
    iter->_current_return_columns.emplace_back(std::move(input));
    iter->_predicate_column_null_states.resize(1);
    auto predicate = NullPredicate::create_shared(0, "c0", true, PrimitiveType::TYPE_INT);

    iter->_predicate_column_null_states[0] = SegmentIterator::PredicateColumnNullState::NO_NULLS;
    EXPECT_EQ(nested_ptr, iter->_get_predicate_column(*predicate));

    iter->_predicate_column_null_states[0] = SegmentIterator::PredicateColumnNullState::HAS_NULLS;
    EXPECT_EQ(input_ptr, iter->_get_predicate_column(*predicate));

    iter->_predicate_column_null_states[0] = SegmentIterator::PredicateColumnNullState::UNKNOWN;
    EXPECT_EQ(input_ptr, iter->_get_predicate_column(*predicate));
}

TEST_F(SegmentIteratorPredicateNullStateTest, CopiesNullableColumnWithoutFilteringNullMap) {
    auto iter = make_iter();
    auto input = make_nullable_int_column({10, 20, 30}, {0, 0, 0});
    MutableColumnPtr output = ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
    uint16_t selector[] = {2, 0};

    auto status =
            iter->copy_column_data_by_selector(input.get(), output, selector, 2, 3,
                                               SegmentIterator::PredicateColumnNullState::NO_NULLS);

    ASSERT_TRUE(status.ok()) << status.to_string();
    ASSERT_TRUE(output->is_nullable());
    EXPECT_FALSE(assert_cast<const ColumnNullable&>(*output).has_null());
    expect_nullable_int_column(output, {30, 10}, {0, 0});
}

TEST_F(SegmentIteratorPredicateNullStateTest, PreservesNullsForKnownAndUnknownStates) {
    for (auto null_state : {SegmentIterator::PredicateColumnNullState::HAS_NULLS,
                            SegmentIterator::PredicateColumnNullState::UNKNOWN}) {
        SCOPED_TRACE(static_cast<int>(null_state));
        auto iter = make_iter();
        auto input = make_nullable_int_column({10, 20, 30}, {0, 1, 0});
        MutableColumnPtr output =
                ColumnNullable::create(ColumnInt32::create(), ColumnUInt8::create());
        uint16_t selector[] = {1, 2};

        auto status =
                iter->copy_column_data_by_selector(input.get(), output, selector, 2, 3, null_state);

        ASSERT_TRUE(status.ok()) << status.to_string();
        EXPECT_TRUE(assert_cast<const ColumnNullable&>(*output).has_null());
        expect_nullable_int_column(output, {20, 30}, {1, 0});
    }
}

} // namespace doris::segment_v2
