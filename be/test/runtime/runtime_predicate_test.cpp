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

#include "runtime/runtime_predicate.h"

#include <gtest/gtest.h>

#include "core/data_type/data_type_factory.hpp"
#include "core/field.h"
#include "exec/pipeline/thrift_builder.h"
#include "runtime/descriptors.h"

namespace doris {
namespace {

constexpr TPlanNodeId SOURCE_NODE_ID = 10;
constexpr TPlanNodeId TARGET_NODE_ID = 20;
constexpr SlotId SLOT_ID = 0;

TTopnFilterDesc create_topn_filter_desc() {
    auto target_expr = TRuntimeFilterDescBuilder::get_default_expr();

    TTopnFilterDesc desc;
    desc.__set_source_node_id(SOURCE_NODE_ID);
    desc.__set_is_asc(true);
    desc.__set_null_first(false);
    desc.__set_target_node_id_to_target_expr({{TARGET_NODE_ID, target_expr}});
    return desc;
}

SlotDescriptor create_int_slot_descriptor() {
    SlotDescriptor slot_desc;
    slot_desc._id = SLOT_ID;
    slot_desc._col_name = "k1";
    slot_desc._type = DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_INT, false);
    return slot_desc;
}

} // namespace

TEST(RuntimePredicateTest, init_target_creates_column_predicate_for_valid_column_id) {
    RuntimePredicate predicate(create_topn_filter_desc());
    predicate.set_detected_source();

    auto slot_desc = create_int_slot_descriptor();
    phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc;
    slot_id_to_slot_desc[SLOT_ID] = &slot_desc;

    ASSERT_TRUE(predicate.init_target(TARGET_NODE_ID, slot_id_to_slot_desc, 0).ok());

    EXPECT_TRUE(predicate.enable());
    EXPECT_EQ("k1", predicate.get_col_name(TARGET_NODE_ID));
    EXPECT_NE(nullptr, predicate.get_predicate(TARGET_NODE_ID));
}

TEST(RuntimePredicateTest, init_target_without_column_predicate_still_enables_runtime_filter) {
    RuntimePredicate predicate(create_topn_filter_desc());
    predicate.set_detected_source();

    phmap::flat_hash_map<int, SlotDescriptor*> slot_id_to_slot_desc;
    ASSERT_TRUE(predicate.init_target(TARGET_NODE_ID, slot_id_to_slot_desc, -1).ok());

    EXPECT_TRUE(predicate.enable());
    EXPECT_EQ(nullptr, predicate.get_predicate(TARGET_NODE_ID));

    auto top_value = Field::create_field<TYPE_INT>(10);
    ASSERT_TRUE(predicate.update(top_value).ok());
    EXPECT_TRUE(predicate.has_value());
    EXPECT_EQ(top_value, predicate.get_value());
}

} // namespace doris
