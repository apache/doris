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

#include "exprs/vslot_ref.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest.h>

#include <set>
#include <string>
#include <vector>

#include "core/data_type/data_type_number.h"
#include "exprs/vexpr_context.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"

namespace doris {

TEST(VSlotRefDigestTest, PathComponentsAndClones) {
    const auto type = std::make_shared<DataTypeInt64>();
    const std::vector<std::vector<std::string>> paths = {{"a.b"}, {"a", "b"}, {"a%2Eb"}, {},
                                                         {""},    {"a", ""},  {"a"},     {"", "a"}};
    std::set<uint64_t> digests;
    for (const auto& path : paths) {
        TSlotDescriptor thrift_slot;
        thrift_slot.__set_id(0);
        thrift_slot.__set_parent(0);
        thrift_slot.__set_slotType(type->to_thrift());
        thrift_slot.__set_nullIndicatorBit(-1);
        thrift_slot.__set_colName("v.a.b");
        thrift_slot.__set_col_unique_id(7);
        thrift_slot.__set_column_paths(path);
        SlotDescriptor slot(thrift_slot);
        TTupleDescriptor thrift_tuple;
        thrift_tuple.__set_id(0);
        TupleDescriptor tuple(thrift_tuple);
        tuple.add_slot(&slot);
        DescriptorTbl descriptors;
        descriptors._slot_desc_map[0] = &slot;
        RuntimeState state;
        state.set_desc_tbl(&descriptors);
        RowDescriptor row(&tuple);

        auto ref = VSlotRef::create_shared(&slot);
        auto context = VExprContext::create_shared(ref);
        ASSERT_TRUE(context->prepare(&state, row).ok());
        const auto digest = ref->get_digest(12345);
        EXPECT_NE(digest, 0);
        EXPECT_TRUE(digests.insert(digest).second) << slot.debug_string();

        VExprSPtr clone;
        ASSERT_TRUE(ref->clone_node(&clone).ok());
        EXPECT_EQ(digest, clone->get_digest(12345));
        VExprSPtr second_clone;
        ASSERT_TRUE(clone->clone_node(&second_clone).ok());
        EXPECT_EQ(digest, second_clone->get_digest(12345));

        TExprNode node;
        node.__set_node_type(TExprNodeType::SLOT_REF);
        node.__set_type(type->to_thrift());
        TSlotRef thrift_ref;
        thrift_ref.__set_slot_id(0);
        node.__set_slot_ref(thrift_ref);
        auto from_node = VSlotRef::create_shared(node);
        auto node_context = VExprContext::create_shared(from_node);
        ASSERT_TRUE(node_context->prepare(&state, row).ok());
        EXPECT_EQ(digest, from_node->get_digest(12345));
    }
    EXPECT_EQ(paths.size(), digests.size());
}

} // namespace doris
