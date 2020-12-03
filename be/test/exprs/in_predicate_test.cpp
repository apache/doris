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

#include "exprs/in_predicate.h"

#include <gtest/gtest.h>

#include <string>

#include "common/object_pool.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"

namespace doris {

// mock
class InPredicateTest : public testing::Test {
public:
    InPredicateTest() : _runtime_stat("abc") {
        _in_node.node_type = TExprNodeType::IN_PRED;
        _in_node.type = TColumnType();
        _in_node.num_children = 0;
        _in_node.in_predicate.is_not_in = false;
        _in_node.__isset.in_predicate = true;

        _tuple_row._tuples[0] = (Tuple*)&_data;
    }
    void init_in_pre(InPredicate* in_pre) {
        in_pre->_children.push_back(_obj_pool.add(new SlotRef(TYPE_INT, 0)));

        for (int i = 0; i < 100; ++i) {
            in_pre->_children.push_back(Expr::create_literal(&_obj_pool, TYPE_INT, &i));
        }

        in_pre->_children.push_back(_obj_pool.add(new SlotRef(TYPE_INT, 4)));
    }

protected:
    virtual void SetUp() { _data[0] = _data[1] = -1; }
    virtual void TearDown() {}

private:
    TExprNode _in_node;
    ObjectPool _obj_pool;
    RuntimeState _runtime_stat;
    RowDescriptor _row_desc;
    int _data[2];
    TupleRow _tuple_row;
};

TEST_F(InPredicateTest, push_100_const) {
    InPredicate in_pre(_in_node);
    in_pre._children.push_back(_obj_pool.add(new SlotRef(TYPE_INT, 0)));
    Status status = in_pre.prepare(&_runtime_stat, _row_desc);
    ASSERT_TRUE(status.ok());

    for (int i = 0; i < 100; ++i) {
        in_pre.insert(&i);
    }

    ASSERT_EQ(100, in_pre._hybird_set->size());
    ASSERT_EQ(1, in_pre._children.size());

    for (int i = 0; i < 100; ++i) {
        _data[0] = i;
        ASSERT_TRUE(*(bool*)in_pre.get_value(&_tuple_row));
    }

    _data[0] = 101;
    ASSERT_FALSE(*(bool*)in_pre.get_value(&_tuple_row));
}

TEST_F(InPredicateTest, no_child) {
    InPredicate in_pre(_in_node);
    Status status = in_pre.prepare(&_runtime_stat, _row_desc);
    ASSERT_FALSE(status.ok());
}
TEST_F(InPredicateTest, diff_type) {
    InPredicate in_pre(_in_node);
    SlotRef* slot_ref = _obj_pool.add(new SlotRef(TYPE_BOOLEAN, 0));
    in_pre._children.push_back(slot_ref);

    for (int i = 0; i < 100; ++i) {
        in_pre._children.push_back(Expr::create_literal(&_obj_pool, TYPE_INT, &i));
    }

    Status status = in_pre.prepare(&_runtime_stat, _row_desc);
    ASSERT_FALSE(status.ok());
}

TEST_F(InPredicateTest, 100_const) {
    InPredicate in_pre(_in_node);
    init_in_pre(&in_pre);
    Status status = in_pre.prepare(&_runtime_stat, _row_desc);
    ASSERT_TRUE(status.ok());
    status = in_pre.prepare(&_runtime_stat, _row_desc);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(100, in_pre._hybird_set->size());
    ASSERT_EQ(2, in_pre._children.size());

    for (int i = 0; i < 100; ++i) {
        _data[0] = i;
        ASSERT_TRUE(*(bool*)in_pre.get_value(&_tuple_row));
    }

    _data[0] = 101;
    ASSERT_FALSE(*(bool*)in_pre.get_value(&_tuple_row));
    _data[1] = 101;
    ASSERT_TRUE(*(bool*)in_pre.get_value(&_tuple_row));
}

} // namespace doris

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}

/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */
