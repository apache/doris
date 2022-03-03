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

#include "exec/es/es_predicate.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "exec/es/es_query_builder.h"
#include "exprs/binary_predicate.h"
#include "gen_cpp/Exprs_types.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "runtime/mem_tracker.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"

namespace doris {

class RuntimeState;

class EsPredicateTest : public testing::Test {
public:
    EsPredicateTest() : _runtime_state(TQueryGlobals()) {
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
        TDescriptorTable t_desc_table;

        // table descriptors
        TTableDescriptor t_table_desc;
        t_table_desc.id = 0;
        t_table_desc.tableType = TTableType::ES_TABLE;
        t_table_desc.numCols = 1;
        t_table_desc.numClusteringCols = 0;
        t_table_desc.__isset.esTable = true;
        t_desc_table.tableDescriptors.push_back(t_table_desc);
        t_desc_table.__isset.tableDescriptors = true;

        // TSlotDescriptor
        int offset = 1;
        int i = 0;
        // id
        {
            TSlotDescriptor t_slot_desc;
            t_slot_desc.__set_slotType(TypeDescriptor(TYPE_INT).to_thrift());
            t_slot_desc.__set_columnPos(i);
            t_slot_desc.__set_byteOffset(offset);
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
            t_slot_desc.__set_slotIdx(i);
            t_slot_desc.__set_isMaterialized(true);
            t_slot_desc.colName = "id";
            t_desc_table.slotDescriptors.push_back(t_slot_desc);
            offset += sizeof(int);
        }

        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = offset;
        t_tuple_desc.numNullBytes = 1;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.__isset.slotDescriptors = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);

        DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);
        _runtime_state.set_desc_tbl(_desc_tbl);
    }

    Status build_expr_context_list(std::vector<ExprContext*>& conjunct_ctxs);
    void init();
    void SetUp() override {}
    void TearDown() override {}

private:
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    RuntimeState _runtime_state;
};

Status EsPredicateTest::build_expr_context_list(std::vector<ExprContext*>& conjunct_ctxs) {
    TExpr texpr;
    {
        TExprNode node0;
        node0.opcode = TExprOpcode::GT;
        node0.child_type = TPrimitiveType::BIGINT;
        node0.node_type = TExprNodeType::BINARY_PRED;
        node0.num_children = 2;
        node0.__isset.opcode = true;
        node0.__isset.child_type = true;
        node0.type = gen_type_desc(TPrimitiveType::BOOLEAN);
        texpr.nodes.emplace_back(node0);

        TExprNode node1;
        node1.node_type = TExprNodeType::SLOT_REF;
        node1.type = gen_type_desc(TPrimitiveType::INT);
        node1.__isset.slot_ref = true;
        node1.num_children = 0;
        node1.slot_ref.slot_id = 0;
        node1.slot_ref.tuple_id = 0;
        node1.output_column = true;
        node1.__isset.output_column = true;
        texpr.nodes.emplace_back(node1);

        TExprNode node2;
        TIntLiteral intLiteral;
        intLiteral.value = 10;
        node2.node_type = TExprNodeType::INT_LITERAL;
        node2.type = gen_type_desc(TPrimitiveType::BIGINT);
        node2.__isset.int_literal = true;
        node2.int_literal = intLiteral;
        texpr.nodes.emplace_back(node2);
    }

    std::vector<TExpr> conjuncts;
    conjuncts.emplace_back(texpr);
    Status status = Expr::create_expr_trees(&_obj_pool, conjuncts, &conjunct_ctxs);

    return status;
}

TEST_F(EsPredicateTest, normal) {
    std::vector<ExprContext*> conjunct_ctxs;
    Status status = build_expr_context_list(conjunct_ctxs);
    ASSERT_TRUE(status.ok());
    TupleDescriptor* tuple_desc = _desc_tbl->get_tuple_descriptor(0);
    std::vector<EsPredicate*> predicates;
    for (int i = 0; i < conjunct_ctxs.size(); ++i) {
        EsPredicate* predicate = new EsPredicate(conjunct_ctxs[i], tuple_desc, &_obj_pool);
        if (predicate->build_disjuncts_list().ok()) {
            predicates.push_back(predicate);
        }
    }

    rapidjson::Document document;
    rapidjson::Value compound_bool_value(rapidjson::kObjectType);
    compound_bool_value.SetObject();
    BooleanQueryBuilder::to_query(predicates, &document, &compound_bool_value);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    compound_bool_value.Accept(writer);
    std::string actual_bool_json = buffer.GetString();
    std::string expected_json =
            "{\"bool\":{\"filter\":[{\"bool\":{\"should\":[{\"range\":{\"id\":{\"gt\":\"10\"}}}]}}]"
            "}}";
    LOG(INFO) << "compound bool query" << actual_bool_json;
    ASSERT_STREQ(expected_json.c_str(), actual_bool_json.c_str());
    for (auto predicate : predicates) {
        delete predicate;
    }
}

} // end namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
