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

#include "vec/exprs/vexpr.h"

#include <thrift/protocol/TJSONProtocol.h>

#include <cmath>
#include <iostream>

#include "exec/schema_scanner.h"
#include "gen_cpp/Data_types.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "gtest/gtest.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/memory/chunk_allocator.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "testutil/desc_tbl_builder.h"
#include "vec/exprs/vliteral.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"
TEST(TEST_VEXPR, ABSTEST) {
    doris::ChunkAllocator::init_instance(4096);
    doris::ObjectPool object_pool;
    doris::DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << doris::TYPE_INT << doris::TYPE_DOUBLE;
    doris::DescriptorTbl* desc_tbl = builder.build();

    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);
    doris::RowBatch row_batch(row_desc, 1024);
    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    doris::TExpr exprx = apache::thrift::from_json_string<doris::TExpr>(expr_json);
    doris::vectorized::VExprContext* context = nullptr;
    doris::vectorized::VExpr::create_expr_tree(&object_pool, exprx, &context);

    int32_t k1 = -100;
    for (int i = 0; i < 1024; ++i, k1++) {
        auto idx = row_batch.add_row();
        doris::TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple =
                (doris::Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, slot_desc->slot_size());
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    doris::RuntimeState runtime_stat(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    runtime_stat.set_desc_tbl(desc_tbl);
    std::shared_ptr<doris::MemTracker> tracker = doris::MemTracker::create_tracker();
    context->prepare(&runtime_stat, row_desc, tracker);
    context->open(&runtime_stat);

    auto block = row_batch.convert_to_vec_block();
    int ts = -1;
    context->execute(&block, &ts);

    context->close(&runtime_stat);
}

TEST(TEST_VEXPR, ABSTEST2) {
    using namespace doris;
    SchemaScanner::ColumnDesc column_descs[] = {{"k1", TYPE_INT, sizeof(int32_t), false}};
    SchemaScanner schema_scanner(column_descs, 1);
    ObjectPool object_pool;
    SchemaScannerParam param;
    schema_scanner.init(&param, &object_pool);
    auto tuple_desc = const_cast<TupleDescriptor*>(schema_scanner.tuple_desc());
    RowDescriptor row_desc(tuple_desc, false);
    RowBatch row_batch(row_desc, 1024);
    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    TExpr exprx = apache::thrift::from_json_string<TExpr>(expr_json);

    doris::vectorized::VExprContext* context = nullptr;
    doris::vectorized::VExpr::create_expr_tree(&object_pool, exprx, &context);

    int32_t k1 = -100;
    for (int i = 0; i < 1024; ++i, k1++) {
        auto idx = row_batch.add_row();
        doris::TupleRow* tuple_row = row_batch.get_row(idx);
        auto tuple =
                (doris::Tuple*)(row_batch.tuple_data_pool()->allocate(tuple_desc->byte_size()));
        auto slot_desc = tuple_desc->slots()[0];
        memcpy(tuple->get_slot(slot_desc->tuple_offset()), &k1, slot_desc->slot_size());
        tuple_row->set_tuple(0, tuple);
        row_batch.commit_last_row();
    }

    doris::RuntimeState runtime_stat(doris::TUniqueId(), doris::TQueryOptions(),
                                     doris::TQueryGlobals(), nullptr);
    runtime_stat.init_instance_mem_tracker();
    DescriptorTbl desc_tbl;
    desc_tbl._slot_desc_map[0] = tuple_desc->slots()[0];
    runtime_stat.set_desc_tbl(&desc_tbl);
    std::shared_ptr<doris::MemTracker> tracker = doris::MemTracker::create_tracker();
    context->prepare(&runtime_stat, row_desc, tracker);
    context->open(&runtime_stat);

    auto block = row_batch.convert_to_vec_block();
    int ts = -1;
    context->execute(&block, &ts);
    context->close(&runtime_stat);
}

namespace doris {
template <PrimitiveType T>
struct literal_traits {};

template <>
struct literal_traits<TYPE_BOOLEAN> {
    const static TPrimitiveType::type ttype = TPrimitiveType::BOOLEAN;
    const static TExprNodeType::type tnode_type = TExprNodeType::BOOL_LITERAL;
    using CXXType = bool;
};

template <>
struct literal_traits<TYPE_SMALLINT> {
    const static TPrimitiveType::type ttype = TPrimitiveType::SMALLINT;
    const static TExprNodeType::type tnode_type = TExprNodeType::INT_LITERAL;
    using CXXType = int16_t;
};

template <>
struct literal_traits<TYPE_INT> {
    const static TPrimitiveType::type ttype = TPrimitiveType::INT;
    const static TExprNodeType::type tnode_type = TExprNodeType::INT_LITERAL;
    using CXXType = int32_t;
};

template <>
struct literal_traits<TYPE_BIGINT> {
    const static TPrimitiveType::type ttype = TPrimitiveType::BIGINT;
    const static TExprNodeType::type tnode_type = TExprNodeType::INT_LITERAL;
    using CXXType = int64_t;
};

template <>
struct literal_traits<TYPE_LARGEINT> {
    const static TPrimitiveType::type ttype = TPrimitiveType::LARGEINT;
    const static TExprNodeType::type tnode_type = TExprNodeType::LARGE_INT_LITERAL;
    using CXXType = __int128_t;
};

template <>
struct literal_traits<TYPE_FLOAT> {
    const static TPrimitiveType::type ttype = TPrimitiveType::FLOAT;
    const static TExprNodeType::type tnode_type = TExprNodeType::FLOAT_LITERAL;
    using CXXType = float;
};

template <>
struct literal_traits<TYPE_DOUBLE> {
    const static TPrimitiveType::type ttype = TPrimitiveType::FLOAT;
    const static TExprNodeType::type tnode_type = TExprNodeType::FLOAT_LITERAL;
    using CXXType = float;
};

template <>
struct literal_traits<TYPE_DATETIME> {
    const static TPrimitiveType::type ttype = TPrimitiveType::DATE;
    const static TExprNodeType::type tnode_type = TExprNodeType::STRING_LITERAL;
    using CXXType = std::string;
};

template <>
struct literal_traits<TYPE_DECIMALV2> {
    const static TPrimitiveType::type ttype = TPrimitiveType::DECIMALV2;
    const static TExprNodeType::type tnode_type = TExprNodeType::DECIMAL_LITERAL;
    using CXXType = std::string;
};

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType,
          std::enable_if_t<std::is_integral<U>::value, bool> = true>
void set_literal(TExprNode& node, const U& value) {
    TIntLiteral int_literal;
    int_literal.__set_value(value);
    node.__set_int_literal(int_literal);
}

template <>
void set_literal<TYPE_BOOLEAN, bool>(TExprNode& node, const bool& value) {
    TBoolLiteral bool_literal;
    bool_literal.__set_value(value);
    node.__set_bool_literal(bool_literal);
}

template <>
void set_literal<TYPE_LARGEINT, __int128_t>(TExprNode& node, const __int128_t& value) {
    TLargeIntLiteral largeIntLiteral;
    largeIntLiteral.__set_value(LargeIntValue::to_string(value));
    node.__set_large_int_literal(largeIntLiteral);
}
// std::is_same<U, std::string>::value
template <PrimitiveType T, class U = typename literal_traits<T>::CXXType,
          std::enable_if_t<T == TYPE_DATETIME, bool> = true>
void set_literal(TExprNode& node, const U& value) {
    TDateLiteral date_literal;
    date_literal.__set_value(value);
    node.__set_date_literal(date_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType,
          std::enable_if_t<std::numeric_limits<U>::is_iec559, bool> = true>
void set_literal(TExprNode& node, const U& value) {
    TFloatLiteral floatLiteral;
    floatLiteral.__set_value(value);
    node.__set_float_literal(floatLiteral);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType,
          std::enable_if_t<T == TYPE_DECIMALV2, bool> = true>
void set_literal(TExprNode& node, const U& value) {
    TDecimalLiteral decimal_literal;
    decimal_literal.__set_value(value);
    node.__set_decimal_literal(decimal_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
doris::TExprNode create_literal(const U& value) {
    TExprNode node;
    TTypeDesc type_desc;
    TTypeNode type_node;
    std::vector<TTypeNode> type_nodes;
    type_nodes.emplace_back();
    TScalarType scalar_type;
    scalar_type.__set_precision(27);
    scalar_type.__set_scale(9);
    scalar_type.__set_len(20);
    scalar_type.__set_type(literal_traits<T>::ttype);
    type_nodes[0].__set_scalar_type(scalar_type);
    type_desc.__set_types(type_nodes);
    node.__set_type(type_desc);
    node.__set_node_type(literal_traits<T>::tnode_type);
    set_literal<T, U>(node, value);
    return node;
}
} // namespace doris

TEST(TEST_VEXPR, LITERALTEST) {
    using namespace doris;
    using namespace doris::vectorized;
    {
        VLiteral literal(create_literal<TYPE_BOOLEAN>(true));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        bool v = ctn.column->get_bool(0);
        EXPECT_EQ(v, true);
    }
    {
        VLiteral literal(create_literal<TYPE_SMALLINT>(1024));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = ctn.column->get64(0);
        EXPECT_EQ(v, 1024);
    }
    {
        VLiteral literal(create_literal<TYPE_INT>(1024));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = ctn.column->get64(0);
        EXPECT_EQ(v, 1024);
    }
    {
        VLiteral literal(create_literal<TYPE_BIGINT>(1024));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = ctn.column->get64(0);
        EXPECT_EQ(v, 1024);
    }
    {
        VLiteral literal(create_literal<TYPE_LARGEINT, __int128_t>(1024));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<__int128_t>();
        EXPECT_EQ(v, 1024);
    }
    {
        VLiteral literal(create_literal<TYPE_FLOAT, float>(1024.0f));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<double>();
        EXPECT_FLOAT_EQ(v, 1024.0f);
    }
    {
        VLiteral literal(create_literal<TYPE_DOUBLE, double>(1024.0));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<double>();
        EXPECT_FLOAT_EQ(v, 1024.0);
    }
    {
        vectorized::VecDateTimeValue data_time_value;
        const char* date = "20210407";
        data_time_value.from_date_str(date, strlen(date));
        __int64_t dt;
        memcpy(&dt, &data_time_value, sizeof(__int64_t));
        VLiteral literal(create_literal<TYPE_DATETIME, std::string>(std::string(date)));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<__int64_t>();
        EXPECT_EQ(v, dt);
    }
    {
        VLiteral literal(create_literal<TYPE_DECIMALV2, std::string>(std::string("1234.56")));
        Block block;
        int ret = -1;
        literal.execute(nullptr, &block, &ret);
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<DecimalField<Decimal128>>();
        EXPECT_FLOAT_EQ(((double)v.get_value()) / (std::pow(10, v.get_scale())), 1234.56);
    }
}
