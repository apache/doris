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

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>
#include <stdint.h>
#include <string.h>
#include <sys/types.h>

#include <cmath>
#include <limits>
#include <new>
#include <type_traits>

#include "common/object_pool.h"
#include "exec/schema_scanner.h"
#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/Types_types.h"
#include "gtest/gtest_pred_impl.h"
#include "runtime/descriptors.h"
#include "runtime/jsonb_value.h"
#include "runtime/large_int_value.h"
#include "runtime/runtime_state.h"
#include "testutil/desc_tbl_builder.h"
#include "vec/columns/columns_number.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vliteral.h"
#include "vec/runtime/vdatetime_value.h"
#include "vec/utils/util.hpp"

TEST(TEST_VEXPR, ABSTEST) {
    doris::ObjectPool object_pool;
    doris::DescriptorTblBuilder builder(&object_pool);
    builder.declare_tuple() << doris::TYPE_INT << doris::TYPE_DOUBLE;
    doris::DescriptorTbl* desc_tbl = builder.build();

    auto tuple_desc = const_cast<doris::TupleDescriptor*>(desc_tbl->get_tuple_descriptor(0));
    doris::RowDescriptor row_desc(tuple_desc, false);
    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    doris::TExpr exprx = apache::thrift::from_json_string<doris::TExpr>(expr_json);
    doris::vectorized::VExprContextSPtr context;
    static_cast<void>(doris::vectorized::VExpr::create_expr_tree(exprx, context));

    doris::RuntimeState runtime_stat;
    runtime_stat.set_desc_tbl(desc_tbl);
    auto state = doris::Status::OK();
    state = context->prepare(&runtime_stat, row_desc);
    ASSERT_TRUE(state.ok());
    state = context->open(&runtime_stat);
    ASSERT_TRUE(state.ok());
}

// Only the unit test depend on this, but it is wrong, should not use TTupleDesc to create tuple desc, not
// use columndesc
static doris::TupleDescriptor* create_tuple_desc(
        doris::ObjectPool* pool, std::vector<doris::SchemaScanner::ColumnDesc>& column_descs) {
    using namespace doris;
    int null_column = 0;
    for (int i = 0; i < column_descs.size(); ++i) {
        if (column_descs[i].is_null) {
            null_column++;
        }
    }

    int offset = (null_column + 7) / 8;
    std::vector<SlotDescriptor*> slots;
    int null_byte = 0;
    int null_bit = 0;

    for (int i = 0; i < column_descs.size(); ++i) {
        TSlotDescriptor t_slot_desc;
        if (column_descs[i].type == TYPE_DECIMALV2) {
            t_slot_desc.__set_slotType(TypeDescriptor::create_decimalv2_type(27, 9).to_thrift());
        } else {
            TypeDescriptor descriptor(column_descs[i].type);
            if (column_descs[i].precision >= 0 && column_descs[i].scale >= 0) {
                descriptor.precision = column_descs[i].precision;
                descriptor.scale = column_descs[i].scale;
            }
            t_slot_desc.__set_slotType(descriptor.to_thrift());
        }
        t_slot_desc.__set_colName(column_descs[i].name);
        t_slot_desc.__set_columnPos(i);
        t_slot_desc.__set_byteOffset(offset);

        if (column_descs[i].is_null) {
            t_slot_desc.__set_nullIndicatorByte(null_byte);
            t_slot_desc.__set_nullIndicatorBit(null_bit);
            null_bit = (null_bit + 1) % 8;

            if (0 == null_bit) {
                null_byte++;
            }
        } else {
            t_slot_desc.__set_nullIndicatorByte(0);
            t_slot_desc.__set_nullIndicatorBit(-1);
        }

        t_slot_desc.id = i;
        t_slot_desc.__set_slotIdx(i);
        t_slot_desc.__set_isMaterialized(true);

        SlotDescriptor* slot = pool->add(new (std::nothrow) SlotDescriptor(t_slot_desc));
        slots.push_back(slot);
        offset += column_descs[i].size;
    }

    TTupleDescriptor t_tuple_desc;
    t_tuple_desc.__set_byteSize(offset);
    t_tuple_desc.__set_numNullBytes(0);
    doris::TupleDescriptor* tuple_desc =
            pool->add(new (std::nothrow) doris::TupleDescriptor(t_tuple_desc));

    for (int i = 0; i < slots.size(); ++i) {
        tuple_desc->add_slot(slots[i]);
    }

    return tuple_desc;
}

TEST(TEST_VEXPR, ABSTEST2) {
    using namespace doris;
    std::vector<doris::SchemaScanner::ColumnDesc> column_descs = {
            {"k1", TYPE_INT, sizeof(int32_t), false}};
    ObjectPool object_pool;
    doris::TupleDescriptor* tuple_desc = create_tuple_desc(&object_pool, column_descs);
    RowDescriptor row_desc(tuple_desc, false);
    std::string expr_json =
            R"|({"1":{"lst":["rec",2,{"1":{"i32":20},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"4":{"i32":1},"20":{"i32":-1},"26":{"rec":{"1":{"rec":{"2":{"str":"abs"}}},"2":{"i32":0},"3":{"lst":["rec",1,{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}]},"4":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":6}}}}]}}},"5":{"tf":0},"7":{"str":"abs(INT)"},"9":{"rec":{"1":{"str":"_ZN5doris13MathFunctions3absEPN9doris_udf15FunctionContextERKNS1_6IntValE"}}},"11":{"i64":0}}}},{"1":{"i32":16},"2":{"rec":{"1":{"lst":["rec",1,{"1":{"i32":0},"2":{"rec":{"1":{"i32":5}}}}]}}},"4":{"i32":0},"15":{"rec":{"1":{"i32":0},"2":{"i32":0}}},"20":{"i32":-1},"23":{"i32":-1}}]}})|";
    TExpr exprx = apache::thrift::from_json_string<TExpr>(expr_json);

    doris::vectorized::VExprContextSPtr context;
    static_cast<void>(doris::vectorized::VExpr::create_expr_tree(exprx, context));

    doris::RuntimeState runtime_stat;
    DescriptorTbl desc_tbl;
    desc_tbl._slot_desc_map[0] = tuple_desc->slots()[0];
    runtime_stat.set_desc_tbl(&desc_tbl);
    auto state = Status::OK();
    state = context->prepare(&runtime_stat, row_desc);
    ASSERT_TRUE(state.ok());
    state = context->open(&runtime_stat);
    ASSERT_TRUE(state.ok());
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
    const static TPrimitiveType::type ttype = TPrimitiveType::DATETIME;
    const static TExprNodeType::type tnode_type = TExprNodeType::DATE_LITERAL;
    using CXXType = std::string;
};
template <>
struct literal_traits<TYPE_DATETIMEV2> {
    const static TPrimitiveType::type ttype = TPrimitiveType::DATETIMEV2;
    const static TExprNodeType::type tnode_type = TExprNodeType::DATE_LITERAL;
    using CXXType = std::string;
};
template <>
struct literal_traits<TYPE_DATE> {
    const static TPrimitiveType::type ttype = TPrimitiveType::DATE;
    const static TExprNodeType::type tnode_type = TExprNodeType::DATE_LITERAL;
    using CXXType = std::string;
};
template <>
struct literal_traits<TYPE_DATEV2> {
    const static TPrimitiveType::type ttype = TPrimitiveType::DATEV2;
    const static TExprNodeType::type tnode_type = TExprNodeType::DATE_LITERAL;
    using CXXType = std::string;
};

template <>
struct literal_traits<TYPE_JSONB> {
    const static TPrimitiveType::type ttype = TPrimitiveType::JSONB;
    const static TExprNodeType::type tnode_type = TExprNodeType::JSON_LITERAL;
    using CXXType = std::string;
};

template <>
struct literal_traits<TYPE_STRING> {
    const static TPrimitiveType::type ttype = TPrimitiveType::STRING;
    const static TExprNodeType::type tnode_type = TExprNodeType::STRING_LITERAL;
    using CXXType = std::string;
};

template <>
struct literal_traits<TYPE_DECIMALV2> {
    const static TPrimitiveType::type ttype = TPrimitiveType::DECIMALV2;
    const static TExprNodeType::type tnode_type = TExprNodeType::DECIMAL_LITERAL;
    using CXXType = std::string;
};

//======================== set literal ===================================
template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires std::is_integral_v<U>
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
template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_DATETIME)
void set_literal(TExprNode& node, const U& value) {
    TDateLiteral date_literal;
    date_literal.__set_value(value);
    node.__set_date_literal(date_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_DATETIMEV2)
void set_literal(TExprNode& node, const U& value) {
    TDateLiteral date_literal;
    date_literal.__set_value(value);
    node.__set_date_literal(date_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_DATE)
void set_literal(TExprNode& node, const U& value) {
    TDateLiteral date_literal;
    date_literal.__set_value(value);
    node.__set_date_literal(date_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_DATEV2)
void set_literal(TExprNode& node, const U& value) {
    TDateLiteral date_literal;
    date_literal.__set_value(value);
    node.__set_date_literal(date_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_JSONB)
void set_literal(TExprNode& node, const U& value) {
    TJsonLiteral jsonb_literal;
    jsonb_literal.__set_value(value);
    node.__set_json_literal(jsonb_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_STRING)
void set_literal(TExprNode& node, const U& value) {
    TStringLiteral string_literal;
    string_literal.__set_value(value);
    node.__set_string_literal(string_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires std::numeric_limits<U>::is_iec559
void set_literal(TExprNode& node, const U& value) {
    TFloatLiteral floatLiteral;
    floatLiteral.__set_value(value);
    node.__set_float_literal(floatLiteral);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
    requires(T == TYPE_DECIMALV2)
void set_literal(TExprNode& node, const U& value) {
    TDecimalLiteral decimal_literal;
    decimal_literal.__set_value(value);
    node.__set_decimal_literal(decimal_literal);
}

template <PrimitiveType T, class U = typename literal_traits<T>::CXXType>
doris::TExprNode create_literal(const U& value, int scale = 9) {
    TExprNode node;
    TTypeDesc type_desc;
    TTypeNode type_node;
    std::vector<TTypeNode> type_nodes;
    type_nodes.emplace_back();
    TScalarType scalar_type;
    scalar_type.__set_precision(27);
    scalar_type.__set_scale(scale);
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
    // bool
    {
        VLiteral literal(create_literal<TYPE_BOOLEAN>(true));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<uint8_t>();
        EXPECT_EQ(v, true);
        EXPECT_EQ("1", literal.value());
    }
    // smallint
    {
        VLiteral literal(create_literal<TYPE_SMALLINT>(1024));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<int16_t>();
        EXPECT_EQ(v, 1024);
        EXPECT_EQ("1024", literal.value());
    }
    // int
    {
        VLiteral literal(create_literal<TYPE_INT>(1024));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<int32_t>();
        EXPECT_EQ(v, 1024);
        EXPECT_EQ("1024", literal.value());
    }
    // bigint
    {
        VLiteral literal(create_literal<TYPE_BIGINT>(1024));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<int64_t>();
        EXPECT_EQ(v, 1024);
        EXPECT_EQ("1024", literal.value());
    }
    // large int
    {
        VLiteral literal(create_literal<TYPE_LARGEINT, __int128_t>(1024));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<__int128_t>();
        EXPECT_EQ(v, 1024);
        EXPECT_EQ("1024", literal.value());
    }
    // float
    {
        VLiteral literal(create_literal<TYPE_FLOAT, float>(1024.0f));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<double>();
        EXPECT_FLOAT_EQ(v, 1024.0f);
        EXPECT_EQ("1024", literal.value());
    }
    // double
    {
        VLiteral literal(create_literal<TYPE_DOUBLE, double>(1024.0));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<double>();
        EXPECT_FLOAT_EQ(v, 1024.0);
        EXPECT_EQ("1024", literal.value());
    }
    // datetime
    {
        VecDateTimeValue data_time_value;
        const char* date = "20210407000000";
        data_time_value.from_date_str(date, strlen(date));
        std::cout << data_time_value.type() << std::endl;
        __int64_t dt;
        memcpy(&dt, &data_time_value, sizeof(__int64_t));
        VLiteral literal(create_literal<TYPE_DATETIME, std::string>(std::string(date)));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<__int64_t>();
        EXPECT_EQ(v, dt);
        EXPECT_EQ("2021-04-07 00:00:00", literal.value());
    }
    // datetimev2
    {
        uint16_t year = 1997;
        uint8_t month = 11;
        uint8_t day = 18;
        uint8_t hour = 9;
        uint8_t minute = 12;
        uint8_t second = 46;
        uint32_t microsecond = 999999; // target scale is 4, so the microsecond will be rounded up
        DateV2Value<DateTimeV2ValueType> datetime_v2;
        datetime_v2.unchecked_set_time(year, month, day, hour, minute, second, microsecond);
        std::string date = datetime_v2.debug_string();

        VLiteral literal(create_literal<TYPE_DATETIMEV2, std::string>(date, 4));
        Block block;
        int ret = -1;
        EXPECT_TRUE(literal.execute(nullptr, &block, &ret).ok());
        EXPECT_EQ("1997-11-18 09:12:47.0000", literal.value());
    }
    // date
    {
        VecDateTimeValue data_time_value;
        const char* date = "20210407";
        data_time_value.from_date_str(date, strlen(date));
        __int64_t dt;
        memcpy(&dt, &data_time_value, sizeof(__int64_t));
        VLiteral literal(create_literal<TYPE_DATE, std::string>(std::string(date)));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<__int64_t>();
        EXPECT_EQ(v, dt);
        EXPECT_EQ("2021-04-07", literal.value());
    }
    // datev2
    {
        DateV2Value<DateV2ValueType> data_time_value;
        const char* date = "20210407";
        data_time_value.from_date_str(date, strlen(date));
        uint32_t dt;
        memcpy(&dt, &data_time_value, sizeof(uint32_t));
        VLiteral literal(create_literal<TYPE_DATEV2, std::string>(std::string(date)));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<uint32_t>();
        EXPECT_EQ(v, dt);
        EXPECT_EQ("2021-04-07", literal.value());
    }
    {
        DateV2Value<DateV2ValueType> data_time_value;
        const char* date = "00000000";
        EXPECT_EQ(data_time_value.from_date_str(date, strlen(date), -1, true), true);

        DateV2Value<DateV2ValueType> data_time_value1;
        const char* date1 = "00000101";
        EXPECT_EQ(data_time_value1.from_date_str(date1, strlen(date1), -1, true), true);
        EXPECT_EQ(data_time_value.to_int64(), data_time_value1.to_int64());

        EXPECT_EQ(data_time_value.from_date_str(date, strlen(date)), false);
    }
    {
        DateV2Value<DateTimeV2ValueType> data_time_value;
        const char* date = "00000000111111";
        EXPECT_EQ(data_time_value.from_date_str(date, strlen(date), -1, true), true);

        DateV2Value<DateTimeV2ValueType> data_time_value1;
        const char* date1 = "00000101111111";
        EXPECT_EQ(data_time_value1.from_date_str(date1, strlen(date1), -1, true), true);
        EXPECT_EQ(data_time_value.to_int64(), data_time_value1.to_int64());

        EXPECT_EQ(data_time_value.from_date_str(date, strlen(date)), false);
    }
    // jsonb
    {
        std::string j = R"([null,true,false,100,6.18,"abc"])";
        VLiteral literal(create_literal<TYPE_JSONB, std::string>(j));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        EXPECT_EQ(j, literal.value());
    }
    // string
    {
        std::string s = "I am Amory, 24";
        VLiteral literal(create_literal<TYPE_STRING, std::string>(std::string("I am Amory, 24")));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<String>();
        EXPECT_EQ(v, s);
        EXPECT_EQ(s, literal.value());
    }
    // decimalv2
    {
        VLiteral literal(create_literal<TYPE_DECIMALV2, std::string>(std::string("1234.56")));
        Block block;
        int ret = -1;
        static_cast<void>(literal.execute(nullptr, &block, &ret));
        auto ctn = block.safe_get_by_position(ret);
        auto v = (*ctn.column)[0].get<DecimalField<Decimal128V2>>();
        EXPECT_FLOAT_EQ(((double)v.get_value()) / (std::pow(10, v.get_scale())), 1234.56);
        EXPECT_EQ("1234.560000000", literal.value());
    }
}
