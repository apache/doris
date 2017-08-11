// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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
#include <sstream>

#include "common/object_pool.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "olap/row_block.h"
#include "olap/olap_table.h"
#include "runtime/runtime_state.h"
#include "util/debug_util.h"
#include "util/logging.h"

using std::vector;
using std::string;
using std::endl;
using std::stringstream;

namespace palo {
void set_field_info(
        FieldInfo& info, 
        const char* name, 
        FieldType type,
        FieldAggregationMethod agg,
        uint32_t length,
        bool is_key) {
    info.name = string(name);
    info.type = type;
    info.aggregation = agg;
    info.length = length;
    info.is_key = is_key;
}

static size_t g_row_length = 0;
static size_t g_row_length_mysql = 0;

void make_error_schema_array(vector<FieldInfo>& schema_array) {
    schema_array.clear();
    FieldInfo info;
    set_field_info(info, "", OLAP_FIELD_TYPE_UNKNOWN, OLAP_FIELD_AGGREGATION_SUM, 0, true);
    schema_array.push_back(info);
}

// 生成一份FieldInfo array
void make_schema_array(vector<FieldInfo>& schema_array) {
    schema_array.clear();
    FieldInfo info;
    set_field_info(info, "", OLAP_FIELD_TYPE_INT, OLAP_FIELD_AGGREGATION_SUM, 4, true);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_AGGREGATION_SUM, 8, true);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_AGGREGATION_SUM, 2, true);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_UNSIGNED_INT, OLAP_FIELD_AGGREGATION_MAX, 4, true);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_AGGREGATION_REPLACE, 32, true);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_AGGREGATION_SUM, 4, false);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_AGGREGATION_REPLACE, 64, false);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_DATE, OLAP_FIELD_AGGREGATION_MAX, 4, false);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_INT, OLAP_FIELD_AGGREGATION_SUM, 4, false);
    schema_array.push_back(info);

    g_row_length = sizeof(int) + sizeof(int64_t) + sizeof(int16_t) +
            sizeof(uint32_t) + 32 + sizeof(float) + 64 + sizeof(int) + sizeof(int);

    g_row_length_mysql = g_row_length - 1;
}

// 生成一份short FieldInfo array
void make_schema_array_short(vector<FieldInfo>& schema_array) {
    schema_array.clear();
    FieldInfo info;
    set_field_info(info, "", OLAP_FIELD_TYPE_INT, OLAP_FIELD_AGGREGATION_SUM, 4, true);
    schema_array.push_back(info);

    set_field_info(info, "", OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_AGGREGATION_SUM, 8, true);
    schema_array.push_back(info);
}

void make_string_array_short(vector<string>& string_array) {
    string_array.clear();
    string_array.push_back(string("123"));
    string_array.push_back(string("456000"));
}

// 按照上面的schema构造各个column上的value
void make_string_array(vector<string>& string_array) {
    string_array.clear();
    string_array.push_back(string("123"));
    string_array.push_back(string("456000"));
    string_array.push_back(string("789"));
    string_array.push_back(string("0"));
    string_array.push_back(string("guping"));
    string_array.push_back(string("123.0"));
    string_array.push_back(string("olap"));
    string_array.push_back(string("2012-5-8"));
    string_array.push_back(string("100"));
}

void make_string_array_0(vector<string>& string_array) {
    string_array.clear();
    string_array.push_back(string("123"));
    string_array.push_back(string("456000"));
    string_array.push_back(string("789"));
    string_array.push_back(string("1"));
    string_array.push_back(string("guping"));
    string_array.push_back(string("123.0"));
    string_array.push_back(string("olap"));
    string_array.push_back(string("2012-5-8"));
    string_array.push_back(string("100"));
}

void make_string_array_1(vector<string>& string_array) {
    string_array.clear();
    string_array.push_back(string("123"));
    string_array.push_back(string("456000"));
    string_array.push_back(string("790"));
    string_array.push_back(string("0"));
    string_array.push_back(string("guping"));
    string_array.push_back(string("123.0"));
    string_array.push_back(string("olap"));
    string_array.push_back(string("2012-5-8"));
    string_array.push_back(string("100"));
}
void make_string_array_2(vector<string>& string_array) {
    string_array.clear();
    string_array.push_back(string("123"));
    string_array.push_back(string("456001"));
    string_array.push_back(string("789"));
    string_array.push_back(string("0"));
    string_array.push_back(string("guping"));
    string_array.push_back(string("123.0"));
    string_array.push_back(string("olap"));
    string_array.push_back(string("2012-5-8"));
    string_array.push_back(string("100"));
}
void make_string_array_3(vector<string>& string_array) {
    string_array.clear();
    string_array.push_back(string("124"));
    string_array.push_back(string("456000"));
    string_array.push_back(string("789"));
    string_array.push_back(string("0"));
    string_array.push_back(string("guping"));
    string_array.push_back(string("123.0"));
    string_array.push_back(string("olap"));
    string_array.push_back(string("2012-5-8"));
    string_array.push_back(string("100"));
}

// 测试成员函数在初始化、参数异常条件下的行为
class TestRowBlockSimple : public testing::Test {
protected:
    void SetUp() {
        make_schema_array(_tablet_schema);
        _block_info.checksum = 12345678;
        _block_info.row_num = 8;
        _block_info.unpacked_len = 0;
    }
    void TearDown() {
    }
    vector<FieldInfo> _tablet_schema;
    RowBlockInfo _block_info;
};

// 测试成员init方法
TEST_F(TestRowBlockSimple, test_init) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    ASSERT_EQ(row_block._buf_len, g_row_length * 8);
    ASSERT_EQ(OLAP_ERR_INIT_FAILED, row_block.init(_block_info));
    vector<FieldInfo> temp_schema = row_block.tablet_schema();
    ASSERT_EQ(temp_schema.size(), _tablet_schema.size());
}

// 测试各成员函数在未初始化状态下的行为
TEST_F(TestRowBlockSimple, test_not_init) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_ERR_NOT_INITED, row_block.get_row_to_read(0, NULL));
    RowCursor cursor;
    ASSERT_EQ(OLAP_ERR_NOT_INITED, row_block.find_row(cursor, true, NULL));
    ASSERT_EQ(OLAP_ERR_NOT_INITED, row_block.decompress(NULL, 0, OLAP_COMP_STORAGE));
    ASSERT_EQ(OLAP_ERR_NOT_INITED, row_block.set_row(0, cursor));
    ASSERT_EQ(OLAP_ERR_NOT_INITED, row_block.finalize(0));
}

// 测试各成员函数在输入参数错误情况下的行为
TEST_F(TestRowBlockSimple, test_err_param) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    RowCursor cursor;

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.get_row_to_read(100, NULL));
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.get_row_to_read(1, NULL));
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.get_row_to_read(1, &cursor));

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, 
            row_block.find_row(cursor, false, NULL));

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.compress(
                NULL, 0, NULL, OLAP_COMP_STORAGE));

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.decompress(
                NULL, 0, OLAP_COMP_STORAGE));

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.set_row(100, cursor));
    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.set_row(1, cursor));

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.finalize(100));
}

TEST_F(TestRowBlockSimple, test_invalid_schema) {
    vector<FieldInfo> err_schema;
    make_error_schema_array(err_schema);
    RowBlock err_block(err_schema);
    ASSERT_EQ(OLAP_SUCCESS, err_block.init(_block_info));
    RowCursor cursor;
    uint32_t row_index;
    ASSERT_EQ(OLAP_ERR_INIT_FAILED, err_block.find_row(cursor, false, &row_index));
}

class TestRowBlock : public testing::Test {
public:
    TestRowBlock() : test_index(0) {}
    void SetUp() {
        //LOG(INFO) << "test_index is " << test_index++ << endl;
        make_schema_array(_tablet_schema);
        _block_info.checksum = 12345678;
        _block_info.row_num = 8;
        for (uint32_t i = 0; i < _tablet_schema.size(); i++) {
            _block_info.unpacked_len += _tablet_schema[i].length;
        }
        _block_info.unpacked_len *= _block_info.row_num;
        ASSERT_EQ(OLAP_SUCCESS, cursor.init(_tablet_schema));
        ASSERT_EQ(OLAP_SUCCESS, other_cursor.init(_tablet_schema));
        make_schema_array_short(_tablet_schema_short);
        make_string_array_short(string_array_short);
        ASSERT_EQ(OLAP_SUCCESS, short_cursor.init(_tablet_schema_short));
        ASSERT_EQ(OLAP_SUCCESS, short_cursor.from_string(string_array_short));
    }
    void TearDown() {
    }

    void SetRows(RowBlock& row_block) {
        make_string_array_0(string_array);
        ASSERT_EQ(OLAP_SUCCESS, other_cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.set_row(0, other_cursor));

        make_string_array_1(string_array);
        ASSERT_EQ(OLAP_SUCCESS, other_cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.set_row(1, other_cursor));

        make_string_array_2(string_array);
        ASSERT_EQ(OLAP_SUCCESS, other_cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.set_row(2, other_cursor));

        make_string_array_3(string_array);
        ASSERT_EQ(OLAP_SUCCESS, other_cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.set_row(3, other_cursor));
    }

    RowCursor cursor;
    RowCursor other_cursor;
    RowCursor short_cursor;
    size_t test_index;
    RowBlockInfo _block_info;
    vector<FieldInfo> _tablet_schema;
    vector<string> string_array;
    vector<FieldInfo> _tablet_schema_short;
    vector<string> string_array_short;
};

TEST_F(TestRowBlock, test_set_row) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    ASSERT_EQ(row_block._buf_len, g_row_length * 8);
    SetRows(row_block);

    // 尝试用get_row取出一行来进行比较
    make_string_array_0(string_array);
    ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(0, &other_cursor));
    ASSERT_EQ(0, cursor.cmp(other_cursor));
    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(1, &other_cursor));
    ASSERT_EQ(-1, cursor.cmp(other_cursor));

    // 测试finalize
    ASSERT_EQ(OLAP_SUCCESS, row_block.finalize(4));
    ASSERT_EQ(size_t(4), row_block._info.row_num);
    ASSERT_EQ(size_t(4 * g_row_length), row_block._used_buf_size);

    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(0, &other_cursor));
    ASSERT_EQ(0, cursor.cmp(other_cursor));

    // 再次finalize，这次应该对存储内容没有改动
    ASSERT_EQ(OLAP_SUCCESS, row_block.finalize(4));
    ASSERT_EQ(size_t(4), row_block._info.row_num);
    ASSERT_EQ(size_t(4 * g_row_length), row_block._used_buf_size);

    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(1, &other_cursor));
    ASSERT_EQ(-1, cursor.cmp(other_cursor));

    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(0, &other_cursor));
    ASSERT_EQ(0, cursor.cmp(other_cursor));

    // 测试find_row
    uint32_t index = 0;
    // 使用全key进行测试
    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(0, &other_cursor));
    ASSERT_EQ(OLAP_SUCCESS, row_block.find_row(other_cursor, false, &index));
    ASSERT_EQ(uint32_t(0), index);
    ASSERT_EQ(OLAP_SUCCESS, row_block.find_row(other_cursor, true, &index));
    ASSERT_EQ(uint32_t(1), index);

    ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(3, &other_cursor));
    ASSERT_EQ(OLAP_SUCCESS, row_block.find_row(other_cursor, false, &index));
    ASSERT_EQ(uint32_t(3), index);
    
    ASSERT_EQ(OLAP_SUCCESS, row_block.find_row(other_cursor, true, &index));
    ASSERT_EQ(uint32_t(4), index);
    // 构造一个短key进行测试
    ASSERT_EQ(OLAP_SUCCESS, row_block.find_row(short_cursor, false, &index));
    ASSERT_EQ(uint32_t(0), index);

    ASSERT_EQ(OLAP_SUCCESS, row_block.find_row(short_cursor, true, &index));
    ASSERT_EQ(uint32_t(2), index);

    // 测试clear函数
    ASSERT_EQ(OLAP_SUCCESS, row_block.clear());
    ASSERT_EQ((size_t)8, row_block._info.row_num);
    ASSERT_EQ(size_t(8 * g_row_length), row_block._buf_len);
}

TEST_F(TestRowBlock, test_compress_decompress) {
    RowBlock row_block(_tablet_schema);
    _block_info.row_num = 4;
    _block_info.unpacked_len = 0;
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    ASSERT_EQ(row_block._buf_len, g_row_length * 4);
    LOG(INFO) << "before compression, size is " << g_row_length * 4;
    SetRows(row_block);
    LOG(INFO) << "row block adler32 is " << row_block._info.checksum << endl;
    // 必须先finalize再compress
    ASSERT_EQ(OLAP_SUCCESS, row_block.finalize(4));
    LOG(INFO) << "row block adler32 is " << row_block._info.checksum << endl;
    const size_t buf_len = 100 * 1024;
    char* buf = new char[buf_len];
    size_t written_len = 0;
    ASSERT_EQ(OLAP_SUCCESS, row_block.compress(buf, buf_len, &written_len, 
                OLAP_COMP_STORAGE));
    LOG(INFO) << "after compression, size is " << written_len << endl;

    uint32_t checksum = row_block._info.checksum;
    LOG(INFO) << "row block adler32 is " << row_block._info.checksum << endl;

    // 新建一个rowblock，然后从刚才压出来的数据解压起来
    RowBlock other_row_block(_tablet_schema);
    _block_info.checksum = checksum;
    _block_info.row_num = 4;
    ASSERT_EQ(OLAP_SUCCESS, other_row_block.init(_block_info));
    ASSERT_EQ(other_row_block._buf_len, g_row_length * 4);

    ASSERT_EQ(OLAP_SUCCESS, other_row_block.decompress(buf, 
                written_len, OLAP_COMP_STORAGE));
    // 在两个rowblock里面按行比对数据
    for (size_t i = 0; i < 4; ++i) {
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &cursor));
        ASSERT_EQ(OLAP_SUCCESS, other_row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }

    // 再建一个row_block，从错误的buf中decompress数据，测试异常场景，
    // 分为三种异常场景：1. 待解压buf不正确 2. 内部buflen不够大 3. checksum不正确
    buf[0]++;
    ASSERT_EQ(OLAP_ERR_DECOMPRESS_ERROR, other_row_block.decompress(buf, 
                written_len, OLAP_COMP_STORAGE));
    buf[0]--;

    // 由于变长字符串的缘故，这里没法再检测接出来字符串不够的问题
    // 靠checksum来校验好了, 这里测下内部buf不足的情况
    other_row_block._buf_len--;
    ASSERT_EQ(OLAP_ERR_DECOMPRESS_ERROR, other_row_block.decompress(buf, 
                written_len, OLAP_COMP_STORAGE));
    other_row_block._buf_len++;

    other_row_block._info.checksum++;
    ASSERT_EQ(OLAP_ERR_CHECKSUM_ERROR, other_row_block.decompress(buf, 
                written_len, OLAP_COMP_STORAGE));
    other_row_block._info.checksum--;

    delete[] buf;
}

TEST_F(TestRowBlock, test_crc) {
    unsigned int crc32 = 0xffffffff;
    const char* buf1 = "abcdefg";
    size_t len1 = strlen(buf1);
    unsigned int v1 = crc32c_lut(buf1, 0, len1, crc32);
    LOG(INFO) << "crc32c_lut:" << v1 << endl;
    ASSERT_EQ(v1, 433589182);
    if (1 == check_sse4_2()) {
        unsigned int v2 = baidu_crc32_qw(buf1, crc32, len1);
        LOG(INFO) << "baidu_crc32_qw" << v2 <<endl;
        ASSERT_EQ(v1,v2);
    }

    const char* buf2 = "abcdefghijklmnopqrstuvwxtz";
    size_t len2 = strlen(buf2);
    ASSERT_EQ(crc32c_lut(buf2, 0, len2, crc32),2703567561);
    if (1 == check_sse4_2()) {
        ASSERT_EQ((unsigned int)baidu_crc32_qw(buf2, crc32, len2), 2703567561);
    }
}

class TestRowBlockWithConjuncts : public testing::Test {
public:
    TestRowBlockWithConjuncts() :
        _object_pool(NULL), _runtime_state(NULL), _row_desc(NULL) {
    }
    
    void SetUp() {
        _object_pool = new ObjectPool();
        _runtime_state = _object_pool->add(new RuntimeState(""));

        TDescriptorTable ttbl;
        TTupleDescriptor tuple_desc;
        tuple_desc.__set_id(0);
        tuple_desc.__set_byteSize(8);
        tuple_desc.__set_numNullBytes(0);
        ttbl.tupleDescriptors.push_back(tuple_desc);

        {
            TSlotDescriptor slot_desc;
           
            slot_desc.__set_id(0);
            slot_desc.__set_parent(0);
            // slot_desc.__set_slotType(TPrimitiveType::INT);
            slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::INT));
            slot_desc.__set_columnPos(0);
            slot_desc.__set_byteOffset(4);
            slot_desc.__set_nullIndicatorByte(0);
            slot_desc.__set_nullIndicatorBit(0);
            slot_desc.__set_colName("col1");
            slot_desc.__set_slotIdx(0);
            slot_desc.__set_isMaterialized(true);
            ttbl.slotDescriptors.push_back(slot_desc);
        }

        {
            TSlotDescriptor slot_desc;
            slot_desc.__set_id(1);
            slot_desc.__set_parent(0);
            slot_desc.__set_slotType(gen_type_desc(TPrimitiveType::BIGINT));
            slot_desc.__set_columnPos(1);
            slot_desc.__set_byteOffset(8);
            slot_desc.__set_nullIndicatorByte(0);
            slot_desc.__set_nullIndicatorBit(0);
            slot_desc.__set_colName("col2");
            slot_desc.__set_slotIdx(1);
            slot_desc.__set_isMaterialized(true);
            ttbl.slotDescriptors.push_back(slot_desc);
        }

        DescriptorTbl* desc_tbl = NULL;
        ASSERT_TRUE(DescriptorTbl::create(_object_pool, ttbl, &desc_tbl).ok());
        ASSERT_TRUE(desc_tbl != NULL);
        _runtime_state->set_desc_tbl(desc_tbl);

        std::vector<TTupleId> row_tuples;
        row_tuples.push_back(0);
        std::vector<bool> nullable_tuples;
        nullable_tuples.push_back(false);
        _row_desc = _object_pool->add(
                        new RowDescriptor(*desc_tbl, row_tuples, nullable_tuples));

        make_schema_array(_tablet_schema);
        _block_info.checksum = 12345678;
        _block_info.row_num = 128;
        for (uint32_t i = 0; i < _tablet_schema.size(); i++) {
            _block_info.unpacked_len += _tablet_schema[i].length;
        }
        _block_info.unpacked_len *= _block_info.row_num;
        ASSERT_EQ(OLAP_SUCCESS, cursor.init(_tablet_schema));
        ASSERT_EQ(OLAP_SUCCESS, other_cursor.init(_tablet_schema));
    }

    void TearDown() {
        if (_object_pool != NULL) {
            delete _object_pool;
            _object_pool = NULL;
        }
    }

    Expr* create_expr(int value) {
        TExpr exprs;
        {
            TExprNode expr_node;
            expr_node.__set_node_type(TExprNodeType::BINARY_PRED);
            // TColumnType type;
            // type.__set_type(TPrimitiveType::INT);
            expr_node.__set_type(gen_type_desc(TPrimitiveType::INT));
            expr_node.__set_num_children(2);
            expr_node.__isset.opcode = true;
            // expr_node.__set_opcode(TExprOpcode::LT_INT_INT);
            expr_node.__isset.vector_opcode = true;
            // expr_node.__set_vector_opcode(
            //    TExprOpcode::FILTER_LT_INT_INT);
            exprs.nodes.push_back(expr_node);
        }
        {
            TExprNode expr_node;
            expr_node.__set_node_type(TExprNodeType::SLOT_REF);
            // TColumnType type;
            // type.__set_type(TPrimitiveType::INT);
            expr_node.__set_type(gen_type_desc(TPrimitiveType::INT));
            expr_node.__set_num_children(0);
            expr_node.__isset.slot_ref = true;
            TSlotRef slot_ref;
            slot_ref.__set_slot_id(0);
            slot_ref.__set_tuple_id(0);
            expr_node.__set_slot_ref(slot_ref);
            expr_node.__isset.output_column = true;
            expr_node.__set_output_column(0);
            exprs.nodes.push_back(expr_node);
        }

        {
            TExprNode expr_node;
            expr_node.__set_node_type(TExprNodeType::INT_LITERAL);
            //TColumnType type;
            //type.__set_type(TPrimitiveType::INT);
            expr_node.__set_type(gen_type_desc(TPrimitiveType::INT));
            expr_node.__set_num_children(0);
            expr_node.__isset.int_literal = true;
            TIntLiteral int_literal;
            int_literal.__set_value(value);
            expr_node.__set_int_literal(int_literal);
            exprs.nodes.push_back(expr_node);
        }

        //Expr* root_expr = NULL;
        ExprContext* expr_content = NULL;
        if (Expr::create_expr_tree(_object_pool, exprs, &expr_content).ok()) {
            return expr_content->root();
        } else {
            return NULL;
        }
    }

    void make_schema_array(vector<FieldInfo>& schema_array) {
        schema_array.clear();
        FieldInfo info;
        set_field_info(info, "", OLAP_FIELD_TYPE_INT, OLAP_FIELD_AGGREGATION_NONE, 4, true);
        schema_array.push_back(info);
        set_field_info(info, "", OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_AGGREGATION_SUM, 8, false);
        schema_array.push_back(info);

        g_row_length = sizeof(int) + sizeof(int64_t);
        g_row_length_mysql = g_row_length - 1;
    }

    void set_rows(RowBlock& row_block) {
        vector<string> string_array;
        for (int i = 0; i < _block_info.row_num; ++i) {
            make_row(string_array, i);
            ASSERT_EQ(OLAP_SUCCESS, other_cursor.from_string(string_array));
            ASSERT_EQ(OLAP_SUCCESS, row_block.set_row(i, other_cursor));
        }
    }

    void make_row(vector<string>& string_array, int value) {
        string_array.clear();
        stringstream s;
        s << value;
        string_array.push_back(s.str());
        string_array.push_back(s.str());
    }

    RowCursor cursor;
    RowCursor other_cursor;
    RowCursor short_cursor;
    size_t test_index;
    RowBlockInfo _block_info;
    vector<FieldInfo> _tablet_schema;
    vector<FieldInfo> _tablet_schema_short;
    vector<string> string_array_short;
private:
    ObjectPool* _object_pool;
    RuntimeState* _runtime_state;
    RowDescriptor* _row_desc;
};

TEST_F(TestRowBlockWithConjuncts, simpleTest) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    ASSERT_EQ(row_block._buf_len, g_row_length * _block_info.row_num);
    set_rows(row_block);

    vector<string> string_array;
    for (int i = 0; i < _block_info.row_num; ++i) {
        make_row(string_array, i);
        ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }

    Expr* expr = create_expr(_block_info.row_num + 1);
    ExprContent *expr_content = new ExprContent(expr);
    ASSERT_TRUE(expr != NULL);
    ASSERT_TRUE(expr->prepare(_runtime_state, *_row_desc, expr_content).ok());

    ASSERT_EQ(OLAP_SUCCESS, row_block.eval_conjuncts(expr_content));
    ASSERT_EQ(_block_info.row_num, row_block.row_num());

    for (int i = 0; i < row_block.row_num(); ++i) {
        make_row(string_array, i);
        ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }
}

TEST_F(TestRowBlockWithConjuncts, simpleEvalTest) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    set_rows(row_block);

    Expr* expr = create_expr(_block_info.row_num / 2);
    ExprContent *expr_content = new ExprContext(expr);
    ASSERT_TRUE(expr != NULL);
    ASSERT_TRUE(expr->prepare(_runtime_state, *_row_desc, expr_content).ok());

    // vector<ExprContent*> conjuncts;
   //  conjuncts.push_back(expr_content);
    ASSERT_EQ(OLAP_SUCCESS, row_block.eval_conjuncts(expr_content));
    ASSERT_EQ(_block_info.row_num / 2, row_block.row_num());

    vector<string> string_array;
    for (int i = 0; i < row_block.row_num(); ++i) {
        make_row(string_array, i);
        ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }
}

TEST_F(TestRowBlockWithConjuncts, RestoreTest) {
    RowBlock row_block(_tablet_schema);
    ASSERT_EQ(OLAP_SUCCESS, row_block.init(_block_info));
    set_rows(row_block);
    ASSERT_EQ(_block_info.row_num, row_block.row_num());

    ASSERT_EQ(OLAP_ERR_INPUT_PARAMETER_ERROR, row_block.backup());

    Expr* expr = create_expr(_block_info.row_num / 2);
    ASSERT_TRUE(expr != NULL);
    ASSERT_TRUE(expr->prepare(_runtime_state, *_row_desc).ok());

    vector<Expr*> conjuncts;
    conjuncts.push_back(expr);
    ASSERT_EQ(OLAP_SUCCESS, row_block.eval_conjuncts(conjuncts));
    ASSERT_EQ(_block_info.row_num / 2, row_block.row_num());

    vector<string> string_array;
    for (int i = 0; i < row_block.row_num(); ++i) {
        make_row(string_array, i);
        ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }

    ASSERT_EQ(OLAP_SUCCESS, row_block.backup());

    expr = create_expr(_block_info.row_num / 4);
    ExprContent *expr_content = new ExprContent(expr);
    ASSERT_TRUE(expr != NULL);
    ASSERT_TRUE(expr->prepare(_runtime_state, *_row_desc, expr_content).ok());

    //conjuncts.push_back(expr);
    ASSERT_EQ(OLAP_SUCCESS, row_block.eval_conjuncts(expr_content));
    ASSERT_EQ(_block_info.row_num / 4, row_block.row_num());

    for (int i = 0; i < row_block.row_num(); ++i) {
        make_row(string_array, i);
        ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }

    ASSERT_EQ(OLAP_SUCCESS, row_block.restore());
    ASSERT_EQ(_block_info.row_num / 2, row_block.row_num());

    for (int i = 0; i < row_block.row_num(); ++i) {
        make_row(string_array, i);
        ASSERT_EQ(OLAP_SUCCESS, cursor.from_string(string_array));
        ASSERT_EQ(OLAP_SUCCESS, row_block.get_row_to_read(i, &other_cursor));
        ASSERT_EQ(0, cursor.cmp(other_cursor));
    }
}

}

// @brief Test Stub
int main( int argc, char** argv ) {
    std::string conffile = std::string(getenv("PALO_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    palo::init_glog("be-test");
    int ret = palo::OLAP_SUCCESS;
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
    google::protobuf::ShutdownProtobufLibrary();    
    return ret; 
}
