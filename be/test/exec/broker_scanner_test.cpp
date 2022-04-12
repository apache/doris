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

#include "exec/broker_scanner.h"

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"

namespace doris {

class BrokerScannerTest : public testing::Test {
public:
    BrokerScannerTest() : _tracker(new MemTracker()), _runtime_state(TQueryGlobals()) {
        init();
        _profile = _runtime_state.runtime_profile();
        _runtime_state._instance_mem_tracker.reset(new MemTracker());
    }
    void init();

    static void SetUpTestCase() {
        UserFunctionCache::instance()->init(
                "./be/test/runtime/test_data/user_function_cache/normal");
        CastFunctions::init();
    }

protected:
    virtual void SetUp() {}
    virtual void TearDown() {}

private:
    void init_desc_table();
    void init_params();

    std::shared_ptr<MemTracker> _tracker;
    RuntimeState _runtime_state;
    RuntimeProfile* _profile;
    ObjectPool _obj_pool;
    std::map<std::string, SlotDescriptor*> _slots_map;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    std::vector<TNetworkAddress> _addresses;
    ScannerCounter _counter;
    std::vector<TExpr> _pre_filter;
};

void BrokerScannerTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::MYSQL_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;
    // TSlotDescriptor
    // int offset = 1;
    // int i = 0;
    // k1
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = 0;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k1";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k2
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 4;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k2";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k3
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::INT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 8;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 0;
        t_tuple_desc.byteSize = 12;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }

    // source tuple descriptor
    // TSlotDescriptor
    // int offset = 1;
    // int i = 0;
    // k1
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = 0;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k1";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k2
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 16;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k2";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // k3
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 1;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 32;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "k3";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = 1;
        t_tuple_desc.byteSize = 48;
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void BrokerScannerTest::init_params() {
    _params.column_separator = ',';
    _params.line_delimiter = '\n';

    TTypeDesc int_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::INT);
        node.__set_scalar_type(scalar_type);
        int_type.types.push_back(node);
    }
    TTypeDesc varchar_type;
    {
        TTypeNode node;
        node.__set_type(TTypeNodeType::SCALAR);
        TScalarType scalar_type;
        scalar_type.__set_type(TPrimitiveType::VARCHAR);
        scalar_type.__set_len(5000);
        node.__set_scalar_type(scalar_type);
        varchar_type.types.push_back(node);
    }

    for (int i = 0; i < 3; ++i) {
        TExprNode cast_expr;
        cast_expr.node_type = TExprNodeType::CAST_EXPR;
        cast_expr.type = int_type;
        cast_expr.__set_opcode(TExprOpcode::CAST);
        cast_expr.__set_num_children(1);
        cast_expr.__set_output_scale(-1);
        cast_expr.__isset.fn = true;
        cast_expr.fn.name.function_name = "casttoint";
        cast_expr.fn.binary_type = TFunctionBinaryType::BUILTIN;
        cast_expr.fn.arg_types.push_back(varchar_type);
        cast_expr.fn.ret_type = int_type;
        cast_expr.fn.has_var_args = false;
        cast_expr.fn.__set_signature("casttoint(VARCHAR(*))");
        cast_expr.fn.__isset.scalar_fn = true;
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = 4 + i;
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(i + 1, expr);
        _params.src_slot_ids.push_back(4 + i);
    }
    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(0);
    _params.__set_src_tuple_id(1);
}

void BrokerScannerTest::init() {
    init_desc_table();
    init_params();
}

TEST_F(BrokerScannerTest, normal) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 0;
    range.size = -1;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 1,2,3
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(1, *(int*)tuple->get_slot(0));
    EXPECT_EQ(2, *(int*)tuple->get_slot(4));
    EXPECT_EQ(3, *(int*)tuple->get_slot(8));

    // 4,5,6
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(4, *(int*)tuple->get_slot(0));
    EXPECT_EQ(5, *(int*)tuple->get_slot(4));
    EXPECT_EQ(6, *(int*)tuple->get_slot(8));

    // 7, 8, unqualitifed
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_FALSE(fill_tuple);

    // 8,9,10
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(8, *(int*)tuple->get_slot(0));
    EXPECT_EQ(9, *(int*)tuple->get_slot(4));
    EXPECT_EQ(10, *(int*)tuple->get_slot(8));
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal2) {
    std::vector<TBrokerRangeDesc> ranges;

    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal2_1.csv";
    range.start_offset = 0;
    range.size = 7;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    range.path = "./be/test/exec/test_data/broker_scanner/normal2_2.csv";
    range.start_offset = 0;
    range.size = 4;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 1,2,3
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(1, *(int*)tuple->get_slot(0));
    EXPECT_EQ(2, *(int*)tuple->get_slot(4));
    EXPECT_EQ(3, *(int*)tuple->get_slot(8));

    // 3,4,5
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_TRUE(fill_tuple);
    EXPECT_EQ(3, *(int*)tuple->get_slot(0));
    EXPECT_EQ(4, *(int*)tuple->get_slot(4));
    EXPECT_EQ(5, *(int*)tuple->get_slot(8));

    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(fill_tuple);
    EXPECT_FALSE(eof);

    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(fill_tuple);
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal3) {
    std::vector<TBrokerRangeDesc> ranges;

    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal2_1.csv";
    range.start_offset = 0;
    range.size = 7;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    range.path = "./be/test/exec/test_data/broker_scanner/normal2_2.csv";
    range.start_offset = 0;
    range.size = 5;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 1,2,3
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(1, *(int*)tuple->get_slot(0));
    EXPECT_EQ(2, *(int*)tuple->get_slot(4));
    EXPECT_EQ(3, *(int*)tuple->get_slot(8));

    // 3,4,5
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_TRUE(fill_tuple);
    EXPECT_EQ(3, *(int*)tuple->get_slot(0));
    EXPECT_EQ(4, *(int*)tuple->get_slot(4));
    EXPECT_EQ(5, *(int*)tuple->get_slot(8));

    // first line of normal2_2.csv is 2,3, which is unqualified
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_FALSE(fill_tuple);

    // 4,5,6
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(4, *(int*)tuple->get_slot(0));
    EXPECT_EQ(5, *(int*)tuple->get_slot(4));
    EXPECT_EQ(6, *(int*)tuple->get_slot(8));

    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal4) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 0;
    range.size = 7;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 1,2,3
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(1, *(int*)tuple->get_slot(0));
    EXPECT_EQ(2, *(int*)tuple->get_slot(4));
    EXPECT_EQ(3, *(int*)tuple->get_slot(8));
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal5) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 0;
    range.size = 0;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal6) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 1;
    range.size = 7;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 4,5,6
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(4, *(int*)tuple->get_slot(0));
    EXPECT_EQ(5, *(int*)tuple->get_slot(4));
    EXPECT_EQ(6, *(int*)tuple->get_slot(8));
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal7) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 1;
    range.size = 6;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal8) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 7;
    range.size = 1;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 4,5,6
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(4, *(int*)tuple->get_slot(0));
    EXPECT_EQ(5, *(int*)tuple->get_slot(4));
    EXPECT_EQ(6, *(int*)tuple->get_slot(8));
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, normal9) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/normal.csv";
    range.start_offset = 8;
    range.size = 1;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

TEST_F(BrokerScannerTest, multi_bytes_1) {
    std::vector<TBrokerRangeDesc> ranges;
    TBrokerRangeDesc range;
    range.path = "./be/test/exec/test_data/broker_scanner/multi_bytes_sep.csv";
    range.start_offset = 0;
    range.size = 18;
    range.splittable = true;
    range.file_type = TFileType::FILE_LOCAL;
    range.format_type = TFileFormatType::FORMAT_CSV_PLAIN;
    ranges.push_back(range);

    _params.column_separator_str = "AAAA";
    _params.line_delimiter_str = "BB";
    _params.column_separator_length = 4;
    _params.line_delimiter_length = 2;
    BrokerScanner scanner(&_runtime_state, _profile, _params, ranges, _addresses, _pre_filter,
                          &_counter);
    auto st = scanner.open();
    EXPECT_TRUE(st.ok());

    MemPool tuple_pool(_tracker.get());
    Tuple* tuple = (Tuple*)tuple_pool.allocate(20);
    bool fill_tuple;
    bool eof = false;
    // 4,5,6
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(4, *(int*)tuple->get_slot(0));
    EXPECT_EQ(5, *(int*)tuple->get_slot(4));
    EXPECT_EQ(6, *(int*)tuple->get_slot(8));
    // 1,2,3
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_FALSE(eof);
    EXPECT_EQ(1, *(int*)tuple->get_slot(0));
    EXPECT_EQ(2, *(int*)tuple->get_slot(4));
    EXPECT_EQ(3, *(int*)tuple->get_slot(8));
    // end of file
    st = scanner.get_next(tuple, &tuple_pool, &eof, &fill_tuple);
    EXPECT_TRUE(st.ok());
    EXPECT_TRUE(eof);
}

} // end namespace doris
