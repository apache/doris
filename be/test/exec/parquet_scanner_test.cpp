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
#include <time.h>

#include <map>
#include <string>
#include <vector>

#include "common/object_pool.h"
#include "exec/broker_scan_node.h"
#include "exec/local_file_reader.h"
#include "exprs/cast_functions.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/user_function_cache.h"

namespace doris {

class ParquetScannerTest : public testing::Test {
public:
    ParquetScannerTest() : _runtime_state(TQueryGlobals()) {
        init();
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
    int create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    int create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id);
    void create_expr_info();
    void init_desc_table();
    RuntimeState _runtime_state;
    ObjectPool _obj_pool;
    std::map<std::string, SlotDescriptor*> _slots_map;
    TBrokerScanRangeParams _params;
    DescriptorTbl* _desc_tbl;
    TPlanNode _tnode;
};

#define TUPLE_ID_DST 0
#define TUPLE_ID_SRC 1
#define COLUMN_NUMBERS 20
#define DST_TUPLE_SLOT_ID_START 1
#define SRC_TUPLE_SLOT_ID_START 21
int ParquetScannerTest::create_src_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    const char* columnNames[] = {
            "log_version",       "log_time", "log_time_stamp", "js_version",
            "vst_cookie",        "vst_ip",   "vst_user_id",    "vst_user_agent",
            "device_resolution", "page_url", "page_refer_url", "page_yyid",
            "page_type",         "pos_type", "content_id",     "media_id",
            "spm_cnt",           "spm_pre",  "scm_cnt",        "partition_column"};
    for (int i = 0; i < COLUMN_NUMBERS; i++) {
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
        slot_desc.columnPos = i;
        // Skip the first 8 bytes These 8 bytes are used to indicate whether the field is a null value
        slot_desc.byteOffset = i * 16 + 8; 
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = columnNames[i];
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    {
        // TTupleDescriptor source
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_SRC;
        //Here 8 bytes in order to handle null values
        t_tuple_desc.byteSize = COLUMN_NUMBERS * 16 + 8; 
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

int ParquetScannerTest::create_dst_tuple(TDescriptorTable& t_desc_table, int next_slot_id) {
    int32_t byteOffset = 8; // Skip the first 8 bytes These 8 bytes are used to indicate whether the field is a null value
    {                       //log_version
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR); //parquet::Type::BYTE
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 0;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 0;
        slot_desc.colName = "log_version";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 16;
    { // log_time
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT); //parquet::Type::INT64
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 1;
        slot_desc.colName = "log_time";
        slot_desc.slotIdx = 2;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 8;
    { // log_time_stamp
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT); //parquet::Type::INT32
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 2;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = 2;
        slot_desc.colName = "log_time_stamp";
        slot_desc.slotIdx = 3;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    byteOffset += 8;
    const char* columnNames[] = {
            "log_version",       "log_time", "log_time_stamp", "js_version",
            "vst_cookie",        "vst_ip",   "vst_user_id",    "vst_user_agent",
            "device_resolution", "page_url", "page_refer_url", "page_yyid",
            "page_type",         "pos_type", "content_id",     "media_id",
            "spm_cnt",           "spm_pre",  "scm_cnt",        "partition_column"};
    for (int i = 3; i < COLUMN_NUMBERS; i++, byteOffset += 16) {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::VARCHAR); //parquet::Type::BYTE
            scalar_type.__set_len(65535);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = i;
        slot_desc.byteOffset = byteOffset;
        slot_desc.nullIndicatorByte = i / 8;
        slot_desc.nullIndicatorBit = i % 8;
        slot_desc.colName = columnNames[i];
        slot_desc.slotIdx = i + 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }

    t_desc_table.__isset.slotDescriptors = true;
    {
        // TTupleDescriptor dest
        TTupleDescriptor t_tuple_desc;
        t_tuple_desc.id = TUPLE_ID_DST;
        t_tuple_desc.byteSize = byteOffset + 8; //Here 8 bytes in order to handle null values
        t_tuple_desc.numNullBytes = 0;
        t_tuple_desc.tableId = 0;
        t_tuple_desc.__isset.tableId = true;
        t_desc_table.tupleDescriptors.push_back(t_tuple_desc);
    }
    return next_slot_id;
}

void ParquetScannerTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::BROKER_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;

    next_slot_id = create_dst_tuple(t_desc_table, next_slot_id);

    next_slot_id = create_src_tuple(t_desc_table, next_slot_id);

    DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void ParquetScannerTest::create_expr_info() {
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
    // log_version VARCHAR --> VARCHAR
    {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START);
    }
    // log_time VARCHAR --> BIGINT
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
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
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_big_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 1; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 1, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 1);
    }
    // log_time_stamp VARCHAR --> BIGINT
    {
        TTypeDesc int_type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            int_type.types.push_back(node);
        }
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
        cast_expr.fn.scalar_fn.symbol = "doris::CastFunctions::cast_to_big_int_val";

        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + 2;
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(cast_expr);
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + 2, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + 2);
    }
    // couldn't convert type
    for (int i = 3; i < COLUMN_NUMBERS; i++) {
        TExprNode slot_ref;
        slot_ref.node_type = TExprNodeType::SLOT_REF;
        slot_ref.type = varchar_type;
        slot_ref.num_children = 0;
        slot_ref.__isset.slot_ref = true;
        slot_ref.slot_ref.slot_id = SRC_TUPLE_SLOT_ID_START + i; // log_time id in src tuple
        slot_ref.slot_ref.tuple_id = 1;

        TExpr expr;
        expr.nodes.push_back(slot_ref);

        _params.expr_of_dest_slot.emplace(DST_TUPLE_SLOT_ID_START + i, expr);
        _params.src_slot_ids.push_back(SRC_TUPLE_SLOT_ID_START + i);
    }

    // _params.__isset.expr_of_dest_slot = true;
    _params.__set_dest_tuple_id(TUPLE_ID_DST);
    _params.__set_src_tuple_id(TUPLE_ID_SRC);
}

void ParquetScannerTest::init() {
    create_expr_info();
    init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::SCHEMA_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.broker_scan_node.tuple_id = 0;
    _tnode.__isset.broker_scan_node = true;
}

TEST_F(ParquetScannerTest, normal) {
    BrokerScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node.init(_tnode);
    auto status = scan_node.prepare(&_runtime_state);
    ASSERT_TRUE(status.ok());

    // set scan range
    std::vector<TScanRangeParams> scan_ranges;
    {
        TScanRangeParams scan_range_params;

        TBrokerScanRange broker_scan_range;
        broker_scan_range.params = _params;
        TBrokerRangeDesc range;
        range.start_offset = 0;
        range.size = -1;
        range.format_type = TFileFormatType::FORMAT_PARQUET;
        range.splittable = true;

        std::vector<std::string> columns_from_path{"value"};
        range.__set_columns_from_path(columns_from_path);
        range.__set_num_of_columns_from_file(19);
#if 1
        range.path = "./be/test/exec/test_data/parquet_scanner/localfile.parquet";
        range.file_type = TFileType::FILE_LOCAL;
#else
        range.path = "hdfs://ip:8020/user/xxxx.parq";
        range.file_type = TFileType::FILE_BROKER;
        TNetworkAddress addr;
        addr.__set_hostname("127.0.0.1");
        addr.__set_port(8000);
        broker_scan_range.broker_addresses.push_back(addr);
#endif
        broker_scan_range.ranges.push_back(range);
        scan_range_params.scan_range.__set_broker_scan_range(broker_scan_range);
        scan_ranges.push_back(scan_range_params);
    }

    scan_node.set_scan_ranges(scan_ranges);
    status = scan_node.open(&_runtime_state);
    ASSERT_TRUE(status.ok());

    // Get batch
    RowBatch batch(scan_node.row_desc(), _runtime_state.batch_size());
    bool eof = false;
    for (int i = 0; i < 14; i++) {
        status = scan_node.get_next(&_runtime_state, &batch, &eof);
        ASSERT_TRUE(status.ok());
        ASSERT_EQ(2048, batch.num_rows());
        ASSERT_FALSE(eof);
        batch.reset();
    }

    status = scan_node.get_next(&_runtime_state, &batch, &eof);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(1328, batch.num_rows());
    ASSERT_FALSE(eof);
    batch.reset();
    status = scan_node.get_next(&_runtime_state, &batch, &eof);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(0, batch.num_rows());
    ASSERT_TRUE(eof);

    scan_node.close(&_runtime_state);
    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
}

} // namespace doris

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    return RUN_ALL_TESTS();
}
