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

#include <string>
#include <vector>

#include "common/object_pool.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/wal_manager.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "vec/exec/scan/new_file_scan_node.h"
#include "vec/exec/scan/vfile_scanner.h"

namespace doris {

namespace vectorized {
class VWalScannerTest : public testing::Test {
public:
    VWalScannerTest() : _runtime_state(TQueryGlobals()) {
        init();
        _profile = _runtime_state.runtime_profile();
        _runtime_state.init_mem_trackers();
        static_cast<void>(_runtime_state.init(unique_id, query_options, query_globals, _env));
        _runtime_state.set_query_ctx(query_ctx);
    }
    void init();

    void TearDown() override {
        static_cast<void>(io::global_local_filesystem()->delete_directory(wal_dir));
        SAFE_STOP(_env->_wal_manager);
    }

protected:
    virtual void SetUp() override {}

private:
    void init_desc_table();

    ExecEnv* _env = nullptr;
    std::string wal_dir = "./wal_test";
    int64_t db_id = 1;
    int64_t tb_id = 2;
    int64_t txn_id = 789;
    std::string label = "test";

    TupleId _dst_tuple_id = 0;
    RuntimeState _runtime_state;
    RuntimeProfile* _profile;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    std::vector<TNetworkAddress> _addresses;
    ScannerCounter _counter;
    std::vector<TExpr> _pre_filter;
    TPlanNode _tnode;
    TUniqueId unique_id;
    TQueryOptions query_options;
    TQueryGlobals query_globals;
    QueryContext* query_ctx = nullptr;
};

void VWalScannerTest::init_desc_table() {
    TDescriptorTable t_desc_table;

    // table descriptors
    TTableDescriptor t_table_desc;

    t_table_desc.id = 0;
    t_table_desc.tableType = TTableType::OLAP_TABLE;
    t_table_desc.numCols = 0;
    t_table_desc.numClusteringCols = 0;
    t_desc_table.tableDescriptors.push_back(t_table_desc);
    t_desc_table.__isset.tableDescriptors = true;

    int next_slot_id = 1;
    // TSlotDescriptor
    // int offset = 1;
    // int i = 0;
    // c1
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
        slot_desc.colName = "c1";
        slot_desc.slotIdx = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // c2
    {
        TSlotDescriptor slot_desc;

        slot_desc.id = next_slot_id++;
        slot_desc.parent = 0;
        TTypeDesc type;
        {
            TTypeNode node;
            node.__set_type(TTypeNodeType::SCALAR);
            TScalarType scalar_type;
            scalar_type.__set_type(TPrimitiveType::BIGINT);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 1;
        slot_desc.byteOffset = 4;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "c2";
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
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(10);
            node.__set_scalar_type(scalar_type);
            type.types.push_back(node);
        }
        slot_desc.slotType = type;
        slot_desc.columnPos = 2;
        slot_desc.byteOffset = 8;
        slot_desc.nullIndicatorByte = 0;
        slot_desc.nullIndicatorBit = -1;
        slot_desc.colName = "c3";
        slot_desc.slotIdx = 3;
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

    static_cast<void>(DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl));

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void VWalScannerTest::init() {
    init_desc_table();
    static_cast<void>(io::global_local_filesystem()->create_directory(
            wal_dir + "/" + std::to_string(db_id) + "/" + std::to_string(tb_id)));
    std::string src = "./be/test/exec/test_data/wal_scanner/wal";
    std::string dst = wal_dir + "/" + std::to_string(db_id) + "/" + std::to_string(tb_id) + "/" +
                      std::to_string(txn_id) + "_" + label;
    std::filesystem::copy(src, dst);

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::FILE_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.file_scan_node.tuple_id = 0;
    _tnode.__isset.file_scan_node = true;

    _env = ExecEnv::GetInstance();
    _env->_wal_manager = WalManager::create_shared(_env, wal_dir);
    static_cast<void>(_env->_wal_manager->add_wal_path(db_id, tb_id, txn_id, label));
}

TEST_F(VWalScannerTest, normal) {
    //    config::group_commit_replay_wal_dir = wal_dir;
    NewFileScanNode scan_node(&_obj_pool, _tnode, *_desc_tbl);
    scan_node._output_tuple_desc = _runtime_state.desc_tbl().get_tuple_descriptor(_dst_tuple_id);
    static_cast<void>(scan_node.init(_tnode, &_runtime_state));
    auto status = scan_node.prepare(&_runtime_state);
    EXPECT_TRUE(status.ok());

    std::vector<TFileRangeDesc> ranges;
    TFileRangeDesc range_desc;
    {
        range_desc.start_offset = 0;
        range_desc.size = 1000;
    }
    ranges.push_back(range_desc);
    TFileScanRange scan_range;
    scan_range.ranges = ranges;
    scan_range.__isset.params = true;
    scan_range.params.format_type = TFileFormatType::FORMAT_WAL;
    std::unique_ptr<ShardedKVCache> _kv_cache;
    _kv_cache.reset(new ShardedKVCache(48));
    _runtime_state._wal_id = txn_id;
    VFileScanner scanner(&_runtime_state, &scan_node, -1, scan_range, _profile, _kv_cache.get());
    vectorized::VExprContextSPtrs _conjuncts;
    std::unordered_map<std::string, ColumnValueRangeType> _colname_to_value_range;
    std::unordered_map<std::string, int> _colname_to_slot_id;
    static_cast<void>(scanner.prepare(_conjuncts, &_colname_to_value_range, &_colname_to_slot_id));

    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    auto st = scanner.get_block(&_runtime_state, block.get(), &eof);
    EXPECT_EQ(3, block->rows());
    ASSERT_TRUE(st.ok());
    block->clear();
    st = scanner.get_block(&_runtime_state, block.get(), &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(0, block->rows());
    ASSERT_TRUE(eof);
    static_cast<void>(scanner.close(&_runtime_state));
    static_cast<void>(scan_node.close(&_runtime_state));

    {
        std::stringstream ss;
        scan_node.runtime_profile()->pretty_print(&ss);
        LOG(INFO) << ss.str();
    }
}

} // namespace vectorized
} // namespace doris
