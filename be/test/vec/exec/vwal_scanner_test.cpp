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
#include "cpp/sync_point.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/fs/local_file_system.h"
#include "olap/wal/wal_manager.h"
#include "pipeline/exec/file_scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "vec/exec/scan/vfile_scanner.h"

namespace doris {

namespace vectorized {

class TestSplitSourceConnector : public SplitSourceConnector {
private:
    std::mutex _range_lock;
    TFileScanRange _scan_range;
    int _range_index = 0;

public:
    TestSplitSourceConnector(const TFileScanRange& scan_range) : _scan_range(scan_range) {}

    Status get_next(bool* has_next, TFileRangeDesc* range) override {
        std::lock_guard<std::mutex> l(_range_lock);
        if (_range_index < _scan_range.ranges.size()) {
            *has_next = true;
            *range = _scan_range.ranges[_range_index++];
        } else {
            *has_next = false;
        }
        return Status::OK();
    }

    int num_scan_ranges() override { return _scan_range.ranges.size(); }

    TFileScanRangeParams* get_params() override { return &_scan_range.params; }
};

class VWalScannerTest : public testing::Test {
public:
    VWalScannerTest() : _runtime_state(TQueryGlobals()), _global_profile("<global profile>") {
        _runtime_state.resize_op_id_to_local_state(-1);
        init();
        _profile = _runtime_state.runtime_profile();
        WARN_IF_ERROR(_runtime_state.init(_unique_id, _query_options, _query_globals, _env),
                      "fail to init _runtime_state");
    }
    void init();
    void generate_scanner(std::shared_ptr<VFileScanner>& scanner);

    void TearDown() override {
        WARN_IF_ERROR(_scan_node->close(&_runtime_state), "fail to close scan_node")
        WARN_IF_ERROR(io::global_local_filesystem()->delete_directory(_wal_dir),
                      fmt::format("fail to delete dir={}", _wal_dir));
        SAFE_STOP(_env->_wal_manager);
    }

protected:
    virtual void SetUp() override {}

private:
    void _init_desc_table();

    ExecEnv* _env = nullptr;
    std::string _wal_dir = std::string(getenv("DORIS_HOME")) + "/wal_test";
    int64_t _db_id = 1;
    int64_t _tb_id = 2;
    int64_t _txn_id_1 = 123;
    int64_t _txn_id_2 = 456;
    uint32_t _version_0 = 0;
    uint32_t _version_1 = 1;
    int64_t _backend_id = 1001;
    std::string _label_1 = "test1";
    std::string _label_2 = "test2";

    TupleId _dst_tuple_id = 0;
    RuntimeState _runtime_state;
    RuntimeProfile _global_profile;
    RuntimeProfile* _profile;
    ObjectPool _obj_pool;
    DescriptorTbl* _desc_tbl;
    std::vector<TNetworkAddress> _addresses;
    ScannerCounter _counter;
    std::vector<TExpr> _pre_filter;
    TPlanNode _tnode;
    TUniqueId _unique_id;
    TQueryOptions _query_options;
    TQueryGlobals _query_globals;
    std::shared_ptr<pipeline::FileScanOperatorX> _scan_node = nullptr;
    std::vector<TFileRangeDesc> _ranges;
    TFileRangeDesc _range_desc;
    TFileScanRange _scan_range;
    std::unique_ptr<ShardedKVCache> _kv_cache = nullptr;
    std::unique_ptr<TMasterInfo> _master_info = nullptr;
};

void VWalScannerTest::_init_desc_table() {
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
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(32);
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
        slot_desc.col_unique_id = 0;
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
            scalar_type.__set_type(TPrimitiveType::VARCHAR);
            scalar_type.__set_len(32);
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
        slot_desc.col_unique_id = 1;
        slot_desc.isMaterialized = true;

        t_desc_table.slotDescriptors.push_back(slot_desc);
    }
    // c3
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
            scalar_type.__set_len(32);
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
        slot_desc.col_unique_id = 2;
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

    auto st = DescriptorTbl::create(&_obj_pool, t_desc_table, &_desc_tbl);

    _runtime_state.set_desc_tbl(_desc_tbl);
}

void VWalScannerTest::init() {
    config::group_commit_wal_max_disk_limit = "100M";
    _init_desc_table();
    WARN_IF_ERROR(io::global_local_filesystem()->create_directory(
                          _wal_dir + "/" + std::to_string(_db_id) + "/" + std::to_string(_tb_id)),
                  "fail to creat directory");

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::FILE_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.nullable_tuples.push_back(false);
    _tnode.file_scan_node.tuple_id = 0;
    _tnode.__isset.file_scan_node = true;

    _scan_node =
            std::make_shared<pipeline::FileScanOperatorX>(&_obj_pool, _tnode, 0, *_desc_tbl, 1);
    _scan_node->_output_tuple_desc = _runtime_state.desc_tbl().get_tuple_descriptor(_dst_tuple_id);
    WARN_IF_ERROR(_scan_node->init(_tnode, &_runtime_state), "fail to init scan_node");
    WARN_IF_ERROR(_scan_node->prepare(&_runtime_state), "fail to prepare scan_node");

    auto local_state =
            pipeline::FileScanLocalState::create_unique(&_runtime_state, _scan_node.get());
    std::vector<TScanRangeParams> scan_ranges;
    std::map<int, std::pair<std::shared_ptr<pipeline::LocalExchangeSharedState>,
                            std::shared_ptr<pipeline::Dependency>>>
            le_state_map;
    pipeline::LocalStateInfo info {&_global_profile, scan_ranges, nullptr, le_state_map, 0};
    WARN_IF_ERROR(local_state->init(&_runtime_state, info), "fail to init local_state");
    _runtime_state.emplace_local_state(_scan_node->operator_id(), std::move(local_state));

    _range_desc.start_offset = 0;
    _range_desc.size = 1000;
    _ranges.push_back(_range_desc);
    _scan_range.ranges = _ranges;
    _scan_range.__isset.params = true;
    _scan_range.params.format_type = TFileFormatType::FORMAT_WAL;
    _kv_cache.reset(new ShardedKVCache(48));

    _master_info.reset(new TMasterInfo());
    _env = ExecEnv::GetInstance();
    _env->_master_info = _master_info.get();
    _env->_master_info->network_address.hostname = "host name";
    _env->_master_info->network_address.port = _backend_id;
    _env->_master_info->backend_id = 1001;
    _env->_wal_manager = WalManager::create_shared(_env, _wal_dir);
    std::string base_path;
    auto st = _env->_wal_manager->_init_wal_dirs_info();
    st = _env->_wal_manager->create_wal_path(_db_id, _tb_id, _txn_id_1, _label_1, base_path,
                                             _version_0);
    std::string src = "./be/test/exec/test_data/wal_scanner/wal_version0";
    std::string dst = _wal_dir + "/" + std::to_string(_db_id) + "/" + std::to_string(_tb_id) + "/" +
                      std::to_string(_version_0) + "_" + std::to_string(_backend_id) + "_" +
                      std::to_string(_txn_id_1) + "_" + _label_1;
    std::filesystem::copy(src, dst);
    st = _env->_wal_manager->create_wal_path(_db_id, _tb_id, _txn_id_2, _label_2, base_path,
                                             _version_1);
    src = "./be/test/exec/test_data/wal_scanner/wal_version1";
    dst = _wal_dir + "/" + std::to_string(_db_id) + "/" + std::to_string(_tb_id) + "/" +
          std::to_string(_version_1) + "_" + std::to_string(_backend_id) + "_" +
          std::to_string(_txn_id_2) + "_" + _label_2;
    std::filesystem::copy(src, dst);
}

void VWalScannerTest::generate_scanner(std::shared_ptr<VFileScanner>& scanner) {
    auto split_source = std::make_shared<TestSplitSourceConnector>(_scan_range);
    scanner = std::make_shared<VFileScanner>(
            &_runtime_state,
            &(_runtime_state.get_local_state(0)->cast<pipeline::FileScanLocalState>()), -1,
            split_source, _profile, _kv_cache.get());
    scanner->_is_load = false;
    vectorized::VExprContextSPtrs _conjuncts;
    std::unordered_map<std::string, ColumnValueRangeType> _colname_to_value_range;
    std::unordered_map<std::string, int> _colname_to_slot_id;
    WARN_IF_ERROR(scanner->prepare(_conjuncts, &_colname_to_value_range, &_colname_to_slot_id),
                  "fail to prepare scanner");
}

TEST_F(VWalScannerTest, normal) {
    // read wal file with wal_version=0
    _runtime_state._wal_id = _txn_id_1;
    std::shared_ptr<VFileScanner> scanner = nullptr;
    generate_scanner(scanner);
    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    auto st = scanner->get_block(&_runtime_state, block.get(), &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(3, block->rows());
    block->clear();
    st = scanner->get_block(&_runtime_state, block.get(), &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(0, block->rows());
    ASSERT_TRUE(eof);
    WARN_IF_ERROR(scanner->close(&_runtime_state), "fail to close scanner");
    // read wal file with wal_version=1
    eof = false;
    _runtime_state._wal_id = _txn_id_2;
    generate_scanner(scanner);
    st = scanner->get_block(&_runtime_state, block.get(), &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(3, block->rows());
    block->clear();
    st = scanner->get_block(&_runtime_state, block.get(), &eof);
    ASSERT_TRUE(st.ok());
    EXPECT_EQ(0, block->rows());
    ASSERT_TRUE(eof);
    WARN_IF_ERROR(scanner->close(&_runtime_state), "fail to close scanner");
}

TEST_F(VWalScannerTest, fail_with_not_equal) {
    auto sp = SyncPoint::get_instance();
    SyncPoint::CallbackGuard guard1;
    sp->set_call_back(
            "WalReader::set_column_id_count",
            [](auto&& args) { *try_any_cast<int64_t*>(args[0]) = 2; }, &guard1);
    SyncPoint::CallbackGuard guard2;
    sp->set_call_back(
            "WalReader::set_out_block_column_size",
            [](auto&& args) { *try_any_cast<size_t*>(args[0]) = 2; }, &guard2);
    sp->enable_processing();

    _runtime_state._wal_id = _txn_id_1;
    std::shared_ptr<VFileScanner> scanner = nullptr;
    generate_scanner(scanner);
    std::unique_ptr<vectorized::Block> block(new vectorized::Block());
    bool eof = false;
    auto st = scanner->get_block(&_runtime_state, block.get(), &eof);
    ASSERT_FALSE(st.ok());
    auto msg = st.to_string();
    auto pos = msg.find("not equal");
    ASSERT_TRUE(pos != msg.npos);
    WARN_IF_ERROR(scanner->close(&_runtime_state), "fail to close scanner");
}

} // namespace vectorized
} // namespace doris
