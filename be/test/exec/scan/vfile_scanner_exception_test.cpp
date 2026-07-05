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
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "cpp/sync_point.h"
#include "exec/operator/file_scan_operator.h"
#include "exec/scan/file_scanner.h"
#include "exec/scan/file_scanner_counter_helper.h"
#include "exec/scan/file_scanner_v2.h"
#include "exec/scan/split_source_connector.h"
#include "format/generic_reader.h"
#include "format_v2/table/hive_reader.h"
#include "format_v2/table_reader.h"
#include "io/fs/local_file_system.h"
#include "load/group_commit/wal/wal_manager.h"
#include "runtime/cluster_info.h"
#include "runtime/descriptors.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "runtime/user_function_cache.h"
#include "testutil/mock/mock_query_context.h"

namespace doris {

namespace {

// Reuse Doris test query context to provide a ready-to-use ResourceContext in UT.
std::shared_ptr<QueryContext> create_ut_query_context() {
    return MockQueryContext::create();
}

} // namespace

class TestSplitSourceConnectorStub : public SplitSourceConnector {
private:
    std::mutex _range_lock;
    TFileScanRange _scan_range;
    int _range_index = 0;

public:
    TestSplitSourceConnectorStub(const TFileScanRange& scan_range) : _scan_range(scan_range) {}

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

struct FakeTableReaderStep {
    int64_t total_read_bytes = 0;
    int64_t total_cache_local_bytes = 0;
    int64_t total_cache_remote_bytes = 0;
    bool eof = false;
};

class FakeTableReader final : public format::TableReader {
public:
    FakeTableReader(io::FileReaderStats* file_reader_stats,
                    io::FileCacheStatistics* file_cache_statistics,
                    std::vector<FakeTableReaderStep> steps)
            : _file_reader_stats(file_reader_stats),
              _file_cache_statistics(file_cache_statistics),
              _steps(std::move(steps)) {}

    Status prepare_split(const format::SplitReadOptions& options) override {
        _prepared_ranges.push_back(options.current_range.path);
        return Status::OK();
    }

    Status get_block(Block* block, bool* eos) override {
        static_cast<void>(block);
        DORIS_CHECK_LT(_next_step, _steps.size());
        const auto& step = _steps[_next_step++];
        _file_reader_stats->read_bytes = step.total_read_bytes;
        _file_cache_statistics->bytes_read_from_local = step.total_cache_local_bytes;
        _file_cache_statistics->bytes_read_from_remote = step.total_cache_remote_bytes;
        *eos = step.eof;
        return Status::OK();
    }

    Status close() override { return Status::OK(); }

    const std::vector<std::string>& prepared_ranges() const { return _prepared_ranges; }

private:
    io::FileReaderStats* _file_reader_stats;
    io::FileCacheStatistics* _file_cache_statistics;
    std::vector<FakeTableReaderStep> _steps;
    size_t _next_step = 0;
    std::vector<std::string> _prepared_ranges;
};

class FakeGenericReader final : public GenericReader {
public:
    FakeGenericReader(io::FileCacheStatistics* file_cache_statistics,
                      int64_t close_cache_local_bytes, int64_t close_cache_remote_bytes)
            : _file_cache_statistics(file_cache_statistics),
              _close_cache_local_bytes(close_cache_local_bytes),
              _close_cache_remote_bytes(close_cache_remote_bytes) {}

    Status close() override {
        _file_cache_statistics->bytes_read_from_local += _close_cache_local_bytes;
        _file_cache_statistics->bytes_read_from_remote += _close_cache_remote_bytes;
        return Status::OK();
    }

protected:
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override {
        static_cast<void>(block);
        *read_rows = 0;
        *eof = true;
        return Status::OK();
    }

private:
    io::FileCacheStatistics* _file_cache_statistics;
    int64_t _close_cache_local_bytes;
    int64_t _close_cache_remote_bytes;
};

class VfileScannerExceptionTest : public testing::Test {
public:
    VfileScannerExceptionTest()
            : _query_ctx(create_ut_query_context()),
              _runtime_state(TUniqueId(), 0, TQueryOptions(), TQueryGlobals(),
                             ExecEnv::GetInstance(), _query_ctx.get()),
              _global_profile("<global profile>") {
        _runtime_state.resize_op_id_to_local_state(-1);
        init();
        _profile = _runtime_state.runtime_profile();
        WARN_IF_ERROR(_runtime_state.init(_unique_id, _query_options, _query_globals, _env),
                      "fail to init _runtime_state");
    }
    void init();
    void generate_scanner(std::shared_ptr<FileScanner>& scanner);
    void generate_scanner_v2(std::shared_ptr<FileScannerV2>& scanner);

    void TearDown() override {
        WARN_IF_ERROR(_scan_node->close(&_runtime_state), "fail to close scan_node")
    }

protected:
    virtual void SetUp() override {}

private:
    void _init_desc_table();

    ExecEnv* _env = nullptr;
    int64_t _backend_id = 1001;
    std::string _label_1 = "test1";
    std::string _label_2 = "test2";

    TupleId _dst_tuple_id = 0;
    // Keep the query context alive for the lifetime of RuntimeState in this test fixture.
    std::shared_ptr<QueryContext> _query_ctx;
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
    std::shared_ptr<FileScanOperatorX> _scan_node = nullptr;
    std::vector<TFileRangeDesc> _ranges;
    TFileRangeDesc _range_desc;
    TFileScanRange _scan_range;
    std::unique_ptr<ShardedKVCache> _kv_cache = nullptr;
    std::unique_ptr<ClusterInfo> _cluster_info = nullptr;
};

void VfileScannerExceptionTest::_init_desc_table() {
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

void VfileScannerExceptionTest::init() {
    _init_desc_table();

    // Node Id
    _tnode.node_id = 0;
    _tnode.node_type = TPlanNodeType::FILE_SCAN_NODE;
    _tnode.num_children = 0;
    _tnode.limit = -1;
    _tnode.row_tuples.push_back(0);
    _tnode.file_scan_node.tuple_id = 0;
    _tnode.__isset.file_scan_node = true;

    _scan_node = std::make_shared<FileScanOperatorX>(&_obj_pool, _tnode, 0, *_desc_tbl, 1);
    _scan_node->_output_tuple_desc = _runtime_state.desc_tbl().get_tuple_descriptor(_dst_tuple_id);
    WARN_IF_ERROR(_scan_node->init(_tnode, &_runtime_state), "fail to init scan_node");
    WARN_IF_ERROR(_scan_node->prepare(&_runtime_state), "fail to open scan_node");

    auto local_state = FileScanLocalState::create_unique(&_runtime_state, _scan_node.get());
    std::vector<TScanRangeParams> scan_ranges;
    LocalStateInfo info {.parent_profile = &_global_profile,
                         .scan_ranges = scan_ranges,
                         .shared_state = nullptr,
                         .shared_state_map = {},
                         .task_idx = 0};
    WARN_IF_ERROR(local_state->init(&_runtime_state, info), "fail to init local_state");
    _runtime_state.emplace_local_state(_scan_node->operator_id(), std::move(local_state));

    _range_desc.start_offset = 0;
    _range_desc.size = 1000;
    _ranges.push_back(_range_desc);
    _scan_range.ranges = _ranges;
    _scan_range.__isset.params = true;
    _scan_range.params.format_type = TFileFormatType::FORMAT_JNI;
    _kv_cache.reset(new ShardedKVCache(48));

    _cluster_info.reset(new ClusterInfo());
    _env = ExecEnv::GetInstance();
    _env->_cluster_info = _cluster_info.get();
    _env->_cluster_info->master_fe_addr.hostname = "host name";
    _env->_cluster_info->master_fe_addr.port = _backend_id;
    _env->_cluster_info->backend_id = 1001;
    _env->_wal_manager = 0;
}

void VfileScannerExceptionTest::generate_scanner(std::shared_ptr<FileScanner>& scanner) {
    auto split_source = std::make_shared<TestSplitSourceConnectorStub>(_scan_range);
    std::unordered_map<std::string, int> _colname_to_slot_id;
    scanner = std::make_shared<FileScanner>(
            &_runtime_state, &(_runtime_state.get_local_state(0)->cast<FileScanLocalState>()), -1,
            split_source, _profile, _kv_cache.get(), &_colname_to_slot_id);
    scanner->_is_load = false;
    VExprContextSPtrs _conjuncts;
    WARN_IF_ERROR(scanner->init(&_runtime_state, _conjuncts), "fail to prepare scanner");
}

void VfileScannerExceptionTest::generate_scanner_v2(std::shared_ptr<FileScannerV2>& scanner) {
    auto split_source = std::make_shared<TestSplitSourceConnectorStub>(_scan_range);
    std::unordered_map<std::string, int> _colname_to_slot_id;
    scanner = std::make_shared<FileScannerV2>(
            &_runtime_state, &(_runtime_state.get_local_state(0)->cast<FileScanLocalState>()), -1,
            split_source, _profile, _kv_cache.get(), &_colname_to_slot_id);
    scanner->_is_load = false;
    VExprContextSPtrs _conjuncts;
    WARN_IF_ERROR(scanner->init(&_runtime_state, _conjuncts), "fail to prepare scanner v2");
}

TEST_F(VfileScannerExceptionTest, failure_case) {
    std::shared_ptr<FileScanner> scanner = nullptr;
    generate_scanner(scanner);
    std::unique_ptr<Block> block(new Block());
    bool eof = false;
    auto st = scanner->get_block(&_runtime_state, block.get(), &eof);
    ASSERT_FALSE(st.ok());
    auto msg = st.to_string();
    auto pos = msg.find("Not supported create reader");
    std::cout << "msg = " << msg << std::endl;
    ASSERT_TRUE(pos != msg.npos);
    WARN_IF_ERROR(scanner->close(&_runtime_state), "fail to close scanner");
}

TEST_F(VfileScannerExceptionTest, process_late_arrival_conjuncts_retain) {
    std::shared_ptr<FileScanner> scanner = nullptr;
    generate_scanner(scanner);

    // Simulate some conjuncts in scanner
    // Let's create a dummy expr context to test the exact function
    doris::TExprNode expr_node;
    expr_node.node_type = TExprNodeType::BOOL_LITERAL;
    expr_node.type = create_type_desc(PrimitiveType::TYPE_BOOLEAN);
    expr_node.num_children = 0;
    expr_node.__isset.bool_literal = true;
    expr_node.bool_literal.value = true;

    doris::TExpr texpr;
    texpr.nodes.push_back(expr_node);

    VExprContextSPtr ctx;
    Status st = VExpr::create_expr_tree(texpr, ctx);
    ASSERT_TRUE(st.ok());
    st = ctx->prepare(&_runtime_state, RowDescriptor());
    ASSERT_TRUE(st.ok());
    st = ctx->open(&_runtime_state);
    ASSERT_TRUE(st.ok());

    scanner->_conjuncts.push_back(ctx);
    ASSERT_EQ(scanner->_conjuncts.size(), 1);
    ASSERT_EQ(scanner->_push_down_conjuncts.size(), 0);

    // Call the function that used to clear conjuncts in branch-4.0
    st = scanner->_process_late_arrival_conjuncts();
    ASSERT_TRUE(st.ok());

    // The key assertion: _conjuncts MUST NOT be cleared after this call!
    // This guarantees that subsequent JNI scanners or other readers will still have fallback filters.
    ASSERT_EQ(scanner->_conjuncts.size(), 1);
    // And push_down_conjuncts should be cloned/assigned successfully
    ASSERT_EQ(scanner->_push_down_conjuncts.size(), 1);

    WARN_IF_ERROR(scanner->close(&_runtime_state), "fail to close scanner");
}

TEST_F(VfileScannerExceptionTest, fallback_scan_bytes_follow_file_type) {
    // Verify that range-level file types override deprecated params-level file types.
    _scan_range.params.file_type = TFileType::FILE_S3;
    _scan_range.params.__isset.file_type = true;
    _scan_range.ranges[0].file_type = TFileType::FILE_LOCAL;
    _scan_range.ranges[0].__isset.file_type = true;
    std::shared_ptr<FileScanner> local_scanner = nullptr;
    generate_scanner(local_scanner);
    local_scanner->_current_range = _scan_range.ranges[0];
    int64_t local_before = _runtime_state.get_query_ctx()
                                   ->resource_ctx()
                                   ->io_context()
                                   ->scan_bytes_from_local_storage();
    int64_t remote_before = _runtime_state.get_query_ctx()
                                    ->resource_ctx()
                                    ->io_context()
                                    ->scan_bytes_from_remote_storage();
    local_scanner->_file_reader_stats->read_bytes = 128;
    local_scanner->update_realtime_counters();
    EXPECT_EQ(local_before + 128, _runtime_state.get_query_ctx()
                                          ->resource_ctx()
                                          ->io_context()
                                          ->scan_bytes_from_local_storage());
    EXPECT_EQ(remote_before, _runtime_state.get_query_ctx()
                                     ->resource_ctx()
                                     ->io_context()
                                     ->scan_bytes_from_remote_storage());
    local_scanner->update_realtime_counters();
    EXPECT_EQ(local_before + 128, _runtime_state.get_query_ctx()
                                          ->resource_ctx()
                                          ->io_context()
                                          ->scan_bytes_from_local_storage());
    WARN_IF_ERROR(local_scanner->close(&_runtime_state), "fail to close scanner");

    // Verify that params-level file type still works for old FE plans.
    _scan_range.ranges[0].__isset.file_type = false;
    _scan_range.params.file_type = TFileType::FILE_LOCAL;
    _scan_range.params.__isset.file_type = true;
    std::shared_ptr<FileScanner> compat_scanner = nullptr;
    generate_scanner(compat_scanner);
    compat_scanner->_current_range = _scan_range.ranges[0];
    local_before = _runtime_state.get_query_ctx()
                           ->resource_ctx()
                           ->io_context()
                           ->scan_bytes_from_local_storage();
    remote_before = _runtime_state.get_query_ctx()
                            ->resource_ctx()
                            ->io_context()
                            ->scan_bytes_from_remote_storage();
    compat_scanner->_file_reader_stats->read_bytes = 256;
    compat_scanner->update_realtime_counters();
    EXPECT_EQ(local_before + 256, _runtime_state.get_query_ctx()
                                          ->resource_ctx()
                                          ->io_context()
                                          ->scan_bytes_from_local_storage());
    EXPECT_EQ(remote_before, _runtime_state.get_query_ctx()
                                     ->resource_ctx()
                                     ->io_context()
                                     ->scan_bytes_from_remote_storage());
    WARN_IF_ERROR(compat_scanner->close(&_runtime_state), "fail to close scanner");
}

TEST_F(VfileScannerExceptionTest, fallback_scan_bytes_requires_file_type_when_bytes_exist) {
    TFileRangeDesc range;
    TFileScanRangeParams params;
    EXPECT_DEATH({ static_cast<void>(compute_scan_byte_buckets(1, 0, 0, range, params)); },
                 "file_type");
}

TEST_F(VfileScannerExceptionTest, file_scanner_v2_flushes_each_split_before_switch) {
    _ranges.clear();
    TFileRangeDesc local_range;
    local_range.path = "file:///tmp/local-a.csv";
    local_range.__set_file_type(TFileType::FILE_LOCAL);
    TFileRangeDesc remote_range;
    remote_range.path = "s3://bucket/remote-b.csv";
    remote_range.__set_file_type(TFileType::FILE_S3);
    _ranges.push_back(local_range);
    _ranges.push_back(remote_range);
    _scan_range.ranges = _ranges;
    _scan_range.params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    _scan_range.params.__set_file_type(TFileType::FILE_S3);

    std::shared_ptr<FileScannerV2> scanner = nullptr;
    generate_scanner_v2(scanner);
    auto reader = std::make_unique<FakeTableReader>(scanner->_file_reader_stats.get(),
                                                    scanner->_file_cache_statistics.get(),
                                                    std::vector<FakeTableReaderStep> {
                                                            FakeTableReaderStep {100, 0, 0, true},
                                                            FakeTableReaderStep {160, 0, 0, true},
                                                    });
    auto* reader_ptr = reader.get();
    scanner->_table_reader = std::move(reader);

    Block block;
    bool eof = false;
    auto status = scanner->get_block(&_runtime_state, &block, &eof);
    ASSERT_TRUE(status.ok());
    ASSERT_TRUE(eof);
    EXPECT_EQ(std::vector<std::string>({"file:///tmp/local-a.csv", "s3://bucket/remote-b.csv"}),
              reader_ptr->prepared_ranges());
    EXPECT_EQ(100, _runtime_state.get_query_ctx()
                           ->resource_ctx()
                           ->io_context()
                           ->scan_bytes_from_local_storage());
    EXPECT_EQ(60, _runtime_state.get_query_ctx()
                          ->resource_ctx()
                          ->io_context()
                          ->scan_bytes_from_remote_storage());
}

TEST_F(VfileScannerExceptionTest, file_scanner_v2_update_realtime_counters_do_not_double_count) {
    _scan_range.params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    _scan_range.params.__set_file_type(TFileType::FILE_LOCAL);
    _scan_range.ranges[0].__set_file_type(TFileType::FILE_LOCAL);

    std::shared_ptr<FileScannerV2> scanner = nullptr;
    generate_scanner_v2(scanner);
    scanner->_current_range = _scan_range.ranges[0];
    scanner->_file_reader_stats->read_bytes = 96;
    scanner->_file_cache_statistics->bytes_read_from_local = 0;
    scanner->_file_cache_statistics->bytes_read_from_remote = 0;

    scanner->update_realtime_counters();
    const int64_t local_after_first = _runtime_state.get_query_ctx()
                                              ->resource_ctx()
                                              ->io_context()
                                              ->scan_bytes_from_local_storage();
    scanner->update_realtime_counters();
    EXPECT_EQ(local_after_first, _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage());
}

TEST_F(VfileScannerExceptionTest, file_scanner_v2_prefers_cache_deltas_over_fallback) {
    _scan_range.params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    _scan_range.params.__set_file_type(TFileType::FILE_LOCAL);
    _scan_range.ranges[0].__set_file_type(TFileType::FILE_LOCAL);

    std::shared_ptr<FileScannerV2> scanner = nullptr;
    generate_scanner_v2(scanner);
    scanner->_current_range = _scan_range.ranges[0];
    scanner->_file_reader_stats->read_bytes = 200;
    scanner->_file_cache_statistics->bytes_read_from_local = 40;
    scanner->_file_cache_statistics->bytes_read_from_remote = 80;

    scanner->update_realtime_counters();
    EXPECT_EQ(40, _runtime_state.get_query_ctx()
                          ->resource_ctx()
                          ->io_context()
                          ->scan_bytes_from_local_storage());
    EXPECT_EQ(80, _runtime_state.get_query_ctx()
                          ->resource_ctx()
                          ->io_context()
                          ->scan_bytes_from_remote_storage());
}

TEST_F(VfileScannerExceptionTest, file_scanner_v2_cache_only_delta_keeps_total_scan_bytes) {
    _scan_range.params.__set_format_type(TFileFormatType::FORMAT_CSV_PLAIN);
    _scan_range.params.__set_file_type(TFileType::FILE_LOCAL);
    _scan_range.ranges[0].__set_file_type(TFileType::FILE_LOCAL);

    std::shared_ptr<FileScannerV2> scanner = nullptr;
    generate_scanner_v2(scanner);
    scanner->_current_range = _scan_range.ranges[0];
    scanner->_file_reader_stats->read_bytes = 0;
    scanner->_file_cache_statistics->bytes_read_from_local = 48;
    scanner->_file_cache_statistics->bytes_read_from_remote = 0;

    const int64_t total_before =
            _runtime_state.get_query_ctx()->resource_ctx()->io_context()->scan_bytes();
    const int64_t local_before = _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage();
    scanner->update_realtime_counters();
    EXPECT_EQ(total_before,
              _runtime_state.get_query_ctx()->resource_ctx()->io_context()->scan_bytes());
    EXPECT_EQ(local_before + 48, _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage());
}

TEST_F(VfileScannerExceptionTest, old_scanner_flushes_close_time_cache_stats_before_next_range) {
    TFileRangeDesc next_range;
    next_range.path = "s3://bucket/next-range.csv";
    next_range.__set_file_type(TFileType::FILE_S3);
    _ranges.clear();
    _ranges.push_back(next_range);
    _scan_range.ranges = _ranges;

    std::shared_ptr<FileScanner> scanner = nullptr;
    generate_scanner(scanner);
    scanner->_current_range.path = "file:///tmp/current-range.csv";
    scanner->_current_range.__set_file_type(TFileType::FILE_LOCAL);
    scanner->_first_scan_range = false;
    scanner->_should_stop = true;
    scanner->_cur_reader =
            std::make_unique<FakeGenericReader>(scanner->_file_cache_statistics.get(), 32, 0);

    const int64_t local_before = _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage();
    auto status = scanner->_get_next_reader();
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(local_before + 32, _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage());
    EXPECT_EQ("s3://bucket/next-range.csv", scanner->_current_range.path);
}

TEST_F(VfileScannerExceptionTest, old_scanner_flushes_close_time_cache_stats_on_scanner_close) {
    std::shared_ptr<FileScanner> scanner = nullptr;
    generate_scanner(scanner);
    scanner->_current_range.path = "file:///tmp/final-range.csv";
    scanner->_current_range.__set_file_type(TFileType::FILE_LOCAL);
    scanner->_cur_reader =
            std::make_unique<FakeGenericReader>(scanner->_file_cache_statistics.get(), 64, 0);

    const int64_t local_before = _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage();
    auto status = scanner->close(&_runtime_state);
    ASSERT_TRUE(status.ok());
    EXPECT_EQ(local_before + 64, _runtime_state.get_query_ctx()
                                         ->resource_ctx()
                                         ->io_context()
                                         ->scan_bytes_from_local_storage());
}

TEST(HiveReaderPositionMappingTest, PositionMappingUsesColumnIdxsForFileSlots) {
    TQueryOptions query_options;
    query_options.hive_parquet_use_column_names = false;
    RuntimeState runtime_state(query_options, TQueryGlobals());
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    params.__set_column_idxs({2, 0});
    format::ProjectedColumnBuildContext context {
            .scan_params = &params,
            .runtime_state = &runtime_state,
    };
    format::hive::HiveReader reader;

    TFileScanSlotInfo id_slot;
    id_slot.__set_is_file_slot(true);
    format::ColumnDefinition id_column {
            .identifier = Field::create_field<TYPE_STRING>("id"),
            .name = "id",
            .type = std::make_shared<DataTypeInt32>(),
    };

    TFileScanSlotInfo name_slot;
    name_slot.__set_is_file_slot(true);
    format::ColumnDefinition name_column {
            .identifier = Field::create_field<TYPE_STRING>("name"),
            .name = "name",
            .type = std::make_shared<DataTypeString>(),
    };

    ASSERT_TRUE(reader.annotate_projected_column(id_slot, &context, &id_column).ok());
    ASSERT_TRUE(id_column.has_identifier_field_id());
    EXPECT_EQ(id_column.get_identifier_position(), 2);
    EXPECT_EQ(context.next_file_column_idx, 1);

    ASSERT_TRUE(reader.annotate_projected_column(name_slot, &context, &name_column).ok());
    ASSERT_TRUE(name_column.has_identifier_field_id());
    EXPECT_EQ(name_column.get_identifier_position(), 0);
    EXPECT_EQ(context.next_file_column_idx, 2);
    ASSERT_TRUE(reader.validate_projected_columns(context).ok());
}

TEST(HiveReaderPositionMappingTest, PositionMappingDoesNotConsumePartitionSlots) {
    TQueryOptions query_options;
    query_options.hive_parquet_use_column_names = false;
    RuntimeState runtime_state(query_options, TQueryGlobals());
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    params.__set_column_idxs({3});
    format::ProjectedColumnBuildContext context {
            .scan_params = &params,
            .runtime_state = &runtime_state,
    };
    format::hive::HiveReader reader;

    TFileScanSlotInfo partition_slot;
    partition_slot.__set_is_file_slot(false);
    partition_slot.__set_category(TColumnCategory::PARTITION_KEY);
    format::ColumnDefinition partition_column {
            .identifier = Field::create_field<TYPE_STRING>("year"),
            .name = "year",
            .type = std::make_shared<DataTypeInt32>(),
    };

    TFileScanSlotInfo value_slot;
    value_slot.__set_is_file_slot(true);
    format::ColumnDefinition value_column {
            .identifier = Field::create_field<TYPE_STRING>("value"),
            .name = "value",
            .type = std::make_shared<DataTypeInt32>(),
    };

    ASSERT_TRUE(reader.annotate_projected_column(partition_slot, &context, &partition_column).ok());
    ASSERT_TRUE(partition_column.has_identifier_name());
    EXPECT_EQ(partition_column.get_identifier_name(), "year");
    EXPECT_EQ(context.next_file_column_idx, 0);

    ASSERT_TRUE(reader.annotate_projected_column(value_slot, &context, &value_column).ok());
    ASSERT_TRUE(value_column.has_identifier_field_id());
    EXPECT_EQ(value_column.get_identifier_position(), 3);
    EXPECT_EQ(context.next_file_column_idx, 1);
    ASSERT_TRUE(reader.validate_projected_columns(context).ok());
}

TEST(HiveReaderPositionMappingTest, PositionMappingFailsWhenColumnIdxsMissing) {
    TQueryOptions query_options;
    query_options.hive_parquet_use_column_names = false;
    RuntimeState runtime_state(query_options, TQueryGlobals());
    TFileScanRangeParams params;
    params.__set_format_type(TFileFormatType::FORMAT_PARQUET);
    format::ProjectedColumnBuildContext context {
            .scan_params = &params,
            .runtime_state = &runtime_state,
    };
    format::hive::HiveReader reader;

    TFileScanSlotInfo value_slot;
    value_slot.__set_is_file_slot(true);
    format::ColumnDefinition value_column {
            .identifier = Field::create_field<TYPE_STRING>("value"),
            .name = "value",
            .type = std::make_shared<DataTypeInt32>(),
    };

    auto status = reader.annotate_projected_column(value_slot, &context, &value_column);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(context.next_file_column_idx, 0);
}

} // namespace doris
