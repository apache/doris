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
#include "exec/operator/group_commit_block_sink_operator.h"

#include <gtest/gtest.h>

#include "agent/be_exec_version_manager.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "io/fs/local_file_system.h"
#include "load/group_commit/wal/wal_file_reader.h"
#include "runtime/cluster_info.h"
#include "testutil/column_helper.h"
#include "testutil/mock/mock_runtime_state.h"

namespace doris {

class GroupCommitBlockSinkLayoutTest : public testing::Test {
protected:
    void SetUp() override {
        _saved_wait_replay = config::group_commit_wait_replay_wal_finish;
        _saved_memory_rows = config::group_commit_memory_rows_for_max_filter_ratio;
        _saved_wal_limit = config::group_commit_wal_max_disk_limit;
        config::group_commit_wait_replay_wal_finish = false;
        config::group_commit_memory_rows_for_max_filter_ratio = 10;
        config::group_commit_wal_max_disk_limit = "1G";
        _env = ExecEnv::GetInstance();
        _saved_wal_manager = std::move(_env->_wal_manager);
        _saved_cluster_info = _env->cluster_info();
        _env->set_cluster_info(&_cluster_info);
        _env->set_wal_mgr(WalManager::create_unique(_env, _wal_dir));
        ASSERT_TRUE(io::global_local_filesystem()->create_directory(_wal_dir + "/1/2").ok());
        _env->wal_mgr()->wal_limit_test_bytes = 1024 * 1024 * 1024;
        ASSERT_TRUE(_env->wal_mgr()->_init_wal_dirs_info().ok());
        _state.set_exec_env(_env);
    }

    void TearDown() override {
        config::group_commit_wal_max_disk_limit = _saved_wal_limit;
        config::group_commit_memory_rows_for_max_filter_ratio = _saved_memory_rows;
        config::group_commit_wait_replay_wal_finish = _saved_wait_replay;
        _env->wal_mgr()->stop();
        _env->set_wal_mgr(std::move(_saved_wal_manager));
        _env->set_cluster_info(_saved_cluster_info);
        EXPECT_TRUE(io::global_local_filesystem()->delete_directory(_wal_dir).ok());
    }

    static TSlotDescriptor slot(const std::string& name, const DataTypePtr& type,
                                int unique_id = 0) {
        TSlotDescriptor desc;
        desc.__set_colName(name);
        desc.__set_col_unique_id(unique_id);
        desc.__set_slotType(remove_nullable(type)->to_thrift());
        desc.__set_nullIndicatorBit(type->is_nullable() ? 0 : -1);
        desc.__set_isMaterialized(true);
        return desc;
    }

    std::shared_ptr<LoadBlockQueue> queue(const std::vector<TSlotDescriptor>& slots) {
        std::string label = "layout_test";
        return std::make_shared<LoadBlockQueue>(UniqueId(1, 2), label, 3, 1, 3,
                                                std::make_shared<std::atomic_size_t>(0), false,
                                                60000, 1024 * 1024, slots);
    }

    void set_producer_tuple(const std::vector<TSlotDescriptor>& slots, TupleDescriptor& tuple,
                            ObjectPool& pool) {
        for (const auto& desc : slots) {
            tuple.add_slot(pool.add(new SlotDescriptor(desc)));
        }
    }

    Status add_block(const std::shared_ptr<LoadBlockQueue>& queue,
                     const std::vector<TSlotDescriptor>& producer_slots,
                     const std::shared_ptr<Block>& block, bool buffered, bool write_wal) {
        ObjectPool pool;
        TupleDescriptor tuple(TTupleDescriptor {});
        set_producer_tuple(producer_slots, tuple, pool);
        RowDescriptor row_desc;
        std::vector<TExpr> exprs;
        GroupCommitBlockSinkOperatorX parent(0, row_desc, exprs);
        parent._output_tuple_desc = &tuple;
        GroupCommitBlockSinkLocalState local(&parent, &_state);
        local._load_block_queue = queue;
        local._load_id_removed = true;
        local._group_commit_mode =
                write_wal ? TGroupCommitMode::ASYNC_MODE : TGroupCommitMode::SYNC_MODE;
        local._is_block_appended = !buffered;
        RETURN_IF_ERROR(local._add_block(&_state, block));
        if (buffered) {
            EXPECT_EQ(local._blocks.size(), 1);
            // Exercise the same flush used at EOS after checking max_filter_ratio.
            RETURN_IF_ERROR(local._add_blocks(&_state, true));
            local._remove_estimated_wal_bytes();
        }
        return Status::OK();
    }

    void read_block(const std::shared_ptr<LoadBlockQueue>& queue, Block& block) {
        bool found = false;
        bool eos = false;
        auto dep = Dependency::create_shared(0, 0, "test", true);
        ASSERT_TRUE(queue->get_block(&_state, &block, &found, &eos, dep).ok());
        ASSERT_TRUE(found);
    }

    void check_wal(const std::shared_ptr<LoadBlockQueue>& q, const Block& expected,
                   const std::string& expected_ids) {
        ASSERT_TRUE(q->close_wal().ok());
        std::string path;
        ASSERT_TRUE(_env->wal_mgr()->get_wal_path(3, path).ok());
        WalFileReader reader(path);
        ASSERT_TRUE(reader.init().ok());
        uint32_t version = 0;
        std::string ids;
        ASSERT_TRUE(reader.read_header(version, ids).ok());
        EXPECT_EQ(ids, expected_ids);
        for (int i = 0; i < 2; ++i) {
            PBlock pb;
            ASSERT_TRUE(reader.read_block(pb).ok());
            Block persisted;
            size_t bytes = 0;
            int64_t nanos = 0;
            ASSERT_TRUE(persisted.deserialize(pb, &bytes, &nanos).ok());
            EXPECT_TRUE(ColumnHelper::block_equal(persisted, expected));
        }
        PBlock pb;
        EXPECT_TRUE(reader.read_block(pb).is<ErrorCode::END_OF_FILE>());
    }

    MockRuntimeState _state;
    ExecEnv* _env = nullptr;
    ClusterInfo _cluster_info;
    ClusterInfo* _saved_cluster_info = nullptr;
    std::unique_ptr<WalManager> _saved_wal_manager;
    std::string _saved_wal_limit;
    bool _saved_wait_replay = false;
    int64_t _saved_memory_rows = 0;
    const std::string _wal_dir = "./log/group_commit_layout_test";
};

TEST_F(GroupCommitBlockSinkLayoutTest, DifferentFeLayoutsShareQueueAndWal) {
    const auto int_type = std::make_shared<DataTypeInt32>();
    const auto string_type = std::make_shared<DataTypeString>();
    const auto k = slot("k", int_type, 0);
    // MV IDs can overlap with base and other MV column IDs.
    const auto mv_int = slot("mv_int", make_nullable(int_type), 0);
    const auto mv_string = slot("mv_string", string_type, 1);
    const std::vector<TSlotDescriptor> consumer {k, mv_string, mv_int};
    auto q = queue(consumer);
    ASSERT_TRUE(q->create_wal(1, 2, 3, q->label, _env->wal_mgr(),
                              BeExecVersionManager::get_newest_version())
                        .ok());

    auto key = ColumnHelper::create_nullable_column_with_name<DataTypeInt32>({7, 8}, {0, 0});
    auto number = ColumnHelper::create_nullable_column_with_name<DataTypeInt32>({42, 43}, {0, 1});
    auto text = ColumnHelper::create_nullable_column_with_name<DataTypeString>({"a", "b"}, {0, 0});
    // All block labels are "column": mapping must use the sink descriptor, not expression labels.
    Block expected({key, text, number});
    auto follower_block = Block::create_shared(ColumnsWithTypeAndName {key, number, text});
    ASSERT_TRUE(add_block(q, {k, mv_int, mv_string}, follower_block, true, true).ok());
    auto master_block = Block::create_shared(ColumnsWithTypeAndName {key, text, number});
    ASSERT_TRUE(add_block(q, consumer, master_block, false, true).ok());

    for (int i = 0; i < 2; ++i) {
        Block consumed;
        read_block(q, consumed);
        ASSERT_TRUE(ColumnHelper::block_equal(consumed, expected));
        // Reordering must preserve computed values and column ownership, without copying rows.
        EXPECT_EQ(consumed.get_by_position(1).column.get(), text.column.get());
        EXPECT_EQ(consumed.get_by_position(2).column.get(), number.column.get());
    }
    check_wal(q, expected, "0,1,0");
}

TEST_F(GroupCommitBlockSinkLayoutTest, SameTypeMvColumnsDoNotSilentlySwapValues) {
    auto type = std::make_shared<DataTypeInt32>();
    auto k = slot("k", type);
    auto a = slot("mv_a", type);
    auto b = slot("mv_b", type);
    auto q = queue({k, b, a});
    auto key = ColumnHelper::create_column_with_name<DataTypeInt32>({1});
    auto first = ColumnHelper::create_column_with_name<DataTypeInt32>({11});
    auto second = ColumnHelper::create_column_with_name<DataTypeInt32>({22});
    ASSERT_TRUE(add_block(q, {k, a, b},
                          Block::create_shared(ColumnsWithTypeAndName {key, first, second}), false,
                          false)
                        .ok());
    Block result;
    read_block(q, result);
    EXPECT_EQ((*result.get_by_position(1).column)[0].get<TYPE_INT>(), 22);
    EXPECT_EQ((*result.get_by_position(2).column)[0].get<TYPE_INT>(), 11);
    EXPECT_TRUE(result.get_by_position(0).type->is_nullable());
}

TEST_F(GroupCommitBlockSinkLayoutTest, SharedMvExpressionCanHaveDifferentIndexLocalIds) {
    auto type = std::make_shared<DataTypeInt32>();
    auto q = queue({slot("mv_shared", type, 1)});
    ObjectPool pool;
    TupleDescriptor tuple(TTupleDescriptor {});
    set_producer_tuple({slot("mv_shared", type, 2)}, tuple, pool);
    std::vector<int> permutation;
    ASSERT_TRUE(q->get_column_permutation(tuple, permutation).ok());
    EXPECT_EQ(permutation, (std::vector<int> {0}));
}

TEST_F(GroupCommitBlockSinkLayoutTest, ShadowColumnsUseTypeToDisambiguate) {
    auto original = slot("v", std::make_shared<DataTypeInt32>(), 1);
    auto shadow = slot("v", std::make_shared<DataTypeInt64>(), 1);
    auto q = queue({original, shadow});
    ObjectPool pool;
    TupleDescriptor tuple(TTupleDescriptor {});
    set_producer_tuple({shadow, original}, tuple, pool);
    std::vector<int> permutation;
    ASSERT_TRUE(q->get_column_permutation(tuple, permutation).ok());
    EXPECT_EQ(permutation, (std::vector<int> {1, 0}));
}

TEST_F(GroupCommitBlockSinkLayoutTest, RejectDifferentSchemasWithSameVersionAndIndexCount) {
    auto a = slot("mv_a", std::make_shared<DataTypeInt32>());
    auto q = queue({a});
    auto mismatched = std::vector<std::vector<TSlotDescriptor>> {
            {},
            {a, a},
            {slot("mv_b", std::make_shared<DataTypeInt32>())},
            {slot("mv_a", std::make_shared<DataTypeInt64>())}};
    for (const auto& slots : mismatched) {
        ObjectPool pool;
        TupleDescriptor tuple(TTupleDescriptor {});
        set_producer_tuple(slots, tuple, pool);
        std::vector<int> permutation;
        EXPECT_TRUE(
                q->get_column_permutation(tuple, permutation).is<ErrorCode::DATA_QUALITY_ERROR>());
        EXPECT_TRUE(permutation.empty());
    }
}

} // namespace doris
