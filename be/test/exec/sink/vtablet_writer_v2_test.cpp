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

#include "exec/sink/writer/vtablet_writer_v2.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "core/data_type/data_type_number.h"
#include "exec/operator/operator_helper.h"
#include "exec/sink/delta_writer_v2_pool.h"
#include "exec/sink/load_stream_map_pool.h"
#include "exec/sink/load_stream_stub.h"
#include "exec/sink/sink_test_utils.h"
#include "io/fs/local_file_system.h"
#include "load/memtable/memtable_memory_limiter.h"
#include "runtime/exec_env.h"
#include "storage/storage_engine.h"
#include "storage/tablet/tablet_schema.h"
#include "testutil/column_helper.h"
#include "testutil/creators.h"
#include "util/debug_points.h"
#include "util/defer_op.h"

namespace doris {

class TestVTabletWriterV2 : public ::testing::Test {
public:
    TestVTabletWriterV2() = default;
    ~TestVTabletWriterV2() override = default;
    static void SetUpTestSuite() {}
    static void TearDownTestSuite() {}
};

const int64_t src_id = 1000;

static void add_stream(std::shared_ptr<LoadStreamMap> load_stream_map, int64_t node_id,
                       std::vector<int64_t> success_tablets,
                       std::unordered_map<int64_t, Status> failed_tablets) {
    auto streams = load_stream_map->get_or_create(node_id);
    streams->mark_open();
    for (const auto& tablet_id : success_tablets) {
        streams->select_one_stream()->add_success_tablet(tablet_id);
    }
    for (const auto& [tablet_id, reason] : failed_tablets) {
        streams->select_one_stream()->add_failed_tablet(tablet_id, reason);
    }
}

static std::unique_ptr<VTabletWriterV2> create_vtablet_writer(int num_replicas = 3) {
    TDataSink t_sink;
    t_sink.__isset.olap_table_sink = true;
    t_sink.olap_table_sink.num_replicas = num_replicas;
    VExprContextSPtrs output_exprs;
    std::shared_ptr<Dependency> dep = nullptr;
    std::shared_ptr<Dependency> fin_dep = nullptr;
    auto writer = std::make_unique<VTabletWriterV2>(t_sink, output_exprs, dep, fin_dep);

    int required_replicas = num_replicas / 2 + 1;
    writer->_tablet_replica_info[1] = std::make_pair(num_replicas, required_replicas);
    writer->_tablet_replica_info[2] = std::make_pair(num_replicas, required_replicas);

    return writer;
}

static TColumn create_int_column_desc(bool is_nullable) {
    auto column = testutil::create_tablet_column({"c1", TPrimitiveType::INT, true});
    column.__set_is_allow_null(is_nullable);
    column.__set_col_unique_id(1);
    return column;
}

static void prepare_load_runtime_state(MockRuntimeState& state, int sender_id) {
    state.set_backend_id(1);
    state.set_per_fragment_instance_idx(sender_id);
    state.set_num_per_fragment_instances(2);
    state.set_load_stream_per_node(1);
    state.set_total_load_streams(2);
    state.set_num_local_sink(2);
}

static void prepare_open_streams(std::shared_ptr<LoadStreamMap> load_stream_map, int64_t node_id,
                                 int64_t index_id, const TabletSchemaSPtr& tablet_schema) {
    auto streams = load_stream_map->get_or_create(node_id);
    streams->mark_open();
    for (auto& stream : streams->streams()) {
        stream->_is_open.store(true);
        stream->_status = Status::OK();
        stream->_tablet_schema_for_index->emplace(index_id, tablet_schema);
        stream->_enable_unique_mow_for_index->emplace(index_id, false);
    }
}

static TabletSchemaSPtr create_int_tablet_schema() {
    TabletSchemaPB tablet_schema_pb;
    tablet_schema_pb.set_keys_type(DUP_KEYS);
    tablet_schema_pb.set_num_short_key_columns(1);
    tablet_schema_pb.set_num_rows_per_row_block(1024);
    tablet_schema_pb.set_compress_kind(COMPRESS_NONE);
    tablet_schema_pb.set_next_column_unique_id(2);
    testutil::add_column_pb(&tablet_schema_pb, 1, "c1", "INT", true, false);

    auto tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->init_from_pb(tablet_schema_pb);
    return tablet_schema;
}

static TDataSink create_vtablet_writer_sink(const TOlapTableSchemaParam& schema,
                                            const TOlapTablePartitionParam& partition,
                                            const TOlapTableLocationParam& location,
                                            TTupleId tuple_id, const TUniqueId& load_id) {
    TDataSink t_sink;
    t_sink.__isset.olap_table_sink = true;
    auto& olap_sink = t_sink.olap_table_sink;
    olap_sink.__set_load_id(load_id);
    olap_sink.__set_txn_id(1);
    olap_sink.__set_db_id(schema.db_id);
    olap_sink.__set_table_id(schema.table_id);
    olap_sink.__set_tuple_id(tuple_id);
    olap_sink.__set_num_replicas(1);
    olap_sink.__set_need_gen_rollup(false);
    olap_sink.__set_schema(schema);
    olap_sink.__set_partition(partition);
    olap_sink.__set_location(location);

    TNodeInfo node;
    node.__set_id(1);
    node.__set_option(0);
    node.__set_host("127.0.0.1");
    node.__set_async_internal_port(8060);
    TPaloNodesInfo nodes;
    nodes.nodes.push_back(node);
    olap_sink.__set_nodes_info(nodes);
    return t_sink;
}

TEST_F(TestVTabletWriterV2, shared_delta_writer_should_not_access_destroyed_creator_runtime_state) {
    const bool old_share_delta_writers = config::share_delta_writers;
    const int32_t old_flush_running_count_limit = config::memtable_flush_running_count_limit;
    const bool old_enable_debug_points = config::enable_debug_points;
    config::share_delta_writers = true;
    Defer restore_configs([&] {
        config::share_delta_writers = old_share_delta_writers;
        config::memtable_flush_running_count_limit = old_flush_running_count_limit;
        config::enable_debug_points = old_enable_debug_points;
        DebugPoints::instance()->remove("DeltaWriterV2.write.flush_limit_wait");
    });

    ExecEnv* exec_env = ExecEnv::GetInstance();
    auto old_load_stream_map_pool = std::move(exec_env->_load_stream_map_pool);
    auto old_delta_writer_v2_pool = std::move(exec_env->_delta_writer_v2_pool);
    auto old_memtable_memory_limiter = std::move(exec_env->_memtable_memory_limiter);
    auto old_storage_engine = std::move(exec_env->_storage_engine);
    const std::string old_storage_root_path = config::storage_root_path;
    char cwd_buffer[1024];
    ASSERT_NE(nullptr, getcwd(cwd_buffer, sizeof(cwd_buffer)));
    const std::string test_data_dir =
            std::string(cwd_buffer) + "/vtablet_writer_v2_shared_delta_writer_test";
    Defer restore_exec_env([&]() mutable {
        exec_env->_delta_writer_v2_pool.reset();
        exec_env->_load_stream_map_pool.reset();
        exec_env->_storage_engine.reset();
        exec_env->_memtable_memory_limiter.reset();
        exec_env->_storage_engine = std::move(old_storage_engine);
        exec_env->_memtable_memory_limiter = std::move(old_memtable_memory_limiter);
        exec_env->_delta_writer_v2_pool = std::move(old_delta_writer_v2_pool);
        exec_env->_load_stream_map_pool = std::move(old_load_stream_map_pool);
        config::storage_root_path = old_storage_root_path;
        static_cast<void>(io::global_local_filesystem()->delete_directory(test_data_dir));
    });

    config::storage_root_path = test_data_dir;
    ASSERT_TRUE(io::global_local_filesystem()->delete_directory(test_data_dir).ok());
    ASSERT_TRUE(io::global_local_filesystem()->create_directory(test_data_dir).ok());
    EngineOptions options;
    options.store_paths.emplace_back(test_data_dir, -1);
    auto engine = std::make_unique<StorageEngine>(options);
    ASSERT_TRUE(engine->open().ok());
    exec_env->_storage_engine = std::move(engine);
    auto memtable_memory_limiter = std::make_unique<MemTableMemoryLimiter>();
    ASSERT_TRUE(memtable_memory_limiter->init(1024 * 1024 * 1024).ok());
    exec_env->_memtable_memory_limiter = std::move(memtable_memory_limiter);
    exec_env->_load_stream_map_pool = std::make_unique<LoadStreamMapPool>();
    exec_env->_delta_writer_v2_pool = std::make_unique<DeltaWriterV2Pool>();

    auto creator_ctx = std::make_unique<OperatorContext>();
    OperatorContext current_ctx;
    prepare_load_runtime_state(creator_ctx->state, 0);
    prepare_load_runtime_state(current_ctx.state, 1);

    TOlapTableSchemaParam schema;
    TTupleId tuple_id = 0;
    int64_t index_id = 0;
    sink_test_utils::build_desc_tbl_and_schema(*creator_ctx, schema, tuple_id, index_id, false);
    sink_test_utils::build_desc_tbl_and_schema(current_ctx, schema, tuple_id, index_id, false);
    schema.indexes[0].__set_columns_desc({create_int_column_desc(false)});

    TUniqueId load_id;
    load_id.hi = 380;
    load_id.lo = 1;
    const auto partition = sink_test_utils::build_partition_param(index_id);
    const auto location = sink_test_utils::build_location_param();
    const auto t_sink = create_vtablet_writer_sink(schema, partition, location, tuple_id, load_id);

    VExprContextSPtrs output_exprs;
    auto creator_writer = std::make_unique<VTabletWriterV2>(t_sink, output_exprs, nullptr, nullptr);
    auto current_writer = std::make_unique<VTabletWriterV2>(t_sink, output_exprs, nullptr, nullptr);
    ASSERT_TRUE(creator_writer->_init(&creator_ctx->state, &creator_ctx->profile).ok());
    ASSERT_TRUE(current_writer->_init(&current_ctx.state, &current_ctx.profile).ok());
    ASSERT_EQ(creator_writer->_delta_writer_for_tablet, current_writer->_delta_writer_for_tablet);

    const auto tablet_schema = create_int_tablet_schema();
    prepare_open_streams(creator_writer->_load_stream_map, 1, index_id, tablet_schema);

    auto block = std::make_shared<Block>(ColumnHelper::create_block<DataTypeInt32>({1}));
    Rows rows;
    rows.partition_id = 1;
    rows.index_id = index_id;
    rows.row_idxes.push_back(0);

    const auto first_write_status = creator_writer->_write_memtable(block, 100, rows);
    ASSERT_TRUE(first_write_status.ok()) << first_write_status;
    ASSERT_EQ(1, creator_writer->_delta_writer_for_tablet->size());

    // The first write above creates the shared DeltaWriterV2 and stores
    // creator_ctx->state in DeltaWriterV2::_state. Destroy the creator sink and
    // its RuntimeState to reproduce the original lifetime boundary: another
    // local sink can still reuse the shared writer after the creator state is
    // gone. Do not call creator_writer->_cancel() here because it cancels the
    // shared writer and would hide the dangling RuntimeState path.
    creator_writer.reset();
    creator_ctx.reset();

    // Force the current sink into DeltaWriterV2::write()'s flush-limit wait
    // path, then cancel the current RuntimeState. Fixed code should observe the
    // current sink's cancel state and exit cleanly; current broken code reads
    // the destroyed creator RuntimeState from the shared DeltaWriterV2 and ASAN
    // reports heap-use-after-free in the child process.
    auto debug_point = std::make_shared<DebugPoint>();
    debug_point->execute_limit = 1;
    debug_point->callback = std::function<void()>(
            [&] { current_ctx.state.cancel(Status::Cancelled("current state cancelled")); });
    config::enable_debug_points = true;
    DebugPoints::instance()->add("DeltaWriterV2.write.flush_limit_wait", debug_point);
    config::memtable_flush_running_count_limit = 0;

    EXPECT_EXIT(
            {
                alarm(10);
                auto status = current_writer->_write_memtable(block, 100, rows);
                if (!status.ok() &&
                    status.msg().find("current state cancelled") != std::string::npos) {
                    _exit(0);
                }
                _exit(1);
            },
            ::testing::ExitedWithCode(0), "");

    config::memtable_flush_running_count_limit = old_flush_running_count_limit;
    DebugPoints::instance()->remove("DeltaWriterV2.write.flush_limit_wait");

    current_writer->_cancel(Status::Cancelled("test cleanup"));
}

TEST_F(TestVTabletWriterV2, close_wait_notifier_should_be_scoped_to_load_stream_map) {
    UniqueId load_id1;
    UniqueId load_id2;
    load_id2.lo = 1;
    std::shared_ptr<LoadStreamMap> load_stream_map1 =
            std::make_shared<LoadStreamMap>(load_id1, src_id, 1, 1, nullptr);
    std::shared_ptr<LoadStreamMap> load_stream_map2 =
            std::make_shared<LoadStreamMap>(load_id2, src_id, 1, 1, nullptr);
    auto streams1 = load_stream_map1->get_or_create(1001);
    auto streams2 = load_stream_map2->get_or_create(1002);
    streams1->mark_open();
    streams2->mark_open();

    int64_t version1 = load_stream_map1->close_wait_version();
    int64_t version2 = load_stream_map2->close_wait_version();
    streams1->select_one_stream()->cancel(Status::Cancelled("test"));

    ASSERT_GT(load_stream_map1->close_wait_version(), version1);
    ASSERT_EQ(load_stream_map2->close_wait_version(), version2);
}

TEST_F(TestVTabletWriterV2, one_replica) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});

    auto writer = create_vtablet_writer(1);
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 2);
}

TEST_F(TestVTabletWriterV2, one_replica_fail) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1}, {{2, Status::InternalError("test")}});

    auto writer = create_vtablet_writer(1);
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_EQ(st, Status::InternalError("test"));
}

TEST_F(TestVTabletWriterV2, two_replica) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1, 2}, {});

    auto writer = create_vtablet_writer(2);
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, two_replica_fail) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1002, {1, 2}, {});

    auto writer = create_vtablet_writer(2);
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_EQ(st, Status::InternalError("test"));
}

TEST_F(TestVTabletWriterV2, normal) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1, 2}, {});
    add_stream(load_stream_map, 1003, {1, 2}, {});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 6);
}

TEST_F(TestVTabletWriterV2, miss_one) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {});
    add_stream(load_stream_map, 1003, {1, 2}, {});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 5);
}

TEST_F(TestVTabletWriterV2, miss_two) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {});
    add_stream(load_stream_map, 1003, {1}, {});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, fail_one) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1, 2}, {});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 5);
}

TEST_F(TestVTabletWriterV2, fail_one_duplicate) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1, 2}, {});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    // Duplicate tablets from same node should be ignored
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 5);
}

TEST_F(TestVTabletWriterV2, fail_two_diff_tablet_same_node) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {},
               {{1, Status::InternalError("test")}, {2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1, 2}, {});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, fail_two_diff_tablet_diff_node) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {2}, {{1, Status::InternalError("test")}});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    ASSERT_TRUE(st.ok());
    ASSERT_EQ(tablet_commit_infos.size(), 4);
}

TEST_F(TestVTabletWriterV2, fail_two_same_tablet) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1, 2}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1}, {{2, Status::InternalError("test")}});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    // BE should detect and abort commit if majority of replicas failed
    ASSERT_EQ(st, Status::InternalError("test"));
}

TEST_F(TestVTabletWriterV2, fail_two_miss_one_same_tablet) {
    UniqueId load_id;
    std::vector<TTabletCommitInfo> tablet_commit_infos;
    std::shared_ptr<LoadStreamMap> load_stream_map =
            std::make_shared<LoadStreamMap>(load_id, src_id, 1, 1, nullptr);
    add_stream(load_stream_map, 1001, {1}, {});
    add_stream(load_stream_map, 1002, {1}, {{2, Status::InternalError("test")}});
    add_stream(load_stream_map, 1003, {1}, {{2, Status::InternalError("test")}});

    auto writer = create_vtablet_writer();
    auto st = writer->_create_commit_info(tablet_commit_infos, load_stream_map);
    // BE should detect and abort commit if majority of replicas failed
    ASSERT_EQ(st, Status::InternalError("test"));
}

} // namespace doris
