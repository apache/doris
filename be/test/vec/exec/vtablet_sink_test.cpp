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
#include "vec/sink/vtablet_sink.h"

#include <brpc/closure_guard.h>
#include <brpc/server.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gtest/gtest-message.h>
#include <gtest/gtest-test-part.h>

#include <filesystem>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/object_pool.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "gtest/gtest_pred_impl.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_define.h"
#include "olap/wal_manager.h"
#include "runtime/decimalv2_value.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptor_helper.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/brpc_client_cache.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/proto_util.h"
#include "util/threadpool.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"

namespace google {
namespace protobuf {
class RpcController;
} // namespace protobuf
} // namespace google

namespace doris {
class PFunctionService_Stub;

namespace vectorized {

Status k_add_batch_status;

TDataSink get_data_sink(TDescriptorTable* desc_tbl) {
    int64_t db_id = 1;
    int64_t table_id = 2;
    int64_t partition_id = 3;
    int64_t index1_id = 4;
    int64_t tablet1_id = 6;
    int64_t tablet2_id = 7;

    TDataSink data_sink;
    data_sink.type = TDataSinkType::OLAP_TABLE_SINK;
    data_sink.__isset.olap_table_sink = true;

    TOlapTableSink& tsink = data_sink.olap_table_sink;
    tsink.load_id.hi = 123;
    tsink.load_id.lo = 456;
    tsink.txn_id = 789;
    tsink.db_id = 1;
    tsink.table_id = 2;
    tsink.tuple_id = 0;
    tsink.num_replicas = 3;
    tsink.db_name = "testDb";
    tsink.table_name = "testTable";

    // construct schema
    TOlapTableSchemaParam& tschema = tsink.schema;
    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_INT)
                                           .column_name("c1")
                                           .column_pos(1)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_BIGINT)
                                           .column_name("c2")
                                           .column_pos(2)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .string_type(10)
                                           .column_name("c3")
                                           .column_pos(3)
                                           .build());

            tuple_builder.build(&dtb);
        }
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_INT)
                                           .column_name("c1")
                                           .column_pos(1)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_BIGINT)
                                           .column_name("c2")
                                           .column_pos(2)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .string_type(20)
                                           .column_name("c3")
                                           .column_pos(3)
                                           .build());

            tuple_builder.build(&dtb);
        }

        *desc_tbl = dtb.desc_tbl();
        tschema.slot_descs = desc_tbl->slotDescriptors;
        tschema.tuple_desc = desc_tbl->tupleDescriptors[0];
    }
    // index
    tschema.indexes.resize(1);
    tschema.indexes[0].id = index1_id;
    tschema.indexes[0].columns = {"c1", "c2", "c3"};
    // tschema.indexes[1].id = 5;
    // tschema.indexes[1].columns = {"c1", "c3"};
    // partition
    TOlapTablePartitionParam& tpartition = tsink.partition;
    tpartition.db_id = db_id;
    tpartition.table_id = table_id;
    tpartition.version = table_id;
    tpartition.__set_partition_column("c2");
    tpartition.__set_distributed_columns({"c1", "c3"});
    tpartition.partitions.resize(1);
    tpartition.partitions[0].id = partition_id;
    tpartition.partitions[0].num_buckets = 2;
    tpartition.partitions[0].indexes.resize(1);
    tpartition.partitions[0].indexes[0].index_id = index1_id;
    tpartition.partitions[0].indexes[0].tablets = {tablet1_id, tablet2_id};
    // location
    TOlapTableLocationParam& location = tsink.location;
    location.db_id = db_id;
    location.table_id = table_id;
    location.version = 0;
    location.tablets.resize(2);
    location.tablets[0].tablet_id = tablet1_id;
    location.tablets[0].node_ids = {0, 1, 2};
    location.tablets[1].tablet_id = tablet2_id;
    location.tablets[1].node_ids = {0, 1, 2};
    // location
    TPaloNodesInfo& nodes_info = tsink.nodes_info;
    nodes_info.nodes.resize(3);
    nodes_info.nodes[0].id = 0;
    nodes_info.nodes[0].host = "127.0.0.1";
    nodes_info.nodes[0].async_internal_port = 4356;
    nodes_info.nodes[1].id = 1;
    nodes_info.nodes[1].host = "127.0.0.1";
    nodes_info.nodes[1].async_internal_port = 4356;
    nodes_info.nodes[2].id = 2;
    nodes_info.nodes[2].host = "127.0.0.1";
    nodes_info.nodes[2].async_internal_port = 4357;

    return data_sink;
}

TDataSink get_decimal_sink(TDescriptorTable* desc_tbl) {
    int64_t db_id = 1;
    int64_t table_id = 2;
    int64_t partition_id = 3;
    int64_t index1_id = 4;
    int64_t tablet1_id = 6;
    int64_t tablet2_id = 7;

    TDataSink data_sink;
    data_sink.type = TDataSinkType::OLAP_TABLE_SINK;
    data_sink.__isset.olap_table_sink = true;

    TOlapTableSink& tsink = data_sink.olap_table_sink;
    tsink.load_id.hi = 123;
    tsink.load_id.lo = 456;
    tsink.txn_id = 789;
    tsink.db_id = 1;
    tsink.table_id = 2;
    tsink.tuple_id = 0;
    tsink.num_replicas = 3;
    tsink.db_name = "testDb";
    tsink.table_name = "testTable";

    // construct schema
    TOlapTableSchemaParam& tschema = tsink.schema;
    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .type(TYPE_INT)
                                           .column_name("c1")
                                           .column_pos(1)
                                           .build());
            tuple_builder.add_slot(TSlotDescriptorBuilder()
                                           .decimal_type(5, 2)
                                           .column_name("c2")
                                           .column_pos(2)
                                           .build());

            tuple_builder.build(&dtb);
        }

        *desc_tbl = dtb.desc_tbl();
        tschema.slot_descs = desc_tbl->slotDescriptors;
        tschema.tuple_desc = desc_tbl->tupleDescriptors[0];
    }
    // index
    tschema.indexes.resize(1);
    tschema.indexes[0].id = index1_id;
    tschema.indexes[0].columns = {"c1", "c2"};
    // tschema.indexes[1].id = 5;
    // tschema.indexes[1].columns = {"c1", "c3"};
    // partition
    TOlapTablePartitionParam& tpartition = tsink.partition;
    tpartition.db_id = db_id;
    tpartition.table_id = table_id;
    tpartition.version = table_id;
    tpartition.__set_partition_column("c1");
    tpartition.__set_distributed_columns({"c2"});
    tpartition.partitions.resize(1);
    tpartition.partitions[0].id = partition_id;
    tpartition.partitions[0].num_buckets = 2;
    tpartition.partitions[0].indexes.resize(1);
    tpartition.partitions[0].indexes[0].index_id = index1_id;
    tpartition.partitions[0].indexes[0].tablets = {tablet1_id, tablet2_id};
    // location
    TOlapTableLocationParam& location = tsink.location;
    location.db_id = db_id;
    location.table_id = table_id;
    location.version = 0;
    location.tablets.resize(2);
    location.tablets[0].tablet_id = tablet1_id;
    location.tablets[0].node_ids = {0, 1, 2};
    location.tablets[1].tablet_id = tablet2_id;
    location.tablets[1].node_ids = {0, 1, 2};
    // location
    TPaloNodesInfo& nodes_info = tsink.nodes_info;
    nodes_info.nodes.resize(3);
    nodes_info.nodes[0].id = 0;
    nodes_info.nodes[0].host = "127.0.0.1";
    nodes_info.nodes[0].async_internal_port = 4356;
    nodes_info.nodes[1].id = 1;
    nodes_info.nodes[1].host = "127.0.0.1";
    nodes_info.nodes[1].async_internal_port = 4356;
    nodes_info.nodes[2].id = 2;
    nodes_info.nodes[2].host = "127.0.0.1";
    nodes_info.nodes[2].async_internal_port = 4357;

    return data_sink;
}

class VTestInternalService : public PBackendService {
public:
    VTestInternalService() {}
    virtual ~VTestInternalService() {}

    void transmit_data(::google::protobuf::RpcController* controller,
                       const ::doris::PTransmitDataParams* request,
                       ::doris::PTransmitDataResult* response,
                       ::google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
    }

    void tablet_writer_open(google::protobuf::RpcController* controller,
                            const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response,
                            google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        Status status;
        status.to_protobuf(response->mutable_status());
    }

    void tablet_writer_add_block(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBlockRequest* request,
                                 PTabletWriterAddBlockResult* response,
                                 google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
        {
            std::lock_guard<std::mutex> l(_lock);
            _row_counters += request->tablet_ids_size();
            if (request->eos()) {
                _eof_counters++;
            }
            k_add_batch_status.to_protobuf(response->mutable_status());

            if (request->has_block() && _row_desc != nullptr) {
                vectorized::Block block;
                static_cast<void>(block.deserialize(request->block()));

                for (size_t row_num = 0; row_num < block.rows(); ++row_num) {
                    std::stringstream out;
                    out << "(";
                    for (size_t i = 0; i < block.columns(); ++i) {
                        if (block.get_by_position(i).column) {
                            out << block.get_by_position(i).to_string(row_num);
                        }
                        if (i != block.columns() - 1) {
                            out << ", ";
                        }
                    }
                    out << ")";
                    _output_set->emplace(out.str());
                }
            }
        }
    }
    void tablet_writer_cancel(google::protobuf::RpcController* controller,
                              const PTabletWriterCancelRequest* request,
                              PTabletWriterCancelResult* response,
                              google::protobuf::Closure* done) override {
        brpc::ClosureGuard done_guard(done);
    }

    std::mutex _lock;
    int64_t _eof_counters = 0;
    int64_t _row_counters = 0;
    RowDescriptor* _row_desc = nullptr;
    std::set<std::string>* _output_set = nullptr;
};

class VOlapTableSinkTest : public testing::Test {
public:
    VOlapTableSinkTest() {}
    virtual ~VOlapTableSinkTest() {}
    void SetUp() override {
        k_add_batch_status = Status::OK();
        _env = ExecEnv::GetInstance();
        _env->_master_info = new TMasterInfo();
        _env->_master_info->network_address.hostname = "host name";
        _env->_master_info->network_address.port = 1234;
        _env->_internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
        _env->_function_client_cache = new BrpcClientCache<PFunctionService_Stub>();
        _env->_wal_manager = WalManager::create_shared(_env, wal_dir);
        static_cast<void>(_env->wal_mgr()->init());
        static_cast<void>(ThreadPoolBuilder("SendBatchThreadPool")
                                  .set_min_threads(1)
                                  .set_max_threads(5)
                                  .set_max_queue_size(100)
                                  .build(&_env->_send_batch_thread_pool));
        config::tablet_writer_open_rpc_timeout_sec = 60;
        config::max_send_batch_parallelism_per_job = 1;
    }

    void TearDown() override {
        static_cast<void>(io::global_local_filesystem()->delete_directory(wal_dir));
        SAFE_DELETE(_env->_internal_client_cache);
        SAFE_DELETE(_env->_function_client_cache);
        SAFE_DELETE(_env->_master_info);
        if (_server) {
            _server->Stop(100);
            _server->Join();
            SAFE_DELETE(_server);
        }
    }

    void test_normal(int be_exec_version) {
        // start brpc service first
        _server = new brpc::Server();
        auto service = new VTestInternalService();
        ASSERT_EQ(_server->AddService(service, brpc::SERVER_OWNS_SERVICE), 0);
        brpc::ServerOptions options;
        {
            debug::ScopedLeakCheckDisabler disable_lsan;
            _server->Start(4356, &options);
        }

        TUniqueId fragment_id;
        TQueryOptions query_options;
        query_options.batch_size = 1;
        query_options.be_exec_version = be_exec_version;
        RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
        state.init_mem_trackers(TUniqueId());

        ObjectPool obj_pool;
        TDescriptorTable tdesc_tbl;
        auto t_data_sink = get_data_sink(&tdesc_tbl);

        // crate desc_tabl
        DescriptorTbl* desc_tbl = nullptr;
        auto st = DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
        ASSERT_TRUE(st.ok());
        state._desc_tbl = desc_tbl;

        TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
        LOG(INFO) << "tuple_desc=" << tuple_desc->debug_string();

        RowDescriptor row_desc(*desc_tbl, {0}, {false});
        service->_row_desc = &row_desc;
        std::set<std::string> output_set;
        service->_output_set = &output_set;

        std::vector<TExpr> exprs;
        VOlapTableSink sink(&obj_pool, row_desc, exprs, false);
        ASSERT_TRUE(st.ok());

        // init
        st = sink.init(t_data_sink);
        ASSERT_TRUE(st.ok());
        // prepare
        st = sink.prepare(&state);
        ASSERT_TRUE(st.ok());
        // open
        st = sink.open(&state);
        ASSERT_TRUE(st.ok());

        int slot_count = tuple_desc->slots().size();
        std::vector<vectorized::MutableColumnPtr> columns(slot_count);
        for (int i = 0; i < slot_count; i++) {
            columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
        }

        int col_idx = 0;
        auto* column_ptr = columns[col_idx++].get();
        auto column_vector_int = column_ptr;
        int int_val = 12;
        column_vector_int->insert_data((const char*)&int_val, 0);
        int_val = 13;
        column_vector_int->insert_data((const char*)&int_val, 0);
        int_val = 14;
        column_vector_int->insert_data((const char*)&int_val, 0);

        column_ptr = columns[col_idx++].get();
        auto column_vector_bigint = column_ptr;
        int64_t int64_val = 9;
        column_vector_bigint->insert_data((const char*)&int64_val, 0);
        int64_val = 25;
        column_vector_bigint->insert_data((const char*)&int64_val, 0);
        int64_val = 50;
        column_vector_bigint->insert_data((const char*)&int64_val, 0);

        column_ptr = columns[col_idx++].get();
        auto column_vector_str = column_ptr;
        column_vector_str->insert_data("abc", 3);
        column_vector_str->insert_data("abcd", 4);
        column_vector_str->insert_data("abcde1234567890", 15);

        vectorized::Block block;
        col_idx = 0;
        for (const auto slot_desc : tuple_desc->slots()) {
            block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[col_idx++]),
                                                           slot_desc->get_data_type_ptr(),
                                                           slot_desc->col_name()));
        }

        // send
        st = sink.send(&state, &block);
        ASSERT_TRUE(st.ok());
        // close
        st = sink.close(&state, Status::OK());
        ASSERT_TRUE(st.ok() || st.to_string() == "Internal error: wait close failed. ")
                << st.to_string();

        // each node has a eof
        ASSERT_EQ(2, service->_eof_counters);
        ASSERT_EQ(2 * 2, service->_row_counters);

        // 2node * 2
        ASSERT_EQ(1, state.num_rows_load_filtered());
    }

private:
    ExecEnv* _env = nullptr;
    brpc::Server* _server = nullptr;
    std::string wal_dir = "./wal_test";
};

TEST_F(VOlapTableSinkTest, normal) {
    test_normal(1);
}

TEST_F(VOlapTableSinkTest, fallback) {
    test_normal(0);
}

TEST_F(VOlapTableSinkTest, convert) {
    // start brpc service first
    _server = new brpc::Server();
    auto service = new VTestInternalService();
    ASSERT_EQ(_server->AddService(service, brpc::SERVER_OWNS_SERVICE), 0);
    brpc::ServerOptions options;
    {
        debug::ScopedLeakCheckDisabler disable_lsan;
        _server->Start(4356, &options);
    }

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1024;
    query_options.be_exec_version = 1;
    RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
    state.init_mem_trackers(TUniqueId());

    ObjectPool obj_pool;
    TDescriptorTable tdesc_tbl;
    auto t_data_sink = get_data_sink(&tdesc_tbl);

    // crate desc_tabl
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    state._desc_tbl = desc_tbl;

    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    RowDescriptor row_desc(*desc_tbl, {0}, {false});

    // expr
    std::vector<TExpr> exprs;
    exprs.resize(3);
    exprs[0].nodes.resize(1);
    exprs[0].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[0].nodes[0].type = tdesc_tbl.slotDescriptors[3].slotType;
    exprs[0].nodes[0].num_children = 0;
    exprs[0].nodes[0].__isset.slot_ref = true;
    exprs[0].nodes[0].slot_ref.slot_id = 0;
    exprs[0].nodes[0].slot_ref.tuple_id = 1;

    exprs[1].nodes.resize(1);
    exprs[1].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[1].nodes[0].type = tdesc_tbl.slotDescriptors[4].slotType;
    exprs[1].nodes[0].num_children = 0;
    exprs[1].nodes[0].__isset.slot_ref = true;
    exprs[1].nodes[0].slot_ref.slot_id = 1;
    exprs[1].nodes[0].slot_ref.tuple_id = 1;

    exprs[2].nodes.resize(1);
    exprs[2].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[2].nodes[0].type = tdesc_tbl.slotDescriptors[5].slotType;
    exprs[2].nodes[0].num_children = 0;
    exprs[2].nodes[0].__isset.slot_ref = true;
    exprs[2].nodes[0].slot_ref.slot_id = 2;
    exprs[2].nodes[0].slot_ref.tuple_id = 1;

    VOlapTableSink sink(&obj_pool, row_desc, exprs, false);
    ASSERT_TRUE(st.ok());

    // set output tuple_id
    t_data_sink.olap_table_sink.tuple_id = 1;
    // init
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    // prepare
    st = sink.prepare(&state);
    ASSERT_TRUE(st.ok());
    // open
    st = sink.open(&state);
    ASSERT_TRUE(st.ok());
    // send
    int slot_count = tuple_desc->slots().size();
    std::vector<vectorized::MutableColumnPtr> columns(slot_count);
    for (int i = 0; i < slot_count; i++) {
        columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
    }

    int col_idx = 0;
    auto* column_ptr = columns[col_idx++].get();
    auto column_vector_int = column_ptr;
    int int_val = 12;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 13;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 14;
    column_vector_int->insert_data((const char*)&int_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_bigint = column_ptr;
    int64_t int64_val = 9;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);
    int64_val = 25;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);
    int64_val = 50;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_str = column_ptr;
    column_vector_str->insert_data("abc", 3);
    column_vector_str->insert_data("abcd", 4);
    column_vector_str->insert_data("abcde", 5);

    vectorized::Block block;
    col_idx = 0;
    for (const auto slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[col_idx++]),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }
    st = sink.send(&state, &block);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok() || st.to_string() == "Internal error: wait close failed. ")
            << st.to_string();

    // each node has a eof
    ASSERT_EQ(2, service->_eof_counters);
    ASSERT_EQ(2 * 3, service->_row_counters);

    // 2node * 2
    ASSERT_EQ(0, state.num_rows_load_filtered());
}

TEST_F(VOlapTableSinkTest, add_block_failed) {
    // start brpc service first
    _server = new brpc::Server();
    auto service = new VTestInternalService();
    ASSERT_EQ(_server->AddService(service, brpc::SERVER_OWNS_SERVICE), 0);
    brpc::ServerOptions options;
    {
        debug::ScopedLeakCheckDisabler disable_lsan;
        _server->Start(4356, &options);
    }

    // ObjectPool create before RuntimeState, simulate actual situation better.
    ObjectPool obj_pool;

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
    query_options.be_exec_version = 1;
    RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
    state.init_mem_trackers(TUniqueId());

    TDescriptorTable tdesc_tbl;
    auto t_data_sink = get_data_sink(&tdesc_tbl);

    // crate desc_tabl
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    state._desc_tbl = desc_tbl;

    RowDescriptor row_desc(*desc_tbl, {0}, {false});

    // expr
    std::vector<TExpr> exprs;
    exprs.resize(3);
    exprs[0].nodes.resize(1);
    exprs[0].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[0].nodes[0].type = tdesc_tbl.slotDescriptors[3].slotType;
    exprs[0].nodes[0].num_children = 0;
    exprs[0].nodes[0].__isset.slot_ref = true;
    exprs[0].nodes[0].slot_ref.slot_id = 0;
    exprs[0].nodes[0].slot_ref.tuple_id = 1;

    exprs[1].nodes.resize(1);
    exprs[1].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[1].nodes[0].type = tdesc_tbl.slotDescriptors[4].slotType;
    exprs[1].nodes[0].num_children = 0;
    exprs[1].nodes[0].__isset.slot_ref = true;
    exprs[1].nodes[0].slot_ref.slot_id = 1;
    exprs[1].nodes[0].slot_ref.tuple_id = 1;

    exprs[2].nodes.resize(1);
    exprs[2].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[2].nodes[0].type = tdesc_tbl.slotDescriptors[5].slotType;
    exprs[2].nodes[0].num_children = 0;
    exprs[2].nodes[0].__isset.slot_ref = true;
    exprs[2].nodes[0].slot_ref.slot_id = 2;
    exprs[2].nodes[0].slot_ref.tuple_id = 1;

    VOlapTableSink sink(&obj_pool, row_desc, exprs, false);
    ASSERT_TRUE(st.ok());

    // set output tuple_id
    t_data_sink.olap_table_sink.tuple_id = 1;
    // init
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    st = sink.prepare(&state);
    ASSERT_TRUE(st.ok());
    st = sink.open(&state);
    ASSERT_TRUE(st.ok());
    // send
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);

    int slot_count = tuple_desc->slots().size();
    std::vector<vectorized::MutableColumnPtr> columns(slot_count);
    for (int i = 0; i < slot_count; i++) {
        columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
    }

    int col_idx = 0;
    auto* column_ptr = columns[col_idx++].get();
    auto column_vector_int = column_ptr;
    int int_val = 12;
    column_vector_int->insert_data((const char*)&int_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_bigint = column_ptr;
    int64_t int64_val = 9;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_str = column_ptr;
    column_vector_str->insert_data("abc", 3);

    vectorized::Block block;
    col_idx = 0;
    for (const auto slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[col_idx++]),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }
    // Channels will be cancelled internally, coz brpc returns k_add_batch_status.
    k_add_batch_status = Status::InternalError("dummy failed");
    st = sink.send(&state, &block);
    ASSERT_TRUE(st.ok());

    // Send batch multiple times, can make _cur_batch or _pending_batches(in channels) not empty.
    // To ensure the order of releasing resource is OK.
    static_cast<void>(sink.send(&state, &block));
    static_cast<void>(sink.send(&state, &block));

    // close
    st = sink.close(&state, Status::OK());
    ASSERT_FALSE(st.ok());
}

TEST_F(VOlapTableSinkTest, decimal) {
    // start brpc service first
    _server = new brpc::Server();
    auto service = new VTestInternalService();
    ASSERT_EQ(_server->AddService(service, brpc::SERVER_OWNS_SERVICE), 0);
    brpc::ServerOptions options;
    {
        debug::ScopedLeakCheckDisabler disable_lsan;
        _server->Start(4356, &options);
    }

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
    query_options.be_exec_version = 1;
    RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
    state.init_mem_trackers(TUniqueId());

    ObjectPool obj_pool;
    TDescriptorTable tdesc_tbl;
    auto t_data_sink = get_decimal_sink(&tdesc_tbl);

    // crate desc_tabl
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    state._desc_tbl = desc_tbl;

    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    LOG(INFO) << "tuple_desc=" << tuple_desc->debug_string();

    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    service->_row_desc = &row_desc;
    std::set<std::string> output_set;
    service->_output_set = &output_set;

    std::vector<TExpr> exprs;
    VOlapTableSink sink(&obj_pool, row_desc, exprs, false);
    ASSERT_TRUE(st.ok());

    // init
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    // prepare
    st = sink.prepare(&state);
    ASSERT_TRUE(st.ok());
    // open
    st = sink.open(&state);
    ASSERT_TRUE(st.ok());
    // send
    int slot_count = tuple_desc->slots().size();
    std::vector<vectorized::MutableColumnPtr> columns(slot_count);
    for (int i = 0; i < slot_count; i++) {
        columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
    }

    int col_idx = 0;
    auto* column_ptr = columns[col_idx++].get();
    auto column_vector_int = column_ptr;
    int int_val = 12;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 13;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 14;
    column_vector_int->insert_data((const char*)&int_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_dec = column_ptr;
    DecimalV2Value dec_val(std::string("12.3"));
    column_vector_dec->insert_data((const char*)&dec_val, 0);
    dec_val = std::string("123.123456789");
    column_vector_dec->insert_data((const char*)&dec_val, 0);
    dec_val = std::string("123456789123.1234");
    column_vector_dec->insert_data((const char*)&dec_val, 0);

    vectorized::Block block;
    col_idx = 0;
    for (const auto slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[col_idx++]),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }
    st = sink.send(&state, &block);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok() || st.to_string() == "Internal error: wait close failed. ")
            << st.to_string();

    ASSERT_EQ(2, output_set.size());
    ASSERT_TRUE(output_set.count("(12, 12.300000000)") > 0);
    ASSERT_TRUE(output_set.count("(13, 123.120000000)") > 0);
}

TEST_F(VOlapTableSinkTest, group_commit) {
    // start brpc service first
    _server = new brpc::Server();
    auto service = new VTestInternalService();
    ASSERT_EQ(_server->AddService(service, brpc::SERVER_OWNS_SERVICE), 0);
    brpc::ServerOptions options;
    {
        debug::ScopedLeakCheckDisabler disable_lsan;
        _server->Start(4356, &options);
    }

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
    query_options.be_exec_version = 0;
    RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
    state.init_mem_trackers(TUniqueId());

    ObjectPool obj_pool;
    TDescriptorTable tdesc_tbl;
    auto t_data_sink = get_data_sink(&tdesc_tbl);

    // crate desc_tabl
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    state._desc_tbl = desc_tbl;
    state._wal_id = 789;
    state._import_label = "test";

    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    LOG(INFO) << "tuple_desc=" << tuple_desc->debug_string();

    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    service->_row_desc = &row_desc;
    std::set<std::string> output_set;
    service->_output_set = &output_set;

    std::vector<TExpr> exprs;
    VOlapTableSink sink(&obj_pool, row_desc, exprs, true);

    // init
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    // prepare
    st = sink.prepare(&state);
    ASSERT_TRUE(st.ok());
    // open
    st = sink.open(&state);
    ASSERT_TRUE(st.ok());

    int slot_count = tuple_desc->slots().size();
    std::vector<vectorized::MutableColumnPtr> columns(slot_count);
    for (int i = 0; i < slot_count; i++) {
        columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
    }

    int col_idx = 0;
    auto* column_ptr = columns[col_idx++].get();
    auto column_vector_int = column_ptr;
    int int_val = 12;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 13;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 14;
    column_vector_int->insert_data((const char*)&int_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_bigint = column_ptr;
    int64_t int64_val = 9;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);
    int64_val = 25;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);
    int64_val = 50;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_str = column_ptr;
    column_vector_str->insert_data("abc", 3);
    column_vector_str->insert_data("abcd", 4);
    column_vector_str->insert_data("1234567890", 10);

    vectorized::Block block;
    col_idx = 0;
    for (const auto slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[col_idx++]),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }
    vectorized::Block org_block(block);

    // send
    st = sink.send(&state, &block);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok() || st.to_string() == "Internal error: wait close failed. ")
            << st.to_string();

    // each node has a eof
    ASSERT_EQ(2, service->_eof_counters);
    ASSERT_EQ(2 * 3, service->_row_counters);

    // 2node * 2
    ASSERT_EQ(0, state.num_rows_load_filtered());

    std::string wal_path = wal_dir + "/" + std::to_string(t_data_sink.olap_table_sink.db_id) + "/" +
                           std::to_string(t_data_sink.olap_table_sink.table_id) + "/" +
                           std::to_string(t_data_sink.olap_table_sink.txn_id) + "_" +
                           state.import_label();
    doris::PBlock pblock;
    auto wal_reader = WalReader(wal_path);
    st = wal_reader.init();
    ASSERT_TRUE(st.ok());
    st = wal_reader.read_block(pblock);
    ASSERT_TRUE(st.ok());
    vectorized::Block wal_block;
    ASSERT_TRUE(wal_block.deserialize(pblock).ok());
    ASSERT_TRUE(st.ok() || st.is<ErrorCode::END_OF_FILE>());
    ASSERT_EQ(org_block.rows(), wal_block.rows());
    for (int i = 0; i < org_block.rows(); i++) {
        std::string srcRow = org_block.dump_one_line(i, org_block.columns());
        std::string walRow = wal_block.dump_one_line(i, org_block.columns());
        ASSERT_TRUE(std::strcmp(srcRow.c_str(), walRow.c_str()) == 0);
    }
}

TEST_F(VOlapTableSinkTest, group_commit_with_filter_row) {
    // start brpc service first
    _server = new brpc::Server();
    auto service = new VTestInternalService();
    ASSERT_EQ(_server->AddService(service, brpc::SERVER_OWNS_SERVICE), 0);
    brpc::ServerOptions options;
    {
        debug::ScopedLeakCheckDisabler disable_lsan;
        _server->Start(4356, &options);
    }

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
    query_options.be_exec_version = 0;
    RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
    state.init_mem_trackers(TUniqueId());

    ObjectPool obj_pool;
    TDescriptorTable tdesc_tbl;
    auto t_data_sink = get_data_sink(&tdesc_tbl);

    // crate desc_tabl
    DescriptorTbl* desc_tbl = nullptr;
    auto st = DescriptorTbl::create(&obj_pool, tdesc_tbl, &desc_tbl);
    ASSERT_TRUE(st.ok());
    state._desc_tbl = desc_tbl;
    state._wal_id = 789;
    state._import_label = "test";

    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    LOG(INFO) << "tuple_desc=" << tuple_desc->debug_string();

    RowDescriptor row_desc(*desc_tbl, {0}, {false});
    service->_row_desc = &row_desc;
    std::set<std::string> output_set;
    service->_output_set = &output_set;

    std::vector<TExpr> exprs;
    VOlapTableSink sink(&obj_pool, row_desc, exprs, true);

    // init
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    // prepare
    st = sink.prepare(&state);
    ASSERT_TRUE(st.ok());
    // open
    st = sink.open(&state);
    ASSERT_TRUE(st.ok());

    int slot_count = tuple_desc->slots().size();
    std::vector<vectorized::MutableColumnPtr> columns(slot_count);
    for (int i = 0; i < slot_count; i++) {
        columns[i] = tuple_desc->slots()[i]->get_empty_mutable_column();
    }

    int col_idx = 0;
    auto* column_ptr = columns[col_idx++].get();
    auto column_vector_int = column_ptr;
    int int_val = 12;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 13;
    column_vector_int->insert_data((const char*)&int_val, 0);
    int_val = 14;
    column_vector_int->insert_data((const char*)&int_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_bigint = column_ptr;
    int64_t int64_val = 9;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);
    int64_val = 25;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);
    int64_val = 50;
    column_vector_bigint->insert_data((const char*)&int64_val, 0);

    column_ptr = columns[col_idx++].get();
    auto column_vector_str = column_ptr;
    column_vector_str->insert_data("abc", 3);
    column_vector_str->insert_data("abcd", 4);
    column_vector_str->insert_data("abcde1234567890", 15);

    vectorized::Block block;
    col_idx = 0;
    for (const auto slot_desc : tuple_desc->slots()) {
        block.insert(vectorized::ColumnWithTypeAndName(std::move(columns[col_idx++]),
                                                       slot_desc->get_data_type_ptr(),
                                                       slot_desc->col_name()));
    }
    vectorized::Block org_block(block);

    // send
    st = sink.send(&state, &block);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok() || st.to_string() == "Internal error: wait close failed. ")
            << st.to_string();

    // each node has a eof
    ASSERT_EQ(2, service->_eof_counters);
    ASSERT_EQ(2 * 2, service->_row_counters);

    // 2node * 2
    ASSERT_EQ(1, state.num_rows_load_filtered());

    std::string wal_path = wal_dir + "/" + std::to_string(t_data_sink.olap_table_sink.db_id) + "/" +
                           std::to_string(t_data_sink.olap_table_sink.table_id) + "/" +
                           std::to_string(t_data_sink.olap_table_sink.txn_id) + "_" +
                           state.import_label();
    doris::PBlock pblock;
    auto wal_reader = WalReader(wal_path);
    st = wal_reader.init();
    ASSERT_TRUE(st.ok());
    st = wal_reader.read_block(pblock);
    ASSERT_TRUE(st.ok());
    vectorized::Block wal_block;
    ASSERT_TRUE(wal_block.deserialize(pblock).ok());
    ASSERT_TRUE(st.ok() || st.is<ErrorCode::END_OF_FILE>());
    ASSERT_EQ(org_block.rows() - 1, wal_block.rows());
    for (int i = 0; i < wal_block.rows(); i++) {
        std::string srcRow = org_block.dump_one_line(i, org_block.columns());
        std::string walRow = wal_block.dump_one_line(i, org_block.columns());
        ASSERT_TRUE(std::strcmp(srcRow.c_str(), walRow.c_str()) == 0);
    }
}
} // namespace vectorized
} // namespace doris
