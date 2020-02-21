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

#include "exec/tablet_sink.h"

#include <gtest/gtest.h>

#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "common/config.h"
#include "runtime/decimal_value.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/tuple_row.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "service/brpc.h"
#include "util/brpc_stub_cache.h"
#include "util/cpu_info.h"
#include "runtime/descriptor_helper.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "runtime/exec_env.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/thread_resource_mgr.h"

namespace doris {
namespace stream_load {

Status k_add_batch_status;

class OlapTableSinkTest : public testing::Test {
public:
    OlapTableSinkTest() { }
    virtual ~OlapTableSinkTest() { }
    void SetUp() override {
        k_add_batch_status = Status::OK();
        _env = ExecEnv::GetInstance();
        _env->_thread_mgr = new ThreadResourceMgr();
        _env->_master_info = new TMasterInfo();
        _env->_load_stream_mgr = new LoadStreamMgr();
        _env->_brpc_stub_cache = new BrpcStubCache();
        _env->_buffer_reservation = new ReservationTracker();

        config::tablet_writer_rpc_timeout_sec = 600;
    }
    void TearDown() override {
        delete _env->_brpc_stub_cache;
        _env->_brpc_stub_cache = nullptr;
        delete _env->_load_stream_mgr;
        _env->_load_stream_mgr = nullptr;
        delete _env->_master_info;
        _env->_master_info = nullptr;
        delete _env->_thread_mgr;
        _env->_thread_mgr = nullptr;
    }
private:
    ExecEnv* _env;
};

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

    // cosntruct schema
    TOlapTableSchemaParam& tschema = tsink.schema;
    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
            tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(2).build());
            tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(10).column_name("c3").column_pos(3).build());

            tuple_builder.build(&dtb);
        }
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
            tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_BIGINT).column_name("c2").column_pos(2).build());
            tuple_builder.add_slot(
                TSlotDescriptorBuilder().string_type(20).column_name("c3").column_pos(3).build());

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

    // cosntruct schema
    TOlapTableSchemaParam& tschema = tsink.schema;
    tschema.db_id = 1;
    tschema.table_id = 2;
    tschema.version = 0;

    // descriptor
    {
        TDescriptorTableBuilder dtb;
        {
            TTupleDescriptorBuilder tuple_builder;

            tuple_builder.add_slot(
                TSlotDescriptorBuilder().type(TYPE_INT).column_name("c1").column_pos(1).build());
            tuple_builder.add_slot(
                TSlotDescriptorBuilder().decimal_type(5, 2).column_name("c2").column_pos(2).build());

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

class TestInternalService : public palo::PInternalService {
public:
    TestInternalService() { }
    virtual ~TestInternalService() { }

    void transmit_data(::google::protobuf::RpcController* controller,
                       const ::doris::PTransmitDataParams* request,
                       ::doris::PTransmitDataResult* response,
                       ::google::protobuf::Closure* done) override {
        done->Run();
    }


    void tablet_writer_open(google::protobuf::RpcController* controller,
                            const PTabletWriterOpenRequest* request,
                            PTabletWriterOpenResult* response,
                            google::protobuf::Closure* done) override {
        Status status;
        status.to_protobuf(response->mutable_status());
        done->Run();
    }

    void tablet_writer_add_batch(google::protobuf::RpcController* controller,
                                 const PTabletWriterAddBatchRequest* request,
                                 PTabletWriterAddBatchResult* response,
                                 google::protobuf::Closure* done) override {
        {
            std::lock_guard<std::mutex> l(_lock);
            row_counters += request->tablet_ids_size();
            if (request->eos()) {
                eof_counters++;
            }
            k_add_batch_status.to_protobuf(response->mutable_status());

            if (request->has_row_batch() && _row_desc != nullptr) {
                MemTracker tracker;
                RowBatch batch(*_row_desc, request->row_batch(), &tracker);
                for (int i = 0; i < batch.num_rows(); ++i){
                    LOG(INFO) << batch.get_row(i)->to_string(*_row_desc);
                    _output_set->emplace(batch.get_row(i)->to_string(*_row_desc));
                }
            }
        }
        done->Run();
    }
    void tablet_writer_cancel(google::protobuf::RpcController* controller,
                              const PTabletWriterCancelRequest* request,
                              PTabletWriterCancelResult* response,
                              google::protobuf::Closure* done) override {
        done->Run();
    }

    std::mutex _lock;
    int64_t eof_counters = 0;
    int64_t row_counters = 0;
    RowDescriptor* _row_desc = nullptr;
    std::set<std::string>* _output_set;
};

TEST_F(OlapTableSinkTest, normal) {
    // start brpc service first
    auto server = new brpc::Server();
    auto service = new TestInternalService();
    server->AddService(service, brpc::SERVER_OWNS_SERVICE);
    brpc::ServerOptions options;
    server->Start(4356, &options);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
    RuntimeState state(fragment_id, query_options, TQueryGlobals(), _env);
    state.init_mem_trackers(TUniqueId());
    // state._query_mem_tracker.reset(new MemTracker());
    // state._instance_mem_tracker.reset(new MemTracker(-1, "test", state._query_mem_tracker.get()));

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
    
    OlapTableSink sink(&obj_pool, row_desc, {}, &st);
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
    MemTracker tracker;
    RowBatch batch(row_desc, 1024, &tracker);
    // 12, 9, "abc"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 12;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 9;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 3;
        memcpy(str_val->ptr, "abc", str_val->len);
        batch.commit_last_row();
    }
    // 13, 25, "abcd"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 13;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 25;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 4;
        memcpy(str_val->ptr, "abcd", str_val->len);

        batch.commit_last_row();
    }
    // 14, 50, "abcde"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 14;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 50;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = reinterpret_cast<char*>(batch.tuple_data_pool()->allocate(10));
        str_val->len = 15;
        memcpy(str_val->ptr, "abcde1234567890", str_val->len);

        batch.commit_last_row();
    }
    st = sink.send(&state, &batch);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok());

    // each node has a eof
    ASSERT_EQ(2, service->eof_counters);
    ASSERT_EQ(2 * 2, service->row_counters);

    // 2node * 2
    ASSERT_EQ(1, state.num_rows_load_filtered());

    server->Stop(100);
    server->Join();
    delete server;
}

TEST_F(OlapTableSinkTest, convert) {
    // start brpc service first
    auto server = new brpc::Server();
    auto service = new TestInternalService();
    server->AddService(service, brpc::SERVER_OWNS_SERVICE);
    brpc::ServerOptions options;
    server->Start(4356, &options);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1024;
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

    OlapTableSink sink(&obj_pool, row_desc, exprs, &st);
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
    MemTracker tracker;
    RowBatch batch(row_desc, 1024, &tracker);
    // 12, 9, "abc"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 12;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 9;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 3;
        memcpy(str_val->ptr, "abc", str_val->len);
        batch.commit_last_row();
    }
    // 13, 25, "abcd"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 13;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 25;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 4;
        memcpy(str_val->ptr, "abcd", str_val->len);

        batch.commit_last_row();
    }
    // 14, 50, "abcde"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 14;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 50;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = reinterpret_cast<char*>(batch.tuple_data_pool()->allocate(10));
        str_val->len = 5;
        memcpy(str_val->ptr, "abcde", str_val->len);

        batch.commit_last_row();
    }
    st = sink.send(&state, &batch);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok());

    // each node has a eof
    ASSERT_EQ(2, service->eof_counters);
    ASSERT_EQ(2 * 3, service->row_counters);

    // 2node * 2
    ASSERT_EQ(0, state.num_rows_load_filtered());

    server->Stop(100);
    server->Join();
    delete server;
}

TEST_F(OlapTableSinkTest, init_fail1) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
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

    RowDescriptor row_desc(*desc_tbl, {0}, {false});

    // expr 
    std::vector<TExpr> exprs;
    exprs.resize(1);
    exprs[0].nodes.resize(1);
    exprs[0].nodes[0].node_type = TExprNodeType::SLOT_REF;
    exprs[0].nodes[0].type = tdesc_tbl.slotDescriptors[3].slotType;
    exprs[0].nodes[0].num_children = 0;
    exprs[0].nodes[0].__isset.slot_ref = true;
    exprs[0].nodes[0].slot_ref.slot_id = 0;
    exprs[0].nodes[0].slot_ref.tuple_id = 1;

    {
        OlapTableSink sink(&obj_pool, row_desc, exprs, &st);
        ASSERT_TRUE(st.ok());

        // set output tuple_id
        t_data_sink.olap_table_sink.tuple_id = 5;
        // init
        st = sink.init(t_data_sink);
        ASSERT_TRUE(st.ok());
        st = sink.prepare(&state);
        EXPECT_FALSE(st.ok());
        sink.close(&state, st);
    }
    {
        OlapTableSink sink(&obj_pool, row_desc, exprs, &st);
        ASSERT_TRUE(st.ok());

        // set output tuple_id
        t_data_sink.olap_table_sink.tuple_id = 1;
        // init
        st = sink.init(t_data_sink);
        ASSERT_TRUE(st.ok());
        st = sink.prepare(&state);
        EXPECT_FALSE(st.ok());
        sink.close(&state, st);
    }
}

TEST_F(OlapTableSinkTest, init_fail3) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
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
    exprs[1].nodes[0].type = tdesc_tbl.slotDescriptors[3].slotType;
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

    OlapTableSink sink(&obj_pool, row_desc, exprs, &st);
    ASSERT_TRUE(st.ok());

    // set output tuple_id
    t_data_sink.olap_table_sink.tuple_id = 1;
    // init
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    st = sink.prepare(&state);
    EXPECT_FALSE(st.ok());
    sink.close(&state, st);
}

TEST_F(OlapTableSinkTest, init_fail4) {
    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
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

    OlapTableSink sink(&obj_pool, row_desc, exprs, &st);
    ASSERT_TRUE(st.ok());

    // set output tuple_id
    t_data_sink.olap_table_sink.tuple_id = 1;
    // init
    t_data_sink.olap_table_sink.partition.partitions[0].indexes[0].tablets = {101, 102};
    st = sink.init(t_data_sink);
    ASSERT_TRUE(st.ok());
    st = sink.prepare(&state);
    EXPECT_FALSE(st.ok());
    sink.close(&state, st);
}

TEST_F(OlapTableSinkTest, add_batch_failed) {
    // start brpc service first
    auto server = new brpc::Server();
    auto service = new TestInternalService();
    server->AddService(service, brpc::SERVER_OWNS_SERVICE);
    brpc::ServerOptions options;
    server->Start(4356, &options);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
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

    OlapTableSink sink(&obj_pool, row_desc, exprs, &st);
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
    MemTracker tracker;
    RowBatch batch(row_desc, 1024, &tracker);
    TupleDescriptor* tuple_desc = desc_tbl->get_tuple_descriptor(0);
    // 12, 9, "abc"
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 12;
        *reinterpret_cast<int64_t*>(tuple->get_slot(8)) = 9;
        StringValue* str_val = reinterpret_cast<StringValue*>(tuple->get_slot(16));
        str_val->ptr = (char*)batch.tuple_data_pool()->allocate(10);
        str_val->len = 3;
        memcpy(str_val->ptr, "abc", str_val->len);
        batch.commit_last_row();
    }
    k_add_batch_status = Status::InternalError("dummy failed");
    st = sink.send(&state, &batch);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_FALSE(st.ok());

    server->Stop(100);
    server->Join();
    delete server;
}

TEST_F(OlapTableSinkTest, decimal) {
    // start brpc service first
    auto server = new brpc::Server();
    auto service = new TestInternalService();
    server->AddService(service, brpc::SERVER_OWNS_SERVICE);
    brpc::ServerOptions options;
    server->Start(4356, &options);

    TUniqueId fragment_id;
    TQueryOptions query_options;
    query_options.batch_size = 1;
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
    
    OlapTableSink sink(&obj_pool, row_desc, {}, &st);
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
    MemTracker tracker;
    RowBatch batch(row_desc, 1024, &tracker);
    // 12, 12.3
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 12;
        DecimalValue* dec_val = reinterpret_cast<DecimalValue*>(tuple->get_slot(16));
        *dec_val = DecimalValue("12.3");
        batch.commit_last_row();
    }
    // 13, 123.123456789
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 13;
        DecimalValue* dec_val = reinterpret_cast<DecimalValue*>(tuple->get_slot(16));
        *dec_val = DecimalValue("123.123456789");

        batch.commit_last_row();
    }
    // 14, 123456789123.1234
    {
        Tuple* tuple = (Tuple*)batch.tuple_data_pool()->allocate(tuple_desc->byte_size());
        batch.get_row(batch.add_row())->set_tuple(0, tuple);
        memset(tuple, 0, tuple_desc->byte_size());

        *reinterpret_cast<int*>(tuple->get_slot(4)) = 14;
        DecimalValue* dec_val = reinterpret_cast<DecimalValue*>(tuple->get_slot(16));
        *dec_val = DecimalValue("123456789123.1234");

        batch.commit_last_row();
    }
    st = sink.send(&state, &batch);
    ASSERT_TRUE(st.ok());
    // close
    st = sink.close(&state, Status::OK());
    ASSERT_TRUE(st.ok());

    ASSERT_EQ(2, output_set.size());
    ASSERT_TRUE(output_set.count("[(12 12.3)]") > 0);
    ASSERT_TRUE(output_set.count("[(13 123.12)]") > 0);
    // ASSERT_TRUE(output_set.count("[(14 999.99)]") > 0);

    server->Stop(100);
    server->Join();
    delete server;
}

}
}

int main(int argc, char* argv[]) {
    doris::CpuInfo::init();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
