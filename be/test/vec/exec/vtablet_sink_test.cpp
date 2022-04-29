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

#include <gtest/gtest.h>

#include <map>
#include <string>
#include <vector>

#include "common/config.h"
#include "gen_cpp/HeartbeatService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/bufferpool/reservation_tracker.h"
#include "runtime/decimalv2_value.h"
#include "runtime/descriptor_helper.h"
#include "runtime/exec_env.h"
#include "runtime/result_queue_mgr.h"
#include "runtime/runtime_state.h"
#include "runtime/stream_load/load_stream_mgr.h"
#include "runtime/thread_resource_mgr.h"
#include "runtime/types.h"
#include "service/brpc.h"
#include "util/brpc_client_cache.h"
#include "util/cpu_info.h"
#include "util/debug/leakcheck_disabler.h"
#include "util/proto_util.h"

namespace doris {

namespace stream_load {

extern Status k_add_batch_status;

class VOlapTableSinkTest : public testing::Test {
public:
    VOlapTableSinkTest() {}
    virtual ~VOlapTableSinkTest() {}
    void SetUp() override {
        k_add_batch_status = Status::OK();
        _env = ExecEnv::GetInstance();
        _env->_thread_mgr = new ThreadResourceMgr();
        _env->_master_info = new TMasterInfo();
        _env->_load_stream_mgr = new LoadStreamMgr();
        _env->_internal_client_cache = new BrpcClientCache<PBackendService_Stub>();
        _env->_function_client_cache = new BrpcClientCache<PFunctionService_Stub>();
        _env->_buffer_reservation = new ReservationTracker();
        _env->_task_pool_mem_tracker_registry.reset(new MemTrackerTaskPool());
        ThreadPoolBuilder("SendBatchThreadPool")
                .set_min_threads(1)
                .set_max_threads(5)
                .set_max_queue_size(100)
                .build(&_env->_send_batch_thread_pool);
        config::tablet_writer_open_rpc_timeout_sec = 60;
        config::max_send_batch_parallelism_per_job = 1;
    }

    void TearDown() override {
        SAFE_DELETE(_env->_internal_client_cache);
        SAFE_DELETE(_env->_function_client_cache);
        SAFE_DELETE(_env->_load_stream_mgr);
        SAFE_DELETE(_env->_master_info);
        SAFE_DELETE(_env->_thread_mgr);
        SAFE_DELETE(_env->_buffer_reservation);
        if (_server) {
            _server->Stop(100);
            _server->Join();
            SAFE_DELETE(_server);
        }
    }

private:
    ExecEnv* _env = nullptr;
    brpc::Server* _server = nullptr;
};

TDataSink get_data_sink(TDescriptorTable* desc_tbl);
TDataSink get_decimal_sink(TDescriptorTable* desc_tbl);

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
                                 google::protobuf::Closure* done) {
        brpc::ClosureGuard done_guard(done);
        {
            std::lock_guard<std::mutex> l(_lock);
            _row_counters += request->tablet_ids_size();
            if (request->eos()) {
                _eof_counters++;
            }
            k_add_batch_status.to_protobuf(response->mutable_status());

            if (request->has_block() && _row_desc != nullptr) {
                brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
                attachment_transfer_request_block<PTabletWriterAddBlockRequest>(request, cntl);
                vectorized::Block block(request->block());

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

TEST_F(VOlapTableSinkTest, normal) {
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

    VOlapTableSink sink(&obj_pool, row_desc, {}, &st);
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

    VOlapTableSink sink(&obj_pool, row_desc, exprs, &st);
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

    VOlapTableSink sink(&obj_pool, row_desc, exprs, &st);
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
    sink.send(&state, &block);
    sink.send(&state, &block);

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

    VOlapTableSink sink(&obj_pool, row_desc, {}, &st);
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
} // namespace stream_load
} // namespace doris
