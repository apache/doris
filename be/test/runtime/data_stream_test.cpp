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

#include <iostream>
#include <thread>

#include "common/status.h"
#include "exprs/slot_ref.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/Descriptors_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/client_cache.h"
#include "runtime/data_stream_mgr.h"
#include "runtime/data_stream_recvr.h"
#include "runtime/data_stream_sender.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/logging.h"
#include "util/mem_info.h"
#include "util/thrift_server.h"

using std::string;
using std::vector;
using std::multiset;

using std::unique_ptr;
using std::thread;

namespace doris {

class DorisTestBackend : public BackendServiceIf {
public:
    DorisTestBackend(DataStreamMgr* stream_mgr) : _mgr(stream_mgr) {}
    virtual ~DorisTestBackend() {}

    virtual void exec_plan_fragment(TExecPlanFragmentResult& return_val,
                                    const TExecPlanFragmentParams& params) {}

    virtual void cancel_plan_fragment(TCancelPlanFragmentResult& return_val,
                                      const TCancelPlanFragmentParams& params) {}

    virtual void transmit_data(TTransmitDataResult& return_val, const TTransmitDataParams& params) {
        /*
        LOG(ERROR) << "transmit_data(): instance_id=" << params.dest_fragment_instance_id
            << " node_id=" << params.dest_node_id
            << " #rows=" << params.row_batch.num_rows
            << " eos=" << (params.eos ? "true" : "false");
        if (!params.eos) {
            _mgr->add_data(
                    params.dest_fragment_instance_id,
                    params.dest_node_id,
                    params.row_batch,
                    params.sender_id).set_t_status(&return_val);
        } else {
            Status status = _mgr->close_sender(
                    params.dest_fragment_instance_id, params.dest_node_id, params.sender_id, params.be_number);
            status.set_t_status(&return_val);
            LOG(ERROR) << "close_sender status: " << status.get_error_msg();
        }
        */
    }

    virtual void fetch_data(TFetchDataResult& return_val, const TFetchDataParams& params) {}

    virtual void submit_tasks(TAgentResult& return_val,
                              const std::vector<TAgentTaskRequest>& tasks) {}

    virtual void make_snapshot(TAgentResult& return_val, const TSnapshotRequest& snapshot_request) {
    }

    virtual void release_snapshot(TAgentResult& return_val, const std::string& snapshot_path) {}

    virtual void publish_cluster_state(TAgentResult& return_val,
                                       const TAgentPublishRequest& request) {}

    virtual void submit_etl_task(TAgentResult& return_val, const TMiniLoadEtlTaskRequest& request) {
    }

    virtual void get_etl_status(TMiniLoadEtlStatusResult& return_val,
                                const TMiniLoadEtlStatusRequest& request) {}

    virtual void delete_etl_files(TAgentResult& return_val, const TDeleteEtlFilesRequest& request) {
    }

    virtual void register_pull_load_task(TStatus& _return, const TUniqueId& id,
                                         const int32_t num_senders) {}

    virtual void deregister_pull_load_task(TStatus& _return, const TUniqueId& id) {}

    virtual void report_pull_load_sub_task_info(TStatus& _return,
                                                const TPullLoadSubTaskInfo& task_info) {}

    virtual void fetch_pull_load_task_info(TFetchPullLoadTaskInfoResult& _return,
                                           const TUniqueId& id) {}

    virtual void fetch_all_pull_load_task_infos(TFetchAllPullLoadTaskInfosResult& _return) {}

private:
    DataStreamMgr* _mgr;
};

class DataStreamTest : public testing::Test {
protected:
    DataStreamTest()
            : _limit(new MemTracker(-1)),
              _runtime_state(TUniqueId(), TQueryOptions(), "", &_exec_env),
              _next_val(0) {
        _exec_env.init_for_tests();
        _runtime_state.init_mem_trackers(TUniqueId());
    }
    // null dtor to pass codestyle check
    ~DataStreamTest() {}

    virtual void SetUp() {
        create_row_desc();
        create_tuple_comparator();
        create_row_batch();

        _next_instance_id.lo = 0;
        _next_instance_id.hi = 0;
        _stream_mgr = new DataStreamMgr();

        _broadcast_sink.dest_node_id = DEST_NODE_ID;
        _broadcast_sink.output_partition.type = TPartitionType::UNPARTITIONED;

        _random_sink.dest_node_id = DEST_NODE_ID;
        _random_sink.output_partition.type = TPartitionType::RANDOM;

        _hash_sink.dest_node_id = DEST_NODE_ID;
        _hash_sink.output_partition.type = TPartitionType::HASH_PARTITIONED;
        // there's only one column to partition on
        TExprNode expr_node;
        expr_node.node_type = TExprNodeType::SLOT_REF;
        expr_node.type.types.push_back(TTypeNode());
        expr_node.type.types.back().__isset.scalar_type = true;
        expr_node.type.types.back().scalar_type.type = TPrimitiveType::BIGINT;
        expr_node.num_children = 0;
        TSlotRef slot_ref;
        slot_ref.slot_id = 0;
        expr_node.__set_slot_ref(slot_ref);
        TExpr expr;
        expr.nodes.push_back(expr_node);
        _hash_sink.output_partition.__isset.partition_exprs = true;
        _hash_sink.output_partition.partition_exprs.push_back(expr);

        // Ensure that individual sender info addresses don't change
        _sender_info.reserve(MAX_SENDERS);
        _receiver_info.reserve(MAX_RECEIVERS);
        start_backend();
    }

    const TDataStreamSink& get_sink(TPartitionType::type partition_type) {
        switch (partition_type) {
        case TPartitionType::UNPARTITIONED:
            return _broadcast_sink;
        case TPartitionType::RANDOM:
            return _random_sink;
        case TPartitionType::HASH_PARTITIONED:
            return _hash_sink;
        default:
            DCHECK(false) << "Unhandled sink type: " << partition_type;
        }
        // Should never reach this.
        return _broadcast_sink;
    }

    virtual void TearDown() {
        _lhs_slot_ctx->close(nullptr);
        _rhs_slot_ctx->close(nullptr);
        _exec_env.client_cache()->test_shutdown();
        stop_backend();
    }

    void reset() {
        _sender_info.clear();
        _receiver_info.clear();
        _dest.clear();
    }

    // We reserve contiguous memory for senders in SetUp. If a test uses more
    // senders, a DCHECK will fail and you should increase this value.
    static const int MAX_SENDERS = 16;
    static const int MAX_RECEIVERS = 16;
    static const PlanNodeId DEST_NODE_ID = 1;
    static const int BATCH_CAPACITY = 100; // rows
    static const int PER_ROW_DATA = 8;
    static const int TOTAL_DATA_SIZE = 8 * 1024;
    static const int NUM_BATCHES = TOTAL_DATA_SIZE / BATCH_CAPACITY / PER_ROW_DATA;

    ObjectPool _obj_pool;
    std::shared_ptr<MemTracker> _limit;
    std::shared_ptr<MemTracker> _tracker;
    DescriptorTbl* _desc_tbl;
    const RowDescriptor* _row_desc;
    TupleRowComparator* _less_than;
    ExecEnv _exec_env;
    RuntimeState _runtime_state;
    TUniqueId _next_instance_id;
    string _stmt;

    // RowBatch generation
    std::unique_ptr<RowBatch> _batch;
    int _next_val;
    int64_t* _tuple_mem;

    // receiving node
    DataStreamMgr* _stream_mgr;
    ThriftServer* _server;

    // sending node(s)
    TDataStreamSink _broadcast_sink;
    TDataStreamSink _random_sink;
    TDataStreamSink _hash_sink;
    std::vector<TPlanFragmentDestination> _dest;

    struct SenderInfo {
        thread* thread_handle;
        Status status;
        int num_bytes_sent;

        SenderInfo() : thread_handle(nullptr), num_bytes_sent(0) {}
    };
    std::vector<SenderInfo> _sender_info;

    struct ReceiverInfo {
        TPartitionType::type stream_type;
        int num_senders;
        int receiver_num;

        thread* thread_handle;
        std::shared_ptr<DataStreamRecvr> stream_recvr;
        Status status;
        int num_rows_received;
        multiset<int64_t> data_values;

        ReceiverInfo(TPartitionType::type stream_type, int num_senders, int receiver_num)
                : stream_type(stream_type),
                  num_senders(num_senders),
                  receiver_num(receiver_num),
                  thread_handle(nullptr),
                  stream_recvr(nullptr),
                  num_rows_received(0) {}

        ~ReceiverInfo() {
            delete thread_handle;
            stream_recvr.reset();
        }
    };
    std::vector<ReceiverInfo> _receiver_info;

    // Create an instance id and add it to _dest
    void get_next_instance_id(TUniqueId* instance_id) {
        _dest.push_back(TPlanFragmentDestination());
        TPlanFragmentDestination& dest = _dest.back();
        dest.fragment_instance_id = _next_instance_id;
        dest.server.hostname = "127.0.0.1";
        dest.server.port = config::port;
        *instance_id = _next_instance_id;
        ++_next_instance_id.lo;
    }

    // RowDescriptor to mimic "select bigint_col from alltypesagg", except the slot
    // isn't nullable
    void create_row_desc() {
        // create DescriptorTbl
        TTupleDescriptor tuple_desc;
        tuple_desc.__set_id(0);
        tuple_desc.__set_byteSize(8);
        tuple_desc.__set_numNullBytes(0);
        TDescriptorTable thrift_desc_tbl;
        thrift_desc_tbl.tupleDescriptors.push_back(tuple_desc);
        TSlotDescriptor slot_desc;
        slot_desc.__set_id(0);
        slot_desc.__set_parent(0);

        slot_desc.slotType.types.push_back(TTypeNode());
        slot_desc.slotType.types.back().__isset.scalar_type = true;
        slot_desc.slotType.types.back().scalar_type.type = TPrimitiveType::BIGINT;

        slot_desc.__set_columnPos(0);
        slot_desc.__set_byteOffset(0);
        slot_desc.__set_nullIndicatorByte(0);
        slot_desc.__set_nullIndicatorBit(-1);
        slot_desc.__set_slotIdx(0);
        slot_desc.__set_isMaterialized(true);
        thrift_desc_tbl.slotDescriptors.push_back(slot_desc);
        EXPECT_TRUE(DescriptorTbl::create(&_obj_pool, thrift_desc_tbl, &_desc_tbl).ok());
        _runtime_state.set_desc_tbl(_desc_tbl);

        std::vector<TTupleId> row_tids;
        row_tids.push_back(0);

        std::vector<bool> nullable_tuples;
        nullable_tuples.push_back(false);
        _row_desc = _obj_pool.add(new RowDescriptor(*_desc_tbl, row_tids, nullable_tuples));
    }

    // Create a tuple comparator to sort in ascending order on the single bigint column.
    void create_tuple_comparator() {
        TExprNode expr_node;
        expr_node.node_type = TExprNodeType::SLOT_REF;
        expr_node.type.types.push_back(TTypeNode());
        expr_node.type.types.back().__isset.scalar_type = true;
        expr_node.type.types.back().scalar_type.type = TPrimitiveType::BIGINT;
        expr_node.num_children = 0;
        TSlotRef slot_ref;
        slot_ref.slot_id = 0;
        expr_node.__set_slot_ref(slot_ref);

        SlotRef* lhs_slot = _obj_pool.add(new SlotRef(expr_node));
        _lhs_slot_ctx = _obj_pool.add(new ExprContext(lhs_slot));
        SlotRef* rhs_slot = _obj_pool.add(new SlotRef(expr_node));
        _rhs_slot_ctx = _obj_pool.add(new ExprContext(rhs_slot));

        _lhs_slot_ctx->prepare(&_runtime_state, *_row_desc, _tracker.get());
        _rhs_slot_ctx->prepare(&_runtime_state, *_row_desc, _tracker.get());
        _lhs_slot_ctx->open(nullptr);
        _rhs_slot_ctx->open(nullptr);
        SortExecExprs* sort_exprs = _obj_pool.add(new SortExecExprs());
        sort_exprs->init(vector<ExprContext*>(1, _lhs_slot_ctx),
                         std::vector<ExprContext*>(1, _rhs_slot_ctx));
        _less_than = _obj_pool.add(new TupleRowComparator(*sort_exprs, std::vector<bool>(1, true),
                                                          std::vector<bool>(1, false)));
    }

    // Create _batch, but don't fill it with data yet. Assumes we created _row_desc.
    RowBatch* create_row_batch() {
        RowBatch* batch = new RowBatch(*_row_desc, BATCH_CAPACITY, _limit.get());
        int64_t* tuple_mem =
                reinterpret_cast<int64_t*>(batch->tuple_data_pool()->allocate(BATCH_CAPACITY * 8));
        bzero(tuple_mem, BATCH_CAPACITY * 8);

        for (int i = 0; i < BATCH_CAPACITY; ++i) {
            int idx = batch->add_row();
            TupleRow* row = batch->get_row(idx);
            row->set_tuple(0, reinterpret_cast<Tuple*>(&tuple_mem[i]));
            batch->commit_last_row();
        }

        return batch;
    }

    void get_next_batch(RowBatch* batch, int* next_val) {
        LOG(INFO) << "batch_capacity=" << BATCH_CAPACITY << " next_val=" << *next_val;
        for (int i = 0; i < BATCH_CAPACITY; ++i) {
            TupleRow* row = batch->get_row(i);
            int64_t* val = reinterpret_cast<int64_t*>(row->get_tuple(0)->get_slot(0));
            *val = (*next_val)++;
        }
    }

    // Start receiver (expecting given number of senders) in separate thread.
    void start_receiver(TPartitionType::type stream_type, int num_senders, int receiver_num,
                        int buffer_size, bool is_merging, TUniqueId* out_id = nullptr) {
        VLOG_QUERY << "start receiver";
        RuntimeProfile* profile = _obj_pool.add(new RuntimeProfile("TestReceiver"));
        TUniqueId instance_id;
        get_next_instance_id(&instance_id);
        _receiver_info.push_back(ReceiverInfo(stream_type, num_senders, receiver_num));
        ReceiverInfo& info = _receiver_info.back();
        info.stream_recvr =
                _stream_mgr->create_recvr(&_runtime_state, *_row_desc, instance_id, DEST_NODE_ID,
                                          num_senders, buffer_size, profile, is_merging);
        if (!is_merging) {
            info.thread_handle = new thread(&DataStreamTest::read_stream, this, &info);
        } else {
            info.thread_handle =
                    new thread(&DataStreamTest::read_stream_merging, this, &info, profile);
        }

        if (out_id != nullptr) {
            *out_id = instance_id;
        }
    }

    void join_receivers() {
        VLOG_QUERY << "join receiver\n";

        for (int i = 0; i < _receiver_info.size(); ++i) {
            _receiver_info[i].thread_handle->join();
            _receiver_info[i].stream_recvr->close();
        }
    }

    // Deplete stream and print batches
    void read_stream(ReceiverInfo* info) {
        RowBatch* batch = nullptr;
        VLOG_QUERY << "start reading";

        while (!(info->status = info->stream_recvr->get_batch(&batch)).is_cancelled() &&
               (batch != nullptr)) {
            VLOG_QUERY << "read batch #rows=" << (batch != nullptr ? batch->num_rows() : 0);

            for (int i = 0; i < batch->num_rows(); ++i) {
                TupleRow* row = batch->get_row(i);
                info->data_values.insert(*static_cast<int64_t*>(row->get_tuple(0)->get_slot(0)));
            }

            SleepFor(MonoDelta::FromMilliseconds(
                    10)); // slow down receiver to exercise buffering logic
        }

        if (info->status.is_cancelled()) {
            VLOG_QUERY << "reader is cancelled";
        }

        VLOG_QUERY << "done reading";
    }

    void read_stream_merging(ReceiverInfo* info, RuntimeProfile* profile) {
        info->status = info->stream_recvr->create_merger(*_less_than);
        if (info->status.is_cancelled()) {
            return;
        }
        RowBatch batch(*_row_desc, 1024, _limit.get());
        VLOG_QUERY << "start reading merging";
        bool eos = false;
        while (!(info->status = info->stream_recvr->get_next(&batch, &eos)).is_cancelled()) {
            VLOG_QUERY << "read batch #rows=" << batch.num_rows();
            for (int i = 0; i < batch.num_rows(); ++i) {
                TupleRow* row = batch.get_row(i);
                info->data_values.insert(*static_cast<int64_t*>(row->get_tuple(0)->get_slot(0)));
            }
            SleepFor(MonoDelta::FromMilliseconds(
                    10)); // slow down receiver to exercise buffering logic
            batch.reset();
            if (eos) {
                break;
            }
        }
        if (info->status.is_cancelled()) {
            VLOG_QUERY << "reader is cancelled";
        }
        VLOG_QUERY << "done reading";
    }

    // Verify correctness of receivers' data values.
    void check_receivers(TPartitionType::type stream_type, int num_senders) {
        int64_t total = 0;
        multiset<int64_t> all_data_values;

        for (int i = 0; i < _receiver_info.size(); ++i) {
            ReceiverInfo& info = _receiver_info[i];
            EXPECT_TRUE(info.status.ok());
            total += info.data_values.size();
            DCHECK_EQ(info.stream_type, stream_type);
            DCHECK_EQ(info.num_senders, num_senders);

            if (stream_type == TPartitionType::UNPARTITIONED) {
                EXPECT_EQ(NUM_BATCHES * BATCH_CAPACITY * num_senders, info.data_values.size());
            }

            all_data_values.insert(info.data_values.begin(), info.data_values.end());

            int k = 0;
            for (multiset<int64_t>::iterator j = info.data_values.begin();
                 j != info.data_values.end(); ++j, ++k) {
                if (stream_type == TPartitionType::UNPARTITIONED) {
                    // unpartitioned streams contain all values as many times as there are
                    // senders
                    EXPECT_EQ(k / num_senders, *j);
                } else if (stream_type == TPartitionType::HASH_PARTITIONED) {
                    // hash-partitioned streams send values to the right partition
                    int64_t value = *j;
                    uint32_t hash_val = RawValue::get_hash_value_fvn(&value, TYPE_BIGINT, 0U);
                    EXPECT_EQ(hash_val % _receiver_info.size(), info.receiver_num);
                }
            }
        }

        if (stream_type == TPartitionType::HASH_PARTITIONED) {
            EXPECT_EQ(NUM_BATCHES * BATCH_CAPACITY * num_senders, total);

            int k = 0;
            for (multiset<int64_t>::iterator j = all_data_values.begin();
                 j != all_data_values.end(); ++j, ++k) {
                // each sender sent all values
                EXPECT_EQ(k / num_senders, *j);

                if (k / num_senders != *j) {
                    break;
                }
            }
        }
    }

    void check_senders() {
        for (int i = 0; i < _sender_info.size(); ++i) {
            EXPECT_TRUE(_sender_info[i].status.ok());
            EXPECT_GT(_sender_info[i].num_bytes_sent, 0) << "info  i=" << i;
        }
    }

    // Start backend in separate thread.
    void start_backend() {
        std::shared_ptr<DorisTestBackend> handler(new DorisTestBackend(_stream_mgr));
        std::shared_ptr<apache::thrift::TProcessor> processor(new BackendServiceProcessor(handler));
        _server = new ThriftServer("DataStreamTest backend", processor, config::port, nullptr);
        _server->start();
    }

    void stop_backend() {
        VLOG_QUERY << "stop backend\n";
        _server->stop_for_testing();
        delete _server;
    }

    void start_sender(TPartitionType::type partition_type = TPartitionType::UNPARTITIONED,
                      int channel_buffer_size = 1024) {
        VLOG_QUERY << "start sender";
        int sender_id = _sender_info.size();
        DCHECK_LT(sender_id, MAX_SENDERS);
        _sender_info.push_back(SenderInfo());
        SenderInfo& info = _sender_info.back();
        info.thread_handle = new thread(&DataStreamTest::sender, this, sender_id,
                                        channel_buffer_size, partition_type);
    }

    void join_senders() {
        VLOG_QUERY << "join senders\n";
        for (int i = 0; i < _sender_info.size(); ++i) {
            _sender_info[i].thread_handle->join();
        }
    }

    void sender(int sender_num, int channel_buffer_size, TPartitionType::type partition_type) {
        RuntimeState state(TExecPlanFragmentParams(), TQueryOptions(), "", &_exec_env);
        state.set_desc_tbl(_desc_tbl);
        state.init_mem_trackers(TUniqueId());
        VLOG_QUERY << "create sender " << sender_num;
        const TDataStreamSink& stream_sink =
                (partition_type == TPartitionType::UNPARTITIONED ? _broadcast_sink : _hash_sink);
        DataStreamSender sender(&_obj_pool, sender_num, *_row_desc, stream_sink, _dest,
                                channel_buffer_size);

        TDataSink data_sink;
        data_sink.__set_type(TDataSinkType::DATA_STREAM_SINK);
        data_sink.__set_stream_sink(stream_sink);
        EXPECT_TRUE(sender.init(data_sink).ok());

        EXPECT_TRUE(sender.prepare(&state).ok());
        EXPECT_TRUE(sender.open(&state).ok());
        std::unique_ptr<RowBatch> batch(create_row_batch());
        SenderInfo& info = _sender_info[sender_num];
        int next_val = 0;

        for (int i = 0; i < NUM_BATCHES; ++i) {
            get_next_batch(batch.get(), &next_val);
            VLOG_QUERY << "sender " << sender_num << ": #rows=" << batch->num_rows();
            info.status = sender.send(&state, batch.get());

            if (!info.status.ok()) {
                LOG(WARNING) << "something is wrong when sending: " << info.status.get_error_msg();
                break;
            }
        }

        VLOG_QUERY << "closing sender" << sender_num;
        info.status = sender.close(&state, Status::OK());
        info.num_bytes_sent = sender.get_num_data_bytes_sent();

        batch->reset();
    }

    void test_stream(TPartitionType::type stream_type, int num_senders, int num_receivers,
                     int buffer_size, bool is_merging) {
        LOG(INFO) << "Testing stream=" << stream_type << " #senders=" << num_senders
                  << " #receivers=" << num_receivers << " buffer_size=" << buffer_size;
        reset();

        for (int i = 0; i < num_receivers; ++i) {
            start_receiver(stream_type, num_senders, i, buffer_size, is_merging);
        }

        for (int i = 0; i < num_senders; ++i) {
            start_sender(stream_type, buffer_size);
        }

        join_senders();
        check_senders();
        join_receivers();
        check_receivers(stream_type, num_senders);
    }

private:
    ExprContext* _lhs_slot_ctx;
    ExprContext* _rhs_slot_ctx;
};

TEST_F(DataStreamTest, UnknownSenderSmallResult) {
    // starting a sender w/o a corresponding receiver does not result in an error because
    // we cannot distinguish whether a receiver was never created or the receiver
    // willingly tore down the stream
    // case 1: entire query result fits in single buffer, close() returns ok
    TUniqueId dummy_id;
    get_next_instance_id(&dummy_id);
    start_sender(TPartitionType::UNPARTITIONED, TOTAL_DATA_SIZE + 1024);
    join_senders();
    EXPECT_TRUE(_sender_info[0].status.ok());
    EXPECT_GT(_sender_info[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, UnknownSenderLargeResult) {
    // case 2: query result requires multiple buffers, send() returns ok
    TUniqueId dummy_id;
    get_next_instance_id(&dummy_id);
    start_sender();
    join_senders();
    EXPECT_TRUE(_sender_info[0].status.ok());
    EXPECT_GT(_sender_info[0].num_bytes_sent, 0);
}

TEST_F(DataStreamTest, Cancel) {
    TUniqueId instance_id;
    start_receiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, false, &instance_id);
    _stream_mgr->cancel(instance_id);
    start_receiver(TPartitionType::UNPARTITIONED, 1, 1, 1024, true, &instance_id);
    _stream_mgr->cancel(instance_id);
    join_receivers();
    EXPECT_TRUE(_receiver_info[0].status.is_cancelled());
}

TEST_F(DataStreamTest, BasicTest) {
    // TODO: also test that all client connections have been returned
    TPartitionType::type stream_types[] = {TPartitionType::UNPARTITIONED,
                                           TPartitionType::HASH_PARTITIONED};
    int sender_nums[] = {1, 3};
    int receiver_nums[] = {1, 3};
    int buffer_sizes[] = {1024, 1024 * 1024};
    bool merging[] = {false, true};

    // test_stream(TPartitionType::HASH_PARTITIONED, 1, 3, 1024, true);
    for (int i = 0; i < sizeof(stream_types) / sizeof(*stream_types); ++i) {
        for (int j = 0; j < sizeof(sender_nums) / sizeof(int); ++j) {
            for (int k = 0; k < sizeof(receiver_nums) / sizeof(int); ++k) {
                for (int l = 0; l < sizeof(buffer_sizes) / sizeof(int); ++l) {
                    for (int m = 0; m < sizeof(merging) / sizeof(bool); ++m) {
                        LOG(ERROR) << "before test: stream_type=" << stream_types[i]
                                   << "  sender num=" << sender_nums[j]
                                   << "  receiver_num=" << receiver_nums[k]
                                   << "  buffer_size=" << buffer_sizes[l]
                                   << "  merging=" << (merging[m] ? "true" : "false");
                        test_stream(stream_types[i], sender_nums[j], receiver_nums[k],
                                    buffer_sizes[l], merging[m]);
                        LOG(ERROR) << "after test: stream_type=" << stream_types[i]
                                   << "  sender num=" << sender_nums[j]
                                   << "  receiver_num=" << receiver_nums[k]
                                   << "  buffer_size=" << buffer_sizes[l]
                                   << "  merging=" << (merging[m] ? "true" : "false");
                    }
                }
            }
        }
    }
}

// TODO: more tests:
// - test case for transmission error in last batch
// - receivers getting created concurrently

} // namespace doris

int main(int argc, char** argv) {
    // std::string conffile = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    // if (!doris::config::init(conffile.c_str(), false)) {
    //     fprintf(stderr, "error read config file. conffile path= %s\n", conffile.c_str());
    //     return -1;
    // }
    doris::config::query_scratch_dirs = "/tmp";
    doris::config::max_free_io_buffers = 128;
    doris::config::disable_mem_pools = false;
    doris::config::min_buffer_size = 1024;
    doris::config::read_size = 8388608;
    doris::config::port = 2001;
    doris::config::thrift_connect_timeout_seconds = 20;

    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);

    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();

    return RUN_ALL_TESTS();
}
