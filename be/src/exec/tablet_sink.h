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

#pragma once

#include <memory>
#include <queue>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "util/bitmap.h"
#include "util/countdown_latch.h"
#include "util/ref_count_closure.h"
#include "util/spinlock.h"
#include "util/thread.h"
#include "util/thrift_util.h"

namespace doris {

class Bitmap;
class MemTracker;
class RuntimeProfile;
class RowDescriptor;
class Tuple;
class TupleDescriptor;
class ExprContext;
class TExpr;

namespace stream_load {

class OlapTableSink;

// The counter of add_batch rpc of a single node
struct AddBatchCounter {
    // total execution time of a add_batch rpc
    int64_t add_batch_execution_time_us = 0;
    // lock waiting time in a add_batch rpc
    int64_t add_batch_wait_lock_time_us = 0;
    // number of add_batch call
    int64_t add_batch_num = 0;
    AddBatchCounter& operator+=(const AddBatchCounter& rhs) {
        add_batch_execution_time_us += rhs.add_batch_execution_time_us;
        add_batch_wait_lock_time_us += rhs.add_batch_wait_lock_time_us;
        add_batch_num += rhs.add_batch_num;
        return *this;
    }
    friend AddBatchCounter operator+(const AddBatchCounter& lhs, const AddBatchCounter& rhs) {
        AddBatchCounter sum = lhs;
        sum += rhs;
        return sum;
    }
};

// It's very error-prone to guarantee the handler capture vars' & this closure's destruct sequence.
// So using create() to get the closure pointer is recommended. We can delete the closure ptr before the capture vars destruction.
// Delete this point is safe, don't worry about RPC callback will run after ReusableClosure deleted.
template <typename T>
class ReusableClosure : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID) {
    }
    ~ReusableClosure() {
        // shouldn't delete when Run() is calling or going to be called, wait for current Run() done.
        join();
    }

    static ReusableClosure<T>* create() { return new ReusableClosure<T>(); }

    void addFailedHandler(std::function<void()> fn) { failed_handler = fn; }
    void addSuccessHandler(std::function<void(const T&, bool)> fn) { success_handler = fn; }

    void join() {
        if (cid != INVALID_BTHREAD_ID) {
            brpc::Join(cid);
        }
    }

    // plz follow this order: reset() -> set_in_flight() -> send brpc batch
    void reset() {
        join();
        DCHECK(_packet_in_flight == false);
        cntl.Reset();
        cid = cntl.call_id();
    }

    void set_in_flight() {
        DCHECK(_packet_in_flight == false);
        _packet_in_flight = true;
    }

    bool is_packet_in_flight() { return _packet_in_flight; }

    void end_mark() {
        DCHECK(_is_last_rpc == false);
        _is_last_rpc = true;
    }

    void Run() override {
        DCHECK(_packet_in_flight);
        if (cntl.Failed()) {
            LOG(WARNING) << "failed to send brpc batch, error=" << berror(cntl.ErrorCode())
                         << ", error_text=" << cntl.ErrorText();
            failed_handler();
        } else {
            success_handler(result, _is_last_rpc);
        }
        _packet_in_flight = false;
    }

    brpc::Controller cntl;
    T result;

private:
    brpc::CallId cid;
    std::atomic<bool> _packet_in_flight{false};
    std::atomic<bool> _is_last_rpc{false};
    std::function<void()> failed_handler;
    std::function<void(const T&, bool)> success_handler;
};

class NodeChannel {
public:
    NodeChannel(OlapTableSink* parent, int64_t index_id, int64_t node_id, int32_t schema_hash);
    ~NodeChannel() noexcept;

    // called before open, used to add tablet located in this backend
    void add_tablet(const TTabletWithPartition& tablet) { _all_tablets.emplace_back(tablet); }

    Status init(RuntimeState* state);

    // we use open/open_wait to parallel
    void open();
    Status open_wait();

    Status add_row(Tuple* tuple, int64_t tablet_id);

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    Status mark_close();
    Status close_wait(RuntimeState* state);

    void cancel();

    // return:
    // 0: stopped, send finished(eos request has been sent), or any internal error;
    // 1: running, haven't reach eos.
    // only allow 1 rpc in flight
    // plz make sure, this func should be called after open_wait().
    int try_send_and_fetch_status();

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map,
                     int64_t* serialize_batch_ns, int64_t* mem_exceeded_block_ns,
                     int64_t* queue_push_lock_ns, int64_t* actual_consume_ns,
                     int64_t* total_add_batch_exec_time_ns, int64_t* add_batch_exec_time_ns,
                     int64_t* total_add_batch_num) {
        (*add_batch_counter_map)[_node_id] += _add_batch_counter;
        *serialize_batch_ns += _serialize_batch_ns;
        *mem_exceeded_block_ns += _mem_exceeded_block_ns;
        *queue_push_lock_ns += _queue_push_lock_ns;
        *actual_consume_ns += _actual_consume_ns;
        *add_batch_exec_time_ns = (_add_batch_counter.add_batch_execution_time_us * 1000);
        *total_add_batch_exec_time_ns += *add_batch_exec_time_ns;
        *total_add_batch_num += _add_batch_counter.add_batch_num;
    }

    int64_t node_id() const { return _node_id; }
    const NodeInfo* node_info() const { return _node_info; }
    std::string print_load_info() const { return _load_info; }
    std::string name() const { return _name; }

    Status none_of(std::initializer_list<bool> vars);

    // TODO(HW): remove after mem tracker shared
    void clear_all_batches();

private:
    OlapTableSink* _parent = nullptr;
    int64_t _index_id = -1;
    int64_t _node_id = -1;
    int32_t _schema_hash = 0;
    std::string _load_info;
    std::string _name;

    TupleDescriptor* _tuple_desc = nullptr;
    const NodeInfo* _node_info = nullptr;

    // this should be set in init() using config
    int _rpc_timeout_ms = 60000;
    int64_t _next_packet_seq = 0;

    // user cancel or get some errors
    std::atomic<bool> _cancelled{false};
    SpinLock _cancel_msg_lock;
    std::string _cancel_msg = "";

    // send finished means the consumer thread which send the rpc can exit
    std::atomic<bool> _send_finished{false};

    // add batches finished means the last rpc has be response, used to check whether this channel can be closed
    std::atomic<bool> _add_batches_finished{false};

    bool _eos_is_produced{false}; // only for restricting producer behaviors

    std::unique_ptr<RowDescriptor> _row_desc;
    int _batch_size = 0;
    std::unique_ptr<RowBatch> _cur_batch;
    PTabletWriterAddBatchRequest _cur_add_batch_request;

    std::mutex _pending_batches_lock;
    using AddBatchReq = std::pair<std::unique_ptr<RowBatch>, PTabletWriterAddBatchRequest>;
    std::queue<AddBatchReq> _pending_batches;
    std::atomic<int> _pending_batches_num{0};

    PBackendService_Stub* _stub = nullptr;
    RefCountClosure<PTabletWriterOpenResult>* _open_closure = nullptr;
    ReusableClosure<PTabletWriterAddBatchResult>* _add_batch_closure = nullptr;

    std::vector<TTabletWithPartition> _all_tablets;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    AddBatchCounter _add_batch_counter;
    std::atomic<int64_t> _serialize_batch_ns{0};
    std::atomic<int64_t> _mem_exceeded_block_ns{0};
    std::atomic<int64_t> _queue_push_lock_ns{0};
    std::atomic<int64_t> _actual_consume_ns{0};
};

class IndexChannel {
public:
    IndexChannel(OlapTableSink* parent, int64_t index_id, int32_t schema_hash)
            : _parent(parent), _index_id(index_id), _schema_hash(schema_hash) {}
    ~IndexChannel();

    Status init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets);

    Status add_row(Tuple* tuple, int64_t tablet_id);

    void for_each_node_channel(const std::function<void(NodeChannel*)>& func) {
        for (auto& it : _node_channels) {
            func(it.second);
        }
    }

    void mark_as_failed(const NodeChannel* ch) { _failed_channels.insert(ch->node_id()); }
    bool has_intolerable_failure();

    size_t num_node_channels() const { return _node_channels.size(); }

private:
    OlapTableSink* _parent;
    int64_t _index_id;
    int32_t _schema_hash;

    // BeId -> channel
    std::unordered_map<int64_t, NodeChannel*> _node_channels;
    // from tablet_id to backend channel
    std::unordered_map<int64_t, std::vector<NodeChannel*>> _channels_by_tablet;
    // BeId
    std::set<int64_t> _failed_channels;
};

// Write data to Olap Table.
// When OlapTableSink::open() called, there will be a consumer thread running in the background.
// When you call OlapTableSink::send(), you will be the producer who products pending batches.
// Join the consumer thread in close().
class OlapTableSink : public DataSink {
public:
    // Construct from thrift struct which is generated by FE.
    OlapTableSink(ObjectPool* pool, const RowDescriptor& row_desc, const std::vector<TExpr>& texprs,
                  Status* status);
    ~OlapTableSink() override;

    Status init(const TDataSink& sink) override;

    Status prepare(RuntimeState* state) override;

    Status open(RuntimeState* state) override;

    Status send(RuntimeState* state, RowBatch* batch) override;

    // close() will send RPCs too. If RPCs failed, return error.
    Status close(RuntimeState* state, Status close_status) override;

    // Returns the runtime profile for the sink.
    RuntimeProfile* profile() override { return _profile; }

private:
    // convert input batch to output batch which will be loaded into OLAP table.
    // this is only used in insert statement.
    void _convert_batch(RuntimeState* state, RowBatch* input_batch, RowBatch* output_batch);

    // make input data valid for OLAP table
    // return number of invalid/filtered rows.
    // invalid row number is set in Bitmap
    int _validate_data(RuntimeState* state, RowBatch* batch, Bitmap* filter_bitmap);

    // the consumer func of sending pending batches in every NodeChannel.
    // use polling & NodeChannel::try_send_and_fetch_status() to achieve nonblocking sending.
    // only focus on pending batches and channel status, the internal errors of NodeChannels will be handled by the producer
    void _send_batch_process();

private:
    friend class NodeChannel;
    friend class IndexChannel;

    std::shared_ptr<MemTracker> _mem_tracker;

    ObjectPool* _pool;
    const RowDescriptor& _input_row_desc;

    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    int _num_replicas = -1;
    bool _need_gen_rollup = false;
    int _tuple_desc_id = -1;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    RowDescriptor* _output_row_desc = nullptr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::unique_ptr<RowBatch> _output_batch;

    bool _need_validate_data = false;

    // number of senders used to insert into OlapTable, if we only support single node insert,
    // all data from select should collectted and then send to OlapTable.
    // To support multiple senders, we maintain a channel for each sender.
    int _sender_id = -1;
    int _num_senders = -1;

    // TODO(zc): think about cache this data
    std::shared_ptr<OlapTableSchemaParam> _schema;
    OlapTablePartitionParam* _partition = nullptr;
    OlapTableLocationParam* _location = nullptr;
    DorisNodesInfo* _nodes_info = nullptr;

    RuntimeProfile* _profile = nullptr;

    std::set<int64_t> _partition_ids;

    Bitmap _filter_bitmap;

    // index_channel
    std::vector<IndexChannel*> _channels;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _sender_thread;

    std::vector<DecimalValue> _max_decimal_val;
    std::vector<DecimalValue> _min_decimal_val;

    std::vector<DecimalV2Value> _max_decimalv2_val;
    std::vector<DecimalV2Value> _min_decimalv2_val;

    // Stats for this
    int64_t _convert_batch_ns = 0;
    int64_t _validate_data_ns = 0;
    int64_t _send_data_ns = 0;
    int64_t _serialize_batch_ns = 0;
    int64_t _number_input_rows = 0;
    int64_t _number_output_rows = 0;
    int64_t _number_filtered_rows = 0;

    RuntimeProfile::Counter* _input_rows_counter = nullptr;
    RuntimeProfile::Counter* _output_rows_counter = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _wait_mem_limit_timer = nullptr;
    RuntimeProfile::Counter* _convert_batch_timer = nullptr;
    RuntimeProfile::Counter* _validate_data_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_work_timer = nullptr;
    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;
    RuntimeProfile::Counter* _total_add_batch_exec_timer = nullptr;
    RuntimeProfile::Counter* _max_add_batch_exec_timer = nullptr;
    RuntimeProfile::Counter* _add_batch_number = nullptr;
    RuntimeProfile::Counter* _num_node_channels = nullptr;

    // load mem limit is for remote load channel
    int64_t _load_mem_limit = -1;

    // the timeout of load channels opened by this tablet sink. in second
    int64_t _load_channel_timeout_s = 0;

	// True if this sink has been closed once
	bool _is_closed = false;
	// Save the status of close() method
	Status _close_status;
};

} // namespace stream_load
} // namespace doris
