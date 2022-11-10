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

#include <fmt/format.h>

#include <memory>
#include <queue>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/thread_context.h"
#include "util/bitmap.h"
#include "util/countdown_latch.h"
#include "util/ref_count_closure.h"
#include "util/spinlock.h"
#include "util/thread.h"

namespace doris {

class Bitmap;
class MemTracker;
class RuntimeProfile;
class RowDescriptor;
class ThreadPool;
class ThreadPoolToken;
class Tuple;
class TupleDescriptor;
class ExprContext;
class TExpr;

namespace vectorized {
class Block;
class MutableBlock;
} // namespace vectorized
namespace stream_load {

class OlapTableSink;

// The counter of add_batch rpc of a single node
struct AddBatchCounter {
    // total execution time of a add_batch rpc
    int64_t add_batch_execution_time_us = 0;
    // lock waiting time in a add_batch rpc
    int64_t add_batch_wait_execution_time_us = 0;
    // number of add_batch call
    int64_t add_batch_num = 0;
    // time passed between marked close and finish close
    int64_t close_wait_time_ms = 0;

    AddBatchCounter& operator+=(const AddBatchCounter& rhs) {
        add_batch_execution_time_us += rhs.add_batch_execution_time_us;
        add_batch_wait_execution_time_us += rhs.add_batch_wait_execution_time_us;
        add_batch_num += rhs.add_batch_num;
        close_wait_time_ms += rhs.close_wait_time_ms;
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
class ReusableClosure final : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID) {}
    ~ReusableClosure() override {
        // shouldn't delete when Run() is calling or going to be called, wait for current Run() done.
        join();
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        cntl.Reset();
    }

    static ReusableClosure<T>* create() { return new ReusableClosure<T>(); }

    void addFailedHandler(const std::function<void(bool)>& fn) { failed_handler = fn; }
    void addSuccessHandler(const std::function<void(const T&, bool)>& fn) { success_handler = fn; }

    void join() {
        // We rely on in_flight to assure one rpc is running,
        // while cid is not reliable due to memory order.
        // in_flight is written before getting callid,
        // so we can not use memory fence to synchronize.
        while (_packet_in_flight) {
            // cid here is complicated
            if (cid != INVALID_BTHREAD_ID) {
                // actually cid may be the last rpc call id.
                brpc::Join(cid);
            }
            if (_packet_in_flight) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
        }
    }

    // plz follow this order: reset() -> set_in_flight() -> send brpc batch
    void reset() {
        SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->orphan_mem_tracker());
        cntl.Reset();
        cid = cntl.call_id();
    }

    bool try_set_in_flight() {
        bool value = false;
        return _packet_in_flight.compare_exchange_strong(value, true);
    }

    void clear_in_flight() { _packet_in_flight = false; }

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
            failed_handler(_is_last_rpc);
        } else {
            success_handler(result, _is_last_rpc);
        }
        clear_in_flight();
    }

    brpc::Controller cntl;
    T result;

private:
    brpc::CallId cid;
    std::atomic<bool> _packet_in_flight {false};
    std::atomic<bool> _is_last_rpc {false};
    std::function<void(bool)> failed_handler;
    std::function<void(const T&, bool)> success_handler;
};

class IndexChannel;
class NodeChannel {
public:
    NodeChannel(OlapTableSink* parent, IndexChannel* index_channel, int64_t node_id);
    virtual ~NodeChannel() noexcept;

    // called before open, used to add tablet located in this backend
    void add_tablet(const TTabletWithPartition& tablet) { _all_tablets.emplace_back(tablet); }

    virtual Status init(RuntimeState* state);

    void add_slave_tablet_nodes(int64_t tablet_id, const std::vector<int64_t>& slave_nodes) {
        _slave_tablet_nodes[tablet_id] = slave_nodes;
    }

    // we use open/open_wait to parallel
    void open();
    virtual Status open_wait();

    Status add_row(Tuple* tuple, int64_t tablet_id);

    Status add_row(const BlockRow& block_row, int64_t tablet_id);

    virtual Status add_block(vectorized::Block* block,
                             const std::pair<std::unique_ptr<vectorized::IColumn::Selector>,
                                             std::vector<int64_t>>& payload) {
        LOG(FATAL) << "add block to NodeChannel not supported";
        return Status::OK();
    }

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    virtual void mark_close();
    Status close_wait(RuntimeState* state);

    void cancel(const std::string& cancel_msg);

    // return:
    // 0: stopped, send finished(eos request has been sent), or any internal error;
    // 1: running, haven't reach eos.
    // only allow 1 rpc in flight
    // plz make sure, this func should be called after open_wait().
    virtual int try_send_and_fetch_status(RuntimeState* state,
                                          std::unique_ptr<ThreadPoolToken>& thread_pool_token);

    void try_send_batch(RuntimeState* state);

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map,
                     int64_t* serialize_batch_ns, int64_t* mem_exceeded_block_ns,
                     int64_t* queue_push_lock_ns, int64_t* actual_consume_ns,
                     int64_t* total_add_batch_exec_time_ns, int64_t* add_batch_exec_time_ns,
                     int64_t* total_add_batch_num) {
        (*add_batch_counter_map)[_node_id] += _add_batch_counter;
        (*add_batch_counter_map)[_node_id].close_wait_time_ms = _close_time_ms;
        *serialize_batch_ns += _serialize_batch_ns;
        *mem_exceeded_block_ns += _mem_exceeded_block_ns;
        *queue_push_lock_ns += _queue_push_lock_ns;
        *actual_consume_ns += _actual_consume_ns;
        *add_batch_exec_time_ns = (_add_batch_counter.add_batch_execution_time_us * 1000);
        *total_add_batch_exec_time_ns += *add_batch_exec_time_ns;
        *total_add_batch_num += _add_batch_counter.add_batch_num;
    }

    int64_t node_id() const { return _node_id; }
    std::string host() const { return _node_info.host; }
    std::string name() const { return _name; }

    Status none_of(std::initializer_list<bool> vars);

    void clear_all_batches();

    virtual void clear_all_blocks() {}

    std::string channel_info() const {
        return fmt::format("{}, {}, node={}:{}", _name, _load_info, _node_info.host,
                           _node_info.brpc_port);
    }

    size_t get_pending_bytes() { return _pending_batches_bytes; }

protected:
    void _cancel_with_msg(const std::string& msg);

    virtual void _close_check();

protected:
    bool _is_vectorized = false;
    OlapTableSink* _parent = nullptr;
    IndexChannel* _index_channel = nullptr;
    int64_t _node_id = -1;
    std::string _load_info;
    std::string _name;

    std::shared_ptr<MemTracker> _node_channel_tracker;

    TupleDescriptor* _tuple_desc = nullptr;
    NodeInfo _node_info;

    // this should be set in init() using config
    int _rpc_timeout_ms = 60000;
    int64_t _next_packet_seq = 0;
    MonotonicStopWatch _timeout_watch;

    // the timestamp when this node channel be marked closed and finished closed
    uint64_t _close_time_ms = 0;

    // user cancel or get some errors
    std::atomic<bool> _cancelled {false};
    SpinLock _cancel_msg_lock;
    std::string _cancel_msg = "";

    // send finished means the consumer thread which send the rpc can exit
    std::atomic<bool> _send_finished {false};

    // add batches finished means the last rpc has be response, used to check whether this channel can be closed
    std::atomic<bool> _add_batches_finished {false}; // reuse for vectorized

    bool _eos_is_produced {false}; // only for restricting producer behaviors

    std::unique_ptr<RowDescriptor> _row_desc;
    int _batch_size = 0;

    // limit _pending_batches size
    std::atomic<size_t> _pending_batches_bytes {0};
    size_t _max_pending_batches_bytes {(size_t)config::nodechannel_pending_queue_max_bytes};
    std::mutex _pending_batches_lock;          // reuse for vectorized
    std::atomic<int> _pending_batches_num {0}; // reuse for vectorized

    std::shared_ptr<PBackendService_Stub> _stub = nullptr;
    RefCountClosure<PTabletWriterOpenResult>* _open_closure = nullptr;

    std::vector<TTabletWithPartition> _all_tablets;
    // map from tablet_id to node_id where slave replicas locate in
    std::unordered_map<int64_t, std::vector<int64_t>> _slave_tablet_nodes;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    AddBatchCounter _add_batch_counter;
    std::atomic<int64_t> _serialize_batch_ns {0};
    std::atomic<int64_t> _mem_exceeded_block_ns {0};
    std::atomic<int64_t> _queue_push_lock_ns {0};
    std::atomic<int64_t> _actual_consume_ns {0};

    // lock to protect _is_closed.
    // The methods in the IndexChannel are called back in the RpcClosure in the NodeChannel.
    // However, this rpc callback may occur after the whole task is finished (e.g. due to network latency),
    // and by that time the IndexChannel may have been destructured, so we should not call the
    // IndexChannel methods anymore, otherwise the BE will crash.
    // Therefore, we use the _is_closed and _closed_lock to ensure that the RPC callback
    // function will not call the IndexChannel method after the NodeChannel is closed.
    // The IndexChannel is definitely accessible until the NodeChannel is closed.
    std::mutex _closed_lock;
    bool _is_closed = false;

    RuntimeState* _state;

private:
    std::unique_ptr<RowBatch> _cur_batch;
    PTabletWriterAddBatchRequest _cur_add_batch_request;
    using AddBatchReq = std::pair<std::unique_ptr<RowBatch>, PTabletWriterAddBatchRequest>;
    std::queue<AddBatchReq> _pending_batches;
    ReusableClosure<PTabletWriterAddBatchResult>* _add_batch_closure = nullptr;
};

class IndexChannel {
public:
    IndexChannel(OlapTableSink* parent, int64_t index_id, bool is_vec)
            : _parent(parent), _index_id(index_id), _is_vectorized(is_vec) {
        _index_channel_tracker =
                std::make_unique<MemTracker>("IndexChannel:indexID=" + std::to_string(_index_id));
    }
    ~IndexChannel() = default;

    Status init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets);

    template <typename Row>
    void add_row(const Row& tuple, int64_t tablet_id);

    void for_each_node_channel(
            const std::function<void(const std::shared_ptr<NodeChannel>&)>& func) {
        for (auto& it : _node_channels) {
            func(it.second);
        }
    }

    void mark_as_failed(int64_t node_id, const std::string& host, const std::string& err,
                        int64_t tablet_id = -1);
    Status check_intolerable_failure();

    // set error tablet info in runtime state, so that it can be returned to FE.
    void set_error_tablet_in_state(RuntimeState* state);

    size_t num_node_channels() const { return _node_channels.size(); }

    size_t get_pending_bytes() const {
        size_t mem_consumption = 0;
        for (auto& kv : _node_channels) {
            mem_consumption += kv.second->get_pending_bytes();
        }
        return mem_consumption;
    }

private:
    friend class NodeChannel;
    friend class VNodeChannel;
    friend class VOlapTableSink;

    OlapTableSink* _parent;
    int64_t _index_id;
    bool _is_vectorized = false;

    // from backend channel to tablet_id
    // ATTN: must be placed before `_node_channels` and `_channels_by_tablet`.
    // Because the destruct order of objects is opposite to the creation order.
    // So NodeChannel will be destructured first.
    // And the destructor function of NodeChannel waits for all RPCs to finish.
    // This ensures that it is safe to use `_tablets_by_channel` in the callback function for the end of the RPC.
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _tablets_by_channel;
    // BeId -> channel
    std::unordered_map<int64_t, std::shared_ptr<NodeChannel>> _node_channels;
    // from tablet_id to backend channel
    std::unordered_map<int64_t, std::vector<std::shared_ptr<NodeChannel>>> _channels_by_tablet;

    // lock to protect _failed_channels and _failed_channels_msgs
    mutable SpinLock _fail_lock;
    // key is tablet_id, value is a set of failed node id
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _failed_channels;
    // key is tablet_id, value is error message
    std::unordered_map<int64_t, std::string> _failed_channels_msgs;
    Status _intolerable_failure_status = Status::OK();

    std::unique_ptr<MemTracker> _index_channel_tracker;
};

template <typename Row>
void IndexChannel::add_row(const Row& tuple, int64_t tablet_id) {
    SCOPED_CONSUME_MEM_TRACKER(_index_channel_tracker.get());
    auto it = _channels_by_tablet.find(tablet_id);
    DCHECK(it != _channels_by_tablet.end()) << "unknown tablet, tablet_id=" << tablet_id;
    for (const auto& channel : it->second) {
        // if this node channel is already failed, this add_row will be skipped
        auto st = channel->add_row(tuple, tablet_id);
        if (!st.ok()) {
            mark_as_failed(channel->node_id(), channel->host(), st.get_error_msg(), tablet_id);
            // continue add row to other node, the error will be checked for every batch outside
        }
    }
}

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
    Status _convert_batch(RuntimeState* state, RowBatch* input_batch, RowBatch* output_batch);

    // make input data valid for OLAP table
    // return number of invalid/filtered rows.
    // invalid row number is set in Bitmap
    // set stop_processing is we want to stop the whole process now.
    Status _validate_data(RuntimeState* state, RowBatch* batch, Bitmap* filter_bitmap,
                          int* filtered_rows, bool* stop_processing);
    bool _validate_cell(const TypeDescriptor& type, const std::string& col_name, void* slot,
                        size_t slot_index, fmt::memory_buffer& error_msg, RowBatch* batch);

    // the consumer func of sending pending batches in every NodeChannel.
    // use polling & NodeChannel::try_send_and_fetch_status() to achieve nonblocking sending.
    // only focus on pending batches and channel status, the internal errors of NodeChannels will be handled by the producer
    void _send_batch_process(RuntimeState* state);

protected:
    friend class NodeChannel;
    friend class VNodeChannel;
    friend class IndexChannel;

    bool _is_vectorized = false;

    std::shared_ptr<MemTracker> _mem_tracker;

    ObjectPool* _pool;
    const RowDescriptor& _input_row_desc;

    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    int _num_replicas = -1;
    int _tuple_desc_id = -1;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    RowDescriptor* _output_row_desc = nullptr;

    bool _need_validate_data = false;

    // number of senders used to insert into OlapTable, if we only support single node insert,
    // all data from select should collectted and then send to OlapTable.
    // To support multiple senders, we maintain a channel for each sender.
    int _sender_id = -1;
    int _num_senders = -1;
    bool _is_high_priority = false;

    // TODO(zc): think about cache this data
    std::shared_ptr<OlapTableSchemaParam> _schema;
    OlapTableLocationParam* _location = nullptr;
    bool _write_single_replica = false;
    OlapTableLocationParam* _slave_location = nullptr;
    DorisNodesInfo* _nodes_info = nullptr;

    RuntimeProfile* _profile = nullptr;

    std::set<int64_t> _partition_ids;
    // only used for partition with random distribution
    std::map<int64_t, int64_t> _partition_to_tablet_map;

    Bitmap _filter_bitmap;

    // index_channel
    std::vector<std::shared_ptr<IndexChannel>> _channels;

    CountDownLatch _stop_background_threads_latch;
    scoped_refptr<Thread> _sender_thread;
    std::unique_ptr<ThreadPoolToken> _send_batch_thread_pool_token;

    std::vector<DecimalV2Value> _max_decimalv2_val;
    std::vector<DecimalV2Value> _min_decimalv2_val;

    // Stats for this
    int64_t _convert_batch_ns = 0;
    int64_t _validate_data_ns = 0;
    int64_t _send_data_ns = 0;
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

    int32_t _send_batch_parallelism = 1;
    // Save the status of close() method
    Status _close_status;

    // User can change this config at runtime, avoid it being modified during query or loading process.
    bool _transfer_large_data_by_brpc = false;

    // FIND_TABLET_EVERY_ROW is used for both hash and random distribution info, which indicates that we
    // should compute tablet index for every row
    // FIND_TABLET_EVERY_BATCH is only used for random distribution info, which indicates that we should
    // compute tablet index for every row batch
    // FIND_TABLET_EVERY_SINK is only used for random distribution info, which indicates that we should
    // only compute tablet index in the corresponding partition once for the whole time in olap table sink
    enum FindTabletMode { FIND_TABLET_EVERY_ROW, FIND_TABLET_EVERY_BATCH, FIND_TABLET_EVERY_SINK };
    FindTabletMode findTabletMode = FindTabletMode::FIND_TABLET_EVERY_ROW;

private:
    OlapTablePartitionParam* _partition = nullptr;
    std::vector<ExprContext*> _output_expr_ctxs;
    std::unique_ptr<RowBatch> _output_batch;
};

} // namespace stream_load
} // namespace doris
