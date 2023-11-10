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
#include <brpc/controller.h>
#include <bthread/types.h>
#include <butil/errno.h>
#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <glog/logging.h>
#include <google/protobuf/stubs/callback.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>

#include "olap/wal_writer.h"
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <map>
#include <memory>
#include <mutex>
#include <ostream>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "exec/tablet_info.h"
#include "gutil/ref_counted.h"
#include "runtime/decimalv2_value.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/thread_context.h"
#include "runtime/types.h"
#include "util/countdown_latch.h"
#include "util/runtime_profile.h"
#include "util/spinlock.h"
#include "util/stopwatch.hpp"
#include "vec/columns/column.h"
#include "vec/common/allocator.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/runtime/vfile_format_transformer.h"
#include "vec/sink/vrow_distribution.h"
#include "vec/sink/vtablet_block_convertor.h"
#include "vec/sink/vtablet_finder.h"
#include "vec/sink/writer/async_result_writer.h"

namespace doris {
class ObjectPool;
class RowDescriptor;
class RuntimeState;
class TDataSink;
class TExpr;
class Thread;
class ThreadPoolToken;
class TupleDescriptor;
template <typename T>
class RefCountClosure;

namespace vectorized {

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
// "Ping-Pong" between sender and receiver, `try_set_in_flight` when send, `clear_in_flight` after rpc failure or callback,
// then next send will start, and it will wait for the rpc callback to complete when it is destroyed.
template <typename T>
class ReusableClosure final : public google::protobuf::Closure {
public:
    ReusableClosure() : cid(INVALID_BTHREAD_ID) {}
    ~ReusableClosure() override {
        // shouldn't delete when Run() is calling or going to be called, wait for current Run() done.
        join();
        SCOPED_TRACK_MEMORY_TO_UNKNOWN();
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
        SCOPED_TRACK_MEMORY_TO_UNKNOWN();
        cntl.Reset();
        cid = cntl.call_id();
    }

    // if _packet_in_flight == false, set it to true. Return true.
    // if _packet_in_flight == true, Return false.
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
class VTabletWriter;

class VNodeChannelStat {
public:
    VNodeChannelStat& operator+=(const VNodeChannelStat& stat) {
        mem_exceeded_block_ns += stat.mem_exceeded_block_ns;
        where_clause_ns += stat.where_clause_ns;
        append_node_channel_ns += stat.append_node_channel_ns;
        return *this;
    };

    int64_t mem_exceeded_block_ns = 0;
    int64_t where_clause_ns = 0;
    int64_t append_node_channel_ns = 0;
};

// pair<row_id,tablet_id>
using Payload = std::pair<std::unique_ptr<vectorized::IColumn::Selector>, std::vector<int64_t>>;

// every NodeChannel keeps a data transmission channel with one BE. for multiple times open, it has a dozen of requests and corresponding closures.
class VNodeChannel {
public:
    VNodeChannel(VTabletWriter* parent, IndexChannel* index_channel, int64_t node_id,
                 bool is_incremental = false);

    ~VNodeChannel();

    // called before open, used to add tablet located in this backend. called by IndexChannel::init
    void add_tablet(const TTabletWithPartition& tablet) { _all_tablets.emplace_back(tablet); }
    std::string debug_tablets() const {
        std::stringstream ss;
        for (auto& tab : _all_tablets) {
            tab.printTo(ss);
            ss << '\n';
        }
        return ss.str();
    }

    void add_slave_tablet_nodes(int64_t tablet_id, const std::vector<int64_t>& slave_nodes) {
        _slave_tablet_nodes[tablet_id] = slave_nodes;
    }

    // build a request and build corresponding connect to BE.
    void open();
    // for auto partition, we use this to open more tablet.
    void incremental_open();

    Status init(RuntimeState* state);

    // this will block until all request transmission which were opened or incremental opened finished.
    Status open_wait();

    Status add_block(vectorized::Block* block, const Payload* payload, bool is_append = false);

    // @return: 1 if running, 0 if finished.
    // @caller: VOlapTabletSink::_send_batch_process. it's a continual asynchronous process.
    int try_send_and_fetch_status(RuntimeState* state,
                                  std::unique_ptr<ThreadPoolToken>& thread_pool_token);
    // when there's pending block found by try_send_and_fetch_status(), we will awake a thread to send it.
    void try_send_pending_block(RuntimeState* state);

    void clear_all_blocks();

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    void mark_close();

    bool is_send_data_rpc_done() const;

    bool is_closed() const { return _is_closed; }
    bool is_cancelled() const { return _cancelled; }
    std::string get_cancel_msg() {
        std::stringstream ss;
        ss << "close wait failed coz rpc error";
        {
            std::lock_guard<doris::SpinLock> l(_cancel_msg_lock);
            if (_cancel_msg != "") {
                ss << ". " << _cancel_msg;
            }
        }
        return ss.str();
    }

    // two ways to stop channel:
    // 1. mark_close()->close_wait() PS. close_wait() will block waiting for the last AddBatch rpc response.
    // 2. just cancel()
    Status close_wait(RuntimeState* state);

    void cancel(const std::string& cancel_msg);

    void time_report(std::unordered_map<int64_t, AddBatchCounter>* add_batch_counter_map,
                     int64_t* serialize_batch_ns, VNodeChannelStat* stat,
                     int64_t* queue_push_lock_ns, int64_t* actual_consume_ns,
                     int64_t* total_add_batch_exec_time_ns, int64_t* add_batch_exec_time_ns,
                     int64_t* total_wait_exec_time_ns, int64_t* wait_exec_time_ns,
                     int64_t* total_add_batch_num) const {
        (*add_batch_counter_map)[_node_id] += _add_batch_counter;
        (*add_batch_counter_map)[_node_id].close_wait_time_ms = _close_time_ms;
        *serialize_batch_ns += _serialize_batch_ns;
        *stat += _stat;
        *queue_push_lock_ns += _queue_push_lock_ns;
        *actual_consume_ns += _actual_consume_ns;
        *add_batch_exec_time_ns = (_add_batch_counter.add_batch_execution_time_us * 1000);
        *total_add_batch_exec_time_ns += *add_batch_exec_time_ns;
        *wait_exec_time_ns = (_add_batch_counter.add_batch_wait_execution_time_us * 1000);
        *total_wait_exec_time_ns += *wait_exec_time_ns;
        *total_add_batch_num += _add_batch_counter.add_batch_num;
    }

    int64_t node_id() const { return _node_id; }
    std::string host() const { return _node_info.host; }
    std::string name() const { return _name; }

    Status none_of(std::initializer_list<bool> vars);

    std::string channel_info() const {
        return fmt::format("{}, {}, node={}:{}", _name, _load_info, _node_info.host,
                           _node_info.brpc_port);
    }

    size_t get_pending_bytes() { return _pending_batches_bytes; }

    bool is_incremental() const { return _is_incremental; }

protected:
    // make a real open request for relative BE's load channel.
    void _open_internal(bool is_incremental);

    void _close_check();
    void _cancel_with_msg(const std::string& msg);

    void _add_block_success_callback(const PTabletWriterAddBlockResult& result, bool is_last_rpc);
    void _add_block_failed_callback(bool is_last_rpc);

    VTabletWriter* _parent = nullptr;
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
    doris::SpinLock _cancel_msg_lock;
    std::string _cancel_msg;

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
    // because we have incremantal open, we should keep one relative closure for one request. it's similarly for adding block.
    std::vector<RefCountClosure<PTabletWriterOpenResult>*> _open_closures;

    std::vector<TTabletWithPartition> _all_tablets;
    // map from tablet_id to node_id where slave replicas locate in
    std::unordered_map<int64_t, std::vector<int64_t>> _slave_tablet_nodes;
    std::vector<TTabletCommitInfo> _tablet_commit_infos;

    AddBatchCounter _add_batch_counter;
    std::atomic<int64_t> _serialize_batch_ns {0};
    std::atomic<int64_t> _queue_push_lock_ns {0};
    std::atomic<int64_t> _actual_consume_ns {0};

    VNodeChannelStat _stat;
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
    // rows number received per tablet, tablet_id -> rows_num
    std::vector<std::pair<int64_t, int64_t>> _tablets_received_rows;
    // rows number filtered per tablet, tablet_id -> filtered_rows_num
    std::vector<std::pair<int64_t, int64_t>> _tablets_filtered_rows;

    // build a _cur_mutable_block and push into _pending_blocks. when not building, this block is empty.
    std::unique_ptr<vectorized::MutableBlock> _cur_mutable_block;
    PTabletWriterAddBlockRequest _cur_add_block_request;

    using AddBlockReq =
            std::pair<std::unique_ptr<vectorized::MutableBlock>, PTabletWriterAddBlockRequest>;
    std::queue<AddBlockReq> _pending_blocks;
    ReusableClosure<PTabletWriterAddBlockResult>* _add_block_closure = nullptr;

    bool _is_incremental;
};

// an IndexChannel is related to specific table and its rollup and mv
class IndexChannel {
public:
    IndexChannel(VTabletWriter* parent, int64_t index_id,
                 const vectorized::VExprContextSPtr& where_clause)
            : _parent(parent), _index_id(index_id), _where_clause(where_clause) {
        _index_channel_tracker =
                std::make_unique<MemTracker>("IndexChannel:indexID=" + std::to_string(_index_id));
    }
    ~IndexChannel() = default;

    // allow to init multi times, for incremental open more tablets for one index(table)
    Status init(RuntimeState* state, const std::vector<TTabletWithPartition>& tablets);

    void for_each_node_channel(
            const std::function<void(const std::shared_ptr<VNodeChannel>&)>& func) {
        for (auto& it : _node_channels) {
            func(it.second);
        }
    }

    void mark_as_failed(const VNodeChannel* node_channel, const std::string& err,
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

    void set_tablets_received_rows(
            const std::vector<std::pair<int64_t, int64_t>>& tablets_received_rows, int64_t node_id);

    void set_tablets_filtered_rows(
            const std::vector<std::pair<int64_t, int64_t>>& tablets_filtered_rows, int64_t node_id);

    int64_t num_rows_filtered() {
        DCHECK(!_tablets_filtered_rows.empty());
        // the Unique table has no roll up or materilized view
        // we just add up filtered rows from all partitions
        return std::accumulate(_tablets_filtered_rows.cbegin(), _tablets_filtered_rows.cend(), 0,
                               [](int64_t sum, const auto& a) { return sum + a.second[0].second; });
    }

    // check whether the rows num written by different replicas is consistent
    Status check_tablet_received_rows_consistency();

    // check whether the rows num filtered by different replicas is consistent
    Status check_tablet_filtered_rows_consistency();

    vectorized::VExprContextSPtr get_where_clause() { return _where_clause; }

private:
    friend class VNodeChannel;
    friend class VTabletWriter;
    friend class VRowDistribution;

    VTabletWriter* _parent;
    int64_t _index_id;
    vectorized::VExprContextSPtr _where_clause;

    // from backend channel to tablet_id
    // ATTN: must be placed before `_node_channels` and `_channels_by_tablet`.
    // Because the destruct order of objects is opposite to the creation order.
    // So NodeChannel will be destructured first.
    // And the destructor function of NodeChannel waits for all RPCs to finish.
    // This ensures that it is safe to use `_tablets_by_channel` in the callback function for the end of the RPC.
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _tablets_by_channel;
    // BeId -> channel
    std::unordered_map<int64_t, std::shared_ptr<VNodeChannel>> _node_channels;
    // from tablet_id to backend channel
    std::unordered_map<int64_t, std::vector<std::shared_ptr<VNodeChannel>>> _channels_by_tablet;

    // lock to protect _failed_channels and _failed_channels_msgs
    mutable doris::SpinLock _fail_lock;
    // key is tablet_id, value is a set of failed node id
    std::unordered_map<int64_t, std::unordered_set<int64_t>> _failed_channels;
    // key is tablet_id, value is error message
    std::unordered_map<int64_t, std::string> _failed_channels_msgs;
    Status _intolerable_failure_status = Status::OK();

    std::unique_ptr<MemTracker> _index_channel_tracker;
    // rows num received by DeltaWriter per tablet, tablet_id -> <node_Id, rows_num>
    // used to verify whether the rows num received by different replicas is consistent
    std::map<int64_t, std::vector<std::pair<int64_t, int64_t>>> _tablets_received_rows;

    // rows num filtered by DeltaWriter per tablet, tablet_id -> <node_Id, filtered_rows_num>
    // used to verify whether the rows num filtered by different replicas is consistent
    std::map<int64_t, std::vector<std::pair<int64_t, int64_t>>> _tablets_filtered_rows;
};
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
//
// write result to file
class VTabletWriter final : public AsyncResultWriter {
public:
    VTabletWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);

    Status init_properties(ObjectPool* pool, bool group_commit);

    Status append_block(Block& block) override;

    Status close(Status) override;

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status try_close(RuntimeState* state, Status exec_status);

    // the consumer func of sending pending batches in every NodeChannel.
    // use polling & NodeChannel::try_send_and_fetch_status() to achieve nonblocking sending.
    // only focus on pending batches and channel status, the internal errors of NodeChannels will be handled by the producer
    void _send_batch_process();

    bool is_close_done();

    Status on_partitions_created(TCreatePartitionResult* result);

private:
    friend class VNodeChannel;
    friend class IndexChannel;

    using ChannelDistributionPayload = std::unordered_map<VNodeChannel*, Payload>;
    using ChannelDistributionPayloadVec = std::vector<std::unordered_map<VNodeChannel*, Payload>>;

    void _init_row_distribution();

    Status _init(RuntimeState* state, RuntimeProfile* profile);

    void _generate_one_index_channel_payload(RowPartTabletIds& row_part_tablet_tuple,
                                             int32_t index_idx,
                                             ChannelDistributionPayload& channel_payload);

    void _generate_index_channels_payloads(std::vector<RowPartTabletIds>& row_part_tablet_ids,
                                           ChannelDistributionPayloadVec& payload);

    Status _cancel_channel_and_check_intolerable_failure(Status status, const std::string& err_msg,
                                                         const std::shared_ptr<IndexChannel> ich,
                                                         const std::shared_ptr<VNodeChannel> nch);

    std::pair<vectorized::VExprContextSPtr, vectorized::VExprSPtr> _get_partition_function();

    void _cancel_all_channel(Status status);

    void _save_missing_values(vectorized::ColumnPtr col, vectorized::DataTypePtr value_type,
                              std::vector<int64_t> filter);

    // create partitions when need for auto-partition table using #_partitions_need_create.
    Status _automatic_create_partition();

    Status _incremental_open_node_channel(const std::vector<TOlapTablePartition>& partitions);

    Status write_wal(OlapTableBlockConvertor* block_convertor, OlapTabletFinder* tablet_finder,
                     vectorized::Block* block, RuntimeState* state, int64_t num_rows,
                     int64_t filtered_rows);

    void _group_commit_block(vectorized::Block* input_block, int64_t num_rows, int64_t filter_rows,
                             RuntimeState* state, vectorized::Block* block,
                             OlapTableBlockConvertor* block_convertor,
                             OlapTabletFinder* tablet_finder);

    TDataSink _t_sink;

    std::shared_ptr<MemTracker> _mem_tracker;

    ObjectPool* _pool;

    bthread_t _sender_thread = 0;

    // unique load id
    PUniqueId _load_id;
    int64_t _txn_id = -1;
    int _num_replicas = -1;
    int _tuple_desc_id = -1;

    // this is tuple descriptor of destination OLAP table
    TupleDescriptor* _output_tuple_desc = nullptr;
    RowDescriptor* _output_row_desc = nullptr;

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

    std::unique_ptr<OlapTabletFinder> _tablet_finder;

    // index_channel
    std::mutex _stop_check_channel;
    std::vector<std::shared_ptr<IndexChannel>> _channels;
    std::unordered_map<int64_t, std::shared_ptr<IndexChannel>> _index_id_to_channel;

    std::unique_ptr<ThreadPoolToken> _send_batch_thread_pool_token;

    // support only one partition column now
    std::vector<std::vector<TStringLiteral>> _partitions_need_create;

    std::unique_ptr<OlapTableBlockConvertor> _block_convertor;
    // Stats for this
    int64_t _send_data_ns = 0;
    int64_t _number_input_rows = 0;
    int64_t _number_output_rows = 0;
    int64_t _filter_ns = 0;

    MonotonicStopWatch _row_distribution_watch;

    RuntimeProfile::Counter* _input_rows_counter = nullptr;
    RuntimeProfile::Counter* _output_rows_counter = nullptr;
    RuntimeProfile::Counter* _filtered_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _row_distribution_timer = nullptr;
    RuntimeProfile::Counter* _append_node_channel_timer = nullptr;
    RuntimeProfile::Counter* _filter_timer = nullptr;
    RuntimeProfile::Counter* _where_clause_timer = nullptr;
    RuntimeProfile::Counter* _add_partition_request_timer = nullptr;
    RuntimeProfile::Counter* _wait_mem_limit_timer = nullptr;
    RuntimeProfile::Counter* _validate_data_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_timer = nullptr;
    RuntimeProfile::Counter* _non_blocking_send_work_timer = nullptr;
    RuntimeProfile::Counter* _serialize_batch_timer = nullptr;
    RuntimeProfile::Counter* _total_add_batch_exec_timer = nullptr;
    RuntimeProfile::Counter* _max_add_batch_exec_timer = nullptr;
    RuntimeProfile::Counter* _total_wait_exec_timer = nullptr;
    RuntimeProfile::Counter* _max_wait_exec_timer = nullptr;
    RuntimeProfile::Counter* _add_batch_number = nullptr;
    RuntimeProfile::Counter* _num_node_channels = nullptr;

    // load mem limit is for remote load channel
    int64_t _load_mem_limit = -1;

    // the timeout of load channels opened by this tablet sink. in second
    int64_t _load_channel_timeout_s = 0;

    int32_t _send_batch_parallelism = 1;
    // Save the status of try_close() and close() method
    Status _close_status;
    // if we called try_close(), for auto partition the periodic send thread should stop if it's still waiting for node channels first-time open.
    bool _try_close = false;
    // for non-pipeline, if close() did something, close_wait() should wait it.
    bool _close_wait = false;
    bool _inited = false;

    // User can change this config at runtime, avoid it being modified during query or loading process.
    bool _transfer_large_data_by_brpc = false;

    VOlapTablePartitionParam* _vpartition = nullptr;

    RuntimeState* _state = nullptr;     // not owned, set when open
    RuntimeProfile* _profile = nullptr; // not owned, set when open
    bool _group_commit = false;
    std::shared_ptr<WalWriter> _wal_writer = nullptr;

    VRowDistribution _row_distribution;
    // reuse to avoid frequent memory allocation and release.
    std::vector<RowPartTabletIds> _row_part_tablet_ids;

    int64_t _tb_id;
    int64_t _db_id;
    int64_t _wal_id;
};
} // namespace doris::vectorized
