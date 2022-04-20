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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/runtime/data-stream-sender.h
// and modified by Doris

#ifndef DORIS_BE_RUNTIME_DATA_STREAM_SENDER_H
#define DORIS_BE_RUNTIME_DATA_STREAM_SENDER_H

#include <string>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "gen_cpp/BackendService.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/data.pb.h" // for PRowBatch
#include "gen_cpp/internal_service.pb.h"
#include "service/backend_options.h"
#include "service/brpc.h"
#include "util/ref_count_closure.h"
#include "util/runtime_profile.h"
#include "util/uid_util.h"

namespace doris {

class ExprContext;
class RowBatch;
class RowDescriptor;
class TDataStreamSink;
class TNetworkAddress;
class TPlanFragmentDestination;
class PartitionInfo;
class TupleRow;
class PartRangeKey;
class MemTracker;

// Single sender of an m:n data stream.
// Row batch data is routed to destinations based on the provided
// partitioning specification.
// *Not* thread-safe.
//
// TODO: capture stats that describe distribution of rows/data volume
// across channels.
class DataStreamSender : public DataSink {
public:
    DataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc);
    // Construct a sender according to the output specification (sink),
    // sending to the given destinations.
    // Per_channel_buffer_size is the buffer size allocated to each channel
    // and is specified in bytes.
    // The RowDescriptor must live until close() is called.
    // NOTE: supported partition types are UNPARTITIONED (broadcast) and HASH_PARTITIONED
    DataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                     const TDataStreamSink& sink,
                     const std::vector<TPlanFragmentDestination>& destinations,
                     int per_channel_buffer_size, bool send_query_statistics_with_every_batch);
    virtual ~DataStreamSender();

    virtual Status init(const TDataSink& thrift_sink);

    // Must be called before other API calls, and before the codegen'd IR module is
    // compiled (i.e. in an ExecNode's Prepare() function).
    virtual Status prepare(RuntimeState* state);

    // Must be called before Send() or Close(), and after the codegen'd IR module is
    // compiled (i.e. in an ExecNode's Open() function).
    virtual Status open(RuntimeState* state);

    // send data in 'batch' to destination nodes according to partitioning
    // specification provided in c'tor.
    // Blocks until all rows in batch are placed in their appropriate outgoing
    // buffers (ie, blocks if there are still in-flight rpcs from the last
    // send() call).
    virtual Status send(RuntimeState* state, RowBatch* batch);

    // Flush all buffered data and close all existing channels to destination
    // hosts. Further send() calls are illegal after calling close().
    virtual Status close(RuntimeState* state, Status exec_status);

    /// Serializes the src batch into the dest thrift batch. Maintains metrics.
    /// num_receivers is the number of receivers this batch will be sent to. Only
    /// used to maintain metrics.
    Status serialize_batch(RowBatch* src, PRowBatch* dest, int num_receivers = 1);

    // Return total number of bytes sent in RowBatch.data. If batches are
    // broadcast to multiple receivers, they are counted once per receiver.
    virtual RuntimeProfile* profile() { return _profile; }

    RuntimeState* state() { return _state; }

protected:
    const RowDescriptor& _row_desc;
    // A channel sends data asynchronously via calls to transmit_data
    // to a single destination ipaddress/node.
    // It has a fixed-capacity buffer and allows the caller either to add rows to
    // that buffer individually (AddRow()), or circumvent the buffer altogether and send
    // PRowBatches directly (SendBatch()). Either way, there can only be one in-flight RPC
    // at any one time (ie, sending will block if the most recent rpc hasn't finished,
    // which allows the receiver node to throttle the sender by withholding acks).
    // *Not* thread-safe.
    class Channel {
    public:
        Channel(DataStreamSender* parent, const RowDescriptor& row_desc,
                const TNetworkAddress& brpc_dest, const TUniqueId& fragment_instance_id,
                PlanNodeId dest_node_id, int buffer_size, bool is_transfer_chain,
                bool send_query_statistics_with_every_batch);
        ~Channel();
        // Initialize channel.
        // Returns OK if successful, error indication otherwise.
        Status init(RuntimeState* state);

        // Copies a single row into this channel's output buffer and flushes buffer
        // if it reaches capacity.
        // Returns error status if any of the preceding rpcs failed, OK otherwise.
        Status add_row(TupleRow* row);

        // Asynchronously sends a row batch.
        // Returns the status of the most recently finished transmit_data
        // rpc (or OK if there wasn't one that hasn't been reported yet).
        // if batch is nullptr, send the eof packet
        Status send_batch(PRowBatch* batch, bool eos = false);

        Status send_local_batch(bool eos);

        Status send_local_batch(RowBatch* batch, bool use_move);

        // Flush buffered rows and close channel. This function don't wait the response
        // of close operation, client should call close_wait() to finish channel's close.
        // We split one close operation into two phases in order to make multiple channels
        // can run parallel.
        Status close(RuntimeState* state);

        // Get close wait's response, to finish channel close operation.
        Status close_wait(RuntimeState* state);

        PRowBatch* ch_cur_pb_batch() { return _ch_cur_pb_batch; }

        std::string get_fragment_instance_id_str() {
            UniqueId uid(_fragment_instance_id);
            return uid.to_string();
        }

        TUniqueId get_fragment_instance_id() { return _fragment_instance_id; }

        bool is_local() { return _is_local; }

        Status _wait_last_brpc() {
            if (_closure == nullptr) return Status::OK();
            auto cntl = &_closure->cntl;
            brpc::Join(cntl->call_id());
            if (cntl->Failed()) {
                std::stringstream ss;
                ss << "failed to send brpc batch, error=" << berror(cntl->ErrorCode())
                   << ", error_text=" << cntl->ErrorText()
                   << ", client: " << BackendOptions::get_localhost();
                LOG(WARNING) << ss.str();
                return Status::ThriftRpcError(ss.str());
            }
            return Status::OK();
        }
        // Serialize _batch into _thrift_batch and send via send_batch().
        // Returns send_batch() status.
        Status send_current_batch(bool eos = false);
        Status close_internal();
        // this must be called after calling `send_batch()`
        void ch_roll_pb_batch();

        DataStreamSender* _parent;
        int _buffer_size;

        const RowDescriptor& _row_desc;
        TUniqueId _fragment_instance_id;
        PlanNodeId _dest_node_id;

        int64_t _packet_seq;

        // we're accumulating rows into this batch
        std::unique_ptr<RowBatch> _batch;

        bool _need_close;
        int _be_number;

        TNetworkAddress _brpc_dest_addr;

        // TODO(zc): initused for brpc
        PUniqueId _finst_id;

        // serialized batches for broadcasting; we need two so we can write
        // one while the other one is still being sent.
        // Which is for same reason as `_cur_pb_batch`, `_pb_batch1` and `_pb_batch2`
        // in DataStreamSender.
        PRowBatch* _ch_cur_pb_batch;
        PRowBatch _ch_pb_batch1;
        PRowBatch _ch_pb_batch2;

        PTransmitDataParams _brpc_request;
        std::shared_ptr<PBackendService_Stub> _brpc_stub = nullptr;
        RefCountClosure<PTransmitDataResult>* _closure = nullptr;
        int32_t _brpc_timeout_ms = 500;
        // whether the dest can be treated as query statistics transfer chain.
        bool _is_transfer_chain;
        bool _send_query_statistics_with_every_batch;
        bool _is_local;
    };

    RuntimeProfile* _profile; // Allocated from _pool
    PRowBatch* _cur_pb_batch;
    std::shared_ptr<MemTracker> _mem_tracker;
    ObjectPool* _pool;
    // Sender instance id, unique within a fragment.
    int _sender_id;
    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _bytes_sent_counter;
    // Used to counter send bytes under local data exchange
    RuntimeProfile::Counter* _local_bytes_send_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter;
    RuntimeState* _state;

    std::vector<Channel*> _channels;
    std::vector<std::shared_ptr<Channel>> _channel_shared_ptrs;

private:
    Status compute_range_part_code(RuntimeState* state, TupleRow* row, size_t* hash_value,
                                   bool* ignore);

    int binary_find_partition(const PartRangeKey& key) const;

    Status find_partition(RuntimeState* state, TupleRow* row, PartitionInfo** info, bool* ignore);

    Status process_distribute(RuntimeState* state, TupleRow* row, const PartitionInfo* part,
                              size_t* hash_val);

    void _roll_pb_batch();

    int _current_channel_idx; // index of current channel to send to if _random == true

    TPartitionType::type _part_type;
    bool _ignore_not_found;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PRowBatch _pb_batch1;
    PRowBatch _pb_batch2;

    // This buffer is used to store the serialized rowbatch data.
    // Only works when `config::transfer_data_by_brpc_attachment` is true.
    // The data in the buffer is copied to the attachment of the brpc when it is sent,
    // to avoid an extra pb serialization in the brpc.
    // _tuple_data_buffer_ptr will point to _tuple_data_buffer if `config::transfer_data_by_brpc_attachment` is true.
    std::string _tuple_data_buffer;
    std::string* _tuple_data_buffer_ptr = nullptr;

    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values

    // map from range value to partition_id
    // sorted in ascending orderi by range for binary search
    std::vector<PartitionInfo*> _partition_infos;

    RuntimeProfile::Counter* _ignore_rows;

    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput;

    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;

    bool _transfer_data_by_brpc_attachment = false;
};

} // namespace doris

#endif
