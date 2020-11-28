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

#ifndef DORIS_BE_RUNTIME_DATA_STREAM_SENDER_H
#define DORIS_BE_RUNTIME_DATA_STREAM_SENDER_H

#include <string>
#include <vector>

#include "common/global_types.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/data_sink.h"
#include "gen_cpp/data.pb.h" // for PRowBatch
#include "util/runtime_profile.h"

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
    template <class T>
    Status serialize_batch(RowBatch* src, T* dest, int num_receivers = 1);

    // Return total number of bytes sent in TRowBatch.data. If batches are
    // broadcast to multiple receivers, they are counted once per receiver.
    int64_t get_num_data_bytes_sent() const;

    virtual RuntimeProfile* profile() { return _profile; }

private:
    class Channel;

    Status compute_range_part_code(RuntimeState* state, TupleRow* row, size_t* hash_value,
                                   bool* ignore);

    int binary_find_partition(const PartRangeKey& key) const;

    Status find_partition(RuntimeState* state, TupleRow* row, PartitionInfo** info, bool* ignore);

    Status process_distribute(RuntimeState* state, TupleRow* row, const PartitionInfo* part,
                              size_t* hash_val);

    // Sender instance id, unique within a fragment.
    int _sender_id;

    RuntimeState* _state;
    ObjectPool* _pool;
    const RowDescriptor& _row_desc;

    int _current_channel_idx; // index of current channel to send to if _random == true

    // If true, this sender has been closed. Not valid to call Send() anymore.
    bool _closed;

    TPartitionType::type _part_type;
    bool _ignore_not_found;

    // serialized batches for broadcasting; we need two so we can write
    // one while the other one is still being sent
    PRowBatch _pb_batch1;
    PRowBatch _pb_batch2;
    PRowBatch* _current_pb_batch = nullptr;

    std::vector<ExprContext*> _partition_expr_ctxs; // compute per-row partition values

    std::vector<Channel*> _channels;
    std::vector<std::shared_ptr<Channel>> _channel_shared_ptrs;

    // map from range value to partition_id
    // sorted in ascending orderi by range for binary search
    std::vector<PartitionInfo*> _partition_infos;

    RuntimeProfile* _profile; // Allocated from _pool
    RuntimeProfile::Counter* _serialize_batch_timer;
    RuntimeProfile::Counter* _bytes_sent_counter;
    RuntimeProfile::Counter* _uncompressed_bytes_counter;
    RuntimeProfile::Counter* _ignore_rows;

    std::shared_ptr<MemTracker> _mem_tracker;

    // Throughput per total time spent in sender
    RuntimeProfile::Counter* _overall_throughput;

    // Identifier of the destination plan node.
    PlanNodeId _dest_node_id;
};

} // namespace doris

#endif
