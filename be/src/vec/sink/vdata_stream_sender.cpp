#include "vec/sink/vdata_stream_sender.h"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include "runtime/client_cache.h"
#include "runtime/dpp_sink_internal.h"
#include "runtime/exec_env.h"
#include "runtime/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "vec/common/sip_hash.h"
#include "vec/runtime/vpartition_info.h"

namespace doris::vectorized {

Status VDataStreamSender::Channel::init(RuntimeState* state) {
    _be_number = state->be_number();

    _capacity = std::max(1, _buffer_size / std::max(_row_desc.get_row_size(), 1));
    //_batch.reset(new RowBatch(_row_desc, capacity, _parent->_mem_tracker.get()));
    _mutable_block.reset(new MutableBlock());

    if (_brpc_dest_addr.hostname.empty()) {
        LOG(WARNING) << "there is no brpc destination address's hostname"
                        ", maybe version is not compatible.";
        return Status::InternalError("no brpc destination");
    }

    // initialize brpc request
    _finst_id.set_hi(_fragment_instance_id.hi);
    _finst_id.set_lo(_fragment_instance_id.lo);
    _brpc_request.set_allocated_finst_id(&_finst_id);
    _brpc_request.set_node_id(_dest_node_id);
    _brpc_request.set_sender_id(_parent->_sender_id);
    _brpc_request.set_be_number(_be_number);

    _brpc_timeout_ms = std::min(3600, state->query_options().query_timeout) * 1000;
    _brpc_stub = state->exec_env()->brpc_stub_cache()->get_stub(_brpc_dest_addr);

    // In bucket shuffle join will set fragment_instance_id (-1, -1)
    // to build a camouflaged empty channel. the ip and port is '0.0.0.0:0"
    // so the empty channel not need call function close_internal()
    _need_close = (_fragment_instance_id.hi != -1 && _fragment_instance_id.lo != -1);
    return Status::OK();
}

Status VDataStreamSender::Channel::send_current_block(bool eos) {
    {
        SCOPED_TIMER(_parent->_serialize_batch_timer);
        _pb_block.Clear();
        auto uncompressed_bytes = _mutable_block->to_block().serialize(&_pb_block);
        _mutable_block->clear();

        auto bytes = _pb_block.ByteSize();
        COUNTER_UPDATE(_parent->_bytes_sent_counter, bytes);
        COUNTER_UPDATE(_parent->_uncompressed_bytes_counter, uncompressed_bytes);
    }
    RETURN_IF_ERROR(send_block(&_pb_block, eos));
    return Status::OK();
}

Status VDataStreamSender::Channel::send_block(PBlock* block, bool eos) {
    if (_closure == nullptr) {
        _closure = new RefCountClosure<PTransmitDataResult>();
        _closure->ref();
    } else {
        RETURN_IF_ERROR(_wait_last_brpc());
        _closure->cntl.Reset();
    }
    VLOG_ROW << "Channel::send_batch() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id;
    if (_is_transfer_chain && (_send_query_statistics_with_every_batch || eos)) {
        auto statistic = _brpc_request.mutable_query_statistics();
        _parent->_query_statistics->to_pb(statistic);
    }

    _brpc_request.set_eos(eos);
    if (block != nullptr) {
        _brpc_request.set_allocated_block(block);
    }
    _brpc_request.set_packet_seq(_packet_seq++);

    _closure->ref();
    _closure->cntl.set_timeout_ms(_brpc_timeout_ms);
    _brpc_stub->transmit_block(&_closure->cntl, &_brpc_request, &_closure->result, _closure);
    if (block != nullptr) {
        _brpc_request.release_block();
    }
    return Status::OK();
}

Status VDataStreamSender::Channel::add_row(Block* block, int row) {
    if (_fragment_instance_id.lo == -1) {
        return Status::OK();
    }
    int batch_size = _parent->state()->batch_size();
    if (block->rows() == batch_size) {
        RETURN_IF_ERROR(send_current_block());
    }
    if (_mutable_block->rows() == 0) {
        auto empty_block = block->clone_empty();
        _mutable_block.reset(
                new MutableBlock(empty_block.mutate_columns(), empty_block.get_data_types()));
    }
    _mutable_block->add_row(block, row);
    return Status::OK();
}

Status VDataStreamSender::Channel::close_wait(RuntimeState* state) {
    if (_need_close) {
        Status st = _wait_last_brpc();
        if (!st.ok()) {
            state->log_error(st.get_error_msg());
        }
        _need_close = false;
        return st;
    }
    _mutable_block.reset();
    return Status::OK();
}

Status VDataStreamSender::Channel::close_internal() {
    if (!_need_close) {
        return Status::OK();
    }
    VLOG_RPC << "Channel::close() instance_id=" << _fragment_instance_id
             << " dest_node=" << _dest_node_id
             << " #rows= " << ((_mutable_block == nullptr) ? 0 : _mutable_block->rows());
    if (_mutable_block != nullptr && _mutable_block->rows() > 0) {
        RETURN_IF_ERROR(send_current_block(true));
    } else {
        RETURN_IF_ERROR(send_block(nullptr, true));
    }
    // Don't wait for the last packet to finish, left it to close_wait.
    return Status::OK();
}

Status VDataStreamSender::Channel::close(RuntimeState* state) {
    Status st = close_internal();
    if (!st.ok()) {
        state->log_error(st.get_error_msg());
    }
    return st;
}

VDataStreamSender::VDataStreamSender(ObjectPool* pool, int sender_id, const RowDescriptor& row_desc,
                                     const TDataStreamSink& sink,
                                     const std::vector<TPlanFragmentDestination>& destinations,
                                     int per_channel_buffer_size,
                                     bool send_query_statistics_with_every_batch)
        : _sender_id(sender_id),
          _pool(pool),
          _row_desc(row_desc),
          _current_channel_idx(0),
          _part_type(sink.output_partition.type),
          _ignore_not_found(sink.__isset.ignore_not_found ? sink.ignore_not_found : true),
          _current_pb_block(&_pb_block1),
          _profile(nullptr),
          _serialize_batch_timer(nullptr),
          _bytes_sent_counter(nullptr),
          _dest_node_id(sink.dest_node_id) {
    DCHECK_GT(destinations.size(), 0);
    DCHECK(sink.output_partition.type == TPartitionType::UNPARTITIONED ||
           sink.output_partition.type == TPartitionType::HASH_PARTITIONED ||
           sink.output_partition.type == TPartitionType::RANDOM ||
           sink.output_partition.type == TPartitionType::RANGE_PARTITIONED ||
           sink.output_partition.type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED);
    //
    std::map<int64_t, int64_t> fragment_id_to_channel_index;

    for (int i = 0; i < destinations.size(); ++i) {
        // Select first dest as transfer chain.
        bool is_transfer_chain = (i == 0);
        const auto& fragment_instance_id = destinations[i].fragment_instance_id;
        if (fragment_id_to_channel_index.find(fragment_instance_id.lo) ==
            fragment_id_to_channel_index.end()) {
            _channel_shared_ptrs.emplace_back(
                    new Channel(this, row_desc, destinations[i].brpc_server, fragment_instance_id,
                                sink.dest_node_id, per_channel_buffer_size, is_transfer_chain,
                                send_query_statistics_with_every_batch));
            fragment_id_to_channel_index.emplace(fragment_instance_id.lo,
                                                 _channel_shared_ptrs.size() - 1);
            _channels.push_back(_channel_shared_ptrs.back().get());
        } else {
            _channel_shared_ptrs.emplace_back(
                    _channel_shared_ptrs[fragment_id_to_channel_index[fragment_instance_id.lo]]);
        }
    }
    _name = "VDataStreamSender";
}

VDataStreamSender::~VDataStreamSender() {
    _channel_shared_ptrs.clear();
}

Status VDataStreamSender::init(const TDataSink& tsink) {
    RETURN_IF_ERROR(DataSink::init(tsink));
    const TDataStreamSink& t_stream_sink = tsink.stream_sink;
    if (_part_type == TPartitionType::HASH_PARTITIONED ||
        _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
    } else if (_part_type == TPartitionType::RANGE_PARTITIONED) {
        // Range partition
        // Partition Exprs
        RETURN_IF_ERROR(VExpr::create_expr_trees(
                _pool, t_stream_sink.output_partition.partition_exprs, &_partition_expr_ctxs));
        // Partition infos
        int num_parts = t_stream_sink.output_partition.partition_infos.size();
        if (num_parts == 0) {
            return Status::InternalError("Empty partition info.");
        }
        for (int i = 0; i < num_parts; ++i) {
            VPartitionInfo* info = _pool->add(new VPartitionInfo());
            RETURN_IF_ERROR(VPartitionInfo::from_thrift(
                    _pool, t_stream_sink.output_partition.partition_infos[i], info));
            _partition_infos.push_back(info);
        }
        // partitions should be in ascending order
        std::sort(_partition_infos.begin(), _partition_infos.end(),
                  [](const VPartitionInfo* v1, const VPartitionInfo* v2) {
                      return v1->range() < v2->range();
                  });
    } else {
        // UNPARTITIONED
    }
    return Status::OK();
}

Status VDataStreamSender::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));
    _state = state;

    std::vector<std::string> instances;
    for (const auto& channel : _channels) {
        instances.emplace_back(channel->get_fragment_instance_id_str());
    }
    std::string title = fmt::format("DataStreamSender (dst_id={}, dst_fragments=[{}])",
                                    _dest_node_id, instances);
    _profile = _pool->add(new RuntimeProfile(std::move(title)));
    SCOPED_TIMER(_profile->total_time_counter());
    _mem_tracker = MemTracker::CreateTracker(
            _profile, -1, "DataStreamSender:" + print_id(state->fragment_instance_id()),
            state->instance_mem_tracker());

    if (_part_type == TPartitionType::UNPARTITIONED || _part_type == TPartitionType::RANDOM) {
        // Randomize the order we open/transmit to channels to avoid thundering herd problems.
        srand(reinterpret_cast<uint64_t>(this));
        random_shuffle(_channels.begin(), _channels.end());
    } else if (_part_type == TPartitionType::HASH_PARTITIONED ||
               _part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
    } else {
        RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, _row_desc, _expr_mem_tracker));
        for (auto iter : _partition_infos) {
            RETURN_IF_ERROR(iter->prepare(state, _row_desc, _expr_mem_tracker));
        }
    }

    _bytes_sent_counter = ADD_COUNTER(profile(), "BytesSent", TUnit::BYTES);
    _uncompressed_bytes_counter = ADD_COUNTER(profile(), "UncompressedRowBatchSize", TUnit::BYTES);
    _ignore_rows = ADD_COUNTER(profile(), "IgnoreRows", TUnit::UNIT);
    _serialize_batch_timer = ADD_TIMER(profile(), "SerializeBatchTime");
    _overall_throughput = profile()->add_derived_counter(
            "OverallThroughput", TUnit::BYTES_PER_SECOND,
            boost::bind<int64_t>(&RuntimeProfile::units_per_second, _bytes_sent_counter,
                                 profile()->total_time_counter()),
            "");

    for (int i = 0; i < _channels.size(); ++i) {
        RETURN_IF_ERROR(_channels[i]->init(state));
    }
    return Status::OK();
}

Status VDataStreamSender::open(RuntimeState* state) {
    DCHECK(state != nullptr);
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));
    for (auto iter : _partition_infos) {
        RETURN_IF_ERROR(iter->open(state));
    }
    return Status::OK();
}

Status VDataStreamSender::send(RuntimeState* state, RowBatch* batch) {
    return Status::NotSupported("Not Implemented VOlapScanNode Node::get_next scalar");
}

Status VDataStreamSender::send(RuntimeState* state, Block* block) {
    SCOPED_TIMER(_profile->total_time_counter());
    if (_part_type == TPartitionType::UNPARTITIONED || _channels.size() == 1) {
        // 1. serialize
        // 2. send batch
        // 3. switch proto
        return handle_unpartitioned(block);
    } else if (_part_type == TPartitionType::RANDOM) {
        // 1. select channel
        Channel* current_channel = _channels[_current_channel_idx];
        // 2. serialize
        // 3. send batch
        // 4. switch proto
    } else if (_part_type == TPartitionType::HASH_PARTITIONED) {
        // TODO: use vectorized hash caculate
        int num_channels = _channels.size();
        // will only copy schema
        // we don't want send temp columns
        auto send_block = *block;

        std::vector<int> result(_partition_expr_ctxs.size());
        int counter = 0;

        for (auto ctx : _partition_expr_ctxs) {
            RETURN_IF_ERROR(ctx->execute(block, &result[counter++]));
        }

        // caculate hash
        int rows = block->rows();
        for (int i = 0; i < rows; ++i) {
            SipHash siphash;
            for (int j = 0; j < result.size(); ++j) {
                auto column = block->get_by_position(result[j]).column;
                column->update_hash_with_value(i, siphash);
            }
            auto target_channel_id = siphash.get64() % num_channels;
            RETURN_IF_ERROR(_channels[target_channel_id]->add_row(&send_block, i));
        }
        return Status::OK();

    } else if (_part_type == TPartitionType::BUCKET_SHFFULE_HASH_PARTITIONED) {
        // 1. caculate hash
        // 2. dispatch rows to channel
    } else {
        // Range partition
        // 1. caculate range
        // 2. dispatch rows to channel
    }
    DCHECK(false);
    return Status::OK();
}

Status VDataStreamSender::close(RuntimeState* state, Status exec_status) {
    if (_closed) return Status::OK();
    _closed = true;

    Status final_st = Status::OK();
    for (int i = 0; i < _channels.size(); ++i) {
        Status st = _channels[i]->close(state);
        if (!st.ok() && final_st.ok()) {
            final_st = st;
        }
    }
    // wait all channels to finish
    for (int i = 0; i < _channels.size(); ++i) {
        Status st = _channels[i]->close_wait(state);
        if (!st.ok() && final_st.ok()) {
            final_st = st;
        }
    }
    for (auto iter : _partition_infos) {
        iter->close(state);
    }
    VExpr::close(_partition_expr_ctxs, state);
    return final_st;
}

Status VDataStreamSender::handle_unpartitioned(Block* block) {
    RETURN_IF_ERROR(serialize_block(block, _current_pb_block, _channels.size()));
    for (auto channel : _channels) {
        RETURN_IF_ERROR(channel->send_block(_current_pb_block));
    }
    _current_pb_block = (_current_pb_block == &_pb_block1 ? &_pb_block2 : &_pb_block1);
    VLOG_ROW << "send rows:" << block->rows();
    return Status::OK();
}

Status VDataStreamSender::serialize_block(Block* src, PBlock* dest, int num_receivers) {
    {
        SCOPED_TIMER(_serialize_batch_timer);
        dest->Clear();
        auto uncompressed_bytes = src->serialize(dest);
        auto bytes = dest->ByteSize();

        COUNTER_UPDATE(_bytes_sent_counter, bytes * num_receivers);
        COUNTER_UPDATE(_uncompressed_bytes_counter, uncompressed_bytes * num_receivers);
    }

    return Status::OK();
}

} // namespace doris::vectorized
