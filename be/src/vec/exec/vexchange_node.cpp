#include "vec/exec/vexchange_node.h"

#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "vec/runtime/vdata_stream_mgr.h"
#include "vec/runtime/vdata_stream_recvr.h"

namespace doris::vectorized {
VExchangeNode::VExchangeNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _num_senders(0),
          _is_merging(tnode.exchange_node.__isset.sort_info),
          _stream_recvr(nullptr),
          _offset(tnode.exchange_node.__isset.offset ? tnode.exchange_node.offset : 0) {}

Status VExchangeNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (!_is_merging) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.exchange_node.sort_info, _pool));
    _is_asc_order = tnode.exchange_node.sort_info.is_asc_order;
    _nulls_first = tnode.exchange_node.sort_info.nulls_first;
    return Status::OK();
}

Status VExchangeNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));
    DCHECK_GT(_num_senders, 0);
    _sub_plan_query_statistics_recvr.reset(new QueryStatisticsRecvr());
    _stream_recvr = state->exec_env()->vstream_mgr()->create_recvr(
            state, _input_row_desc, state->fragment_instance_id(), _id, _num_senders,
            config::exchg_node_buffer_size_bytes, _runtime_profile.get(), _is_merging,
            _sub_plan_query_statistics_recvr);

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, _row_descriptor, _row_descriptor,
                                                  expr_mem_tracker()));
    }
    return Status::OK();
}
Status VExchangeNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));

    if (_is_merging) {
        RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
        RETURN_IF_ERROR(_stream_recvr->create_merger(_vsort_exec_exprs.lhs_ordering_expr_ctxs(),
                                                     _is_asc_order, _nulls_first,
                                                     state->batch_size(), _limit, _offset));
    }

    return Status::OK();
}
Status VExchangeNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    return Status::NotSupported("Not Implemented VExchange Node::get_next scalar");
}

Status VExchangeNode::get_next(RuntimeState* state, Block* block, bool* eos) {
    SCOPED_TIMER(runtime_profile()->total_time_counter());
    auto status = _stream_recvr->get_next(block, eos);
    if (block != nullptr) {
        _num_rows_returned += block->rows();
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }
    return status;
}

Status VExchangeNode::close(RuntimeState* state) {
    if (_stream_recvr != nullptr) {
        _stream_recvr->close();
    }
    return ExecNode::close(state);
}

} // namespace doris::vectorized
