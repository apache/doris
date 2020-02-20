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

#include "exec/partitioned_aggregation_node.h"

#include <math.h>
#include <sstream>
#include <thrift/protocol/TDebugProtocol.h>

#include "exec/partitioned_hash_table.inline.h"
#include "exprs/agg_fn_evaluator.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/slot_ref.h"
#include "runtime/buffered_tuple_stream2.inline.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "runtime/tuple_row.h"
#include "udf/udf_internal.h"
#include "util/runtime_profile.h"
#include "util/stack_util.h"

#include "gen_cpp/Exprs_types.h"
#include "gen_cpp/PlanNodes_types.h"

using std::list;

namespace doris {

PartitionedAggregationNode::PartitionedAggregationNode(
        ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs) :
        ExecNode(pool, tnode, descs),
        _intermediate_tuple_id(tnode.agg_node.intermediate_tuple_id),
        _intermediate_tuple_desc(NULL),
        _output_tuple_id(tnode.agg_node.output_tuple_id),
        _output_tuple_desc(NULL),
        _needs_finalize(tnode.agg_node.need_finalize),
        _needs_serialize(false),
        _block_mgr_client(NULL),
        _output_partition(NULL),
        _process_row_batch_fn(NULL),
        _build_timer(NULL),
        _ht_resize_timer(NULL),
        _get_results_timer(NULL),
        _num_hash_buckets(NULL),
        _partitions_created(NULL),
        // _max_partition_level(NULL),
        _num_row_repartitioned(NULL),
        _num_repartitions(NULL),
        _singleton_output_tuple(NULL),
        _singleton_output_tuple_returned(true),
        _partition_pool(new ObjectPool()) {
    DCHECK_EQ(PARTITION_FANOUT, 1 << NUM_PARTITIONING_BITS);
}

Status PartitionedAggregationNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    RETURN_IF_ERROR(
            Expr::create_expr_trees(_pool, tnode.agg_node.grouping_exprs, &_probe_expr_ctxs));
    for (int i = 0; i < tnode.agg_node.aggregate_functions.size(); ++i) {
        AggFnEvaluator* evaluator = NULL;
        RETURN_IF_ERROR(AggFnEvaluator::create(
                    _pool, tnode.agg_node.aggregate_functions[i], &evaluator));
        _aggregate_evaluators.push_back(evaluator);
    }
    return Status::OK();
}

Status PartitionedAggregationNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    RETURN_IF_ERROR(ExecNode::prepare(state));
    _state = state;

    _mem_pool.reset(new MemPool(mem_tracker()));
    _agg_fn_pool.reset(new MemPool(expr_mem_tracker()));

    _build_timer = ADD_TIMER(runtime_profile(), "BuildTime");
    _ht_resize_timer = ADD_TIMER(runtime_profile(), "HTResizeTime");
    _get_results_timer = ADD_TIMER(runtime_profile(), "GetResultsTime");
    _num_hash_buckets = ADD_COUNTER(runtime_profile(), "HashBuckets", TUnit::UNIT);
    _partitions_created = ADD_COUNTER(runtime_profile(), "PartitionsCreated", TUnit::UNIT);
    // _max_partition_level = runtime_profile()->AddHighWaterMarkCounter(
    //         "MaxPartitionLevel", TUnit::UNIT);
    _num_row_repartitioned = ADD_COUNTER(
            runtime_profile(), "RowsRepartitioned", TUnit::UNIT);
    _num_repartitions = ADD_COUNTER(runtime_profile(), "NumRepartitions", TUnit::UNIT);
    _num_spilled_partitions = ADD_COUNTER(
            runtime_profile(), "SpilledPartitions", TUnit::UNIT);
    // _largest_partition_percent = runtime_profile()->AddHighWaterMarkCounter(
    //         "LargestPartitionPercent", TUnit::UNIT);

    _intermediate_tuple_desc =
        state->desc_tbl().get_tuple_descriptor(_intermediate_tuple_id);
    _output_tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    DCHECK_EQ(_intermediate_tuple_desc->slots().size(), _output_tuple_desc->slots().size());

    RETURN_IF_ERROR(
            Expr::prepare(_probe_expr_ctxs, state, child(0)->row_desc(), expr_mem_tracker()));
    // AddExprCtxsToFree(_probe_expr_ctxs);

    _contains_var_len_grouping_exprs = false;
    // Construct build exprs from _intermediate_agg_tuple_desc
    for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
        SlotDescriptor* desc = _intermediate_tuple_desc->slots()[i];
        DCHECK(desc->type().type == TYPE_NULL ||
                desc->type().type == _probe_expr_ctxs[i]->root()->type().type);
        // Hack to avoid TYPE_NULL SlotRefs.
        Expr* expr = desc->type().type != TYPE_NULL ?
            new SlotRef(desc) : new SlotRef(desc, TYPE_BOOLEAN);
        state->obj_pool()->add(expr);
        _build_expr_ctxs.push_back(new ExprContext(expr));
        state->obj_pool()->add(_build_expr_ctxs.back());
        _contains_var_len_grouping_exprs |= expr->type().is_string_type();
    }
    // Construct a new row desc for preparing the build exprs because neither the child's
    // nor this node's output row desc may contain the intermediate tuple, e.g.,
    // in a single-node plan with an intermediate tuple different from the output tuple.
    _intermediate_row_desc.reset(new RowDescriptor(_intermediate_tuple_desc, false));
    RETURN_IF_ERROR(
            Expr::prepare(_build_expr_ctxs, state, *_intermediate_row_desc, expr_mem_tracker()));
    // AddExprCtxsToFree(_build_expr_ctxs);

    int j = _probe_expr_ctxs.size();
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++j) {
        // Skip non-materialized slots; we don't have evaluators instantiated for those.
        while (!_intermediate_tuple_desc->slots()[j]->is_materialized()) {
            DCHECK_LT(j, _intermediate_tuple_desc->slots().size() - 1)
                << "#eval= " << _aggregate_evaluators.size()
                << " #probe=" << _probe_expr_ctxs.size();
            ++j;
        }
        // SlotDescriptor* intermediate_slot_desc = _intermediate_tuple_desc->slots()[j];
        SlotDescriptor* output_slot_desc = _output_tuple_desc->slots()[j];
        FunctionContext* agg_fn_ctx = NULL;
        // RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, child(0)->row_desc(),
        //             intermediate_slot_desc, output_slot_desc, _agg_fn_pool.get(), &agg_fn_ctx));
        RETURN_IF_ERROR(_aggregate_evaluators[i]->prepare(state, child(0)->row_desc(),
                        _agg_fn_pool.get(), output_slot_desc, output_slot_desc,
                        expr_mem_tracker(), &agg_fn_ctx));
        _agg_fn_ctxs.push_back(agg_fn_ctx);
        state->obj_pool()->add(agg_fn_ctx);
        _needs_serialize |= _aggregate_evaluators[i]->supports_serialize();
    }

    if (_probe_expr_ctxs.empty()) {
        // Create single output tuple now; we need to output something
        // even if our input is empty.
        _singleton_output_tuple =
                construct_intermediate_tuple(_agg_fn_ctxs, _mem_pool.get(), NULL, NULL);
        // Check for failures during AggFnEvaluator::init().
        RETURN_IF_ERROR(_state->query_status());
        _singleton_output_tuple_returned = false;
    } else {
        _ht_ctx.reset(new PartitionedHashTableCtx(_build_expr_ctxs, _probe_expr_ctxs, true, true,
                state->fragment_hash_seed(), MAX_PARTITION_DEPTH, 1));
        RETURN_IF_ERROR(_state->block_mgr2()->register_client(
                    min_required_buffers(), mem_tracker(), state, &_block_mgr_client));
        RETURN_IF_ERROR(create_hash_partitions(0));
    }

    // TODO: Is there a need to create the stream here? If memory reservations work we may
    // be able to create this stream lazily and only whenever we need to spill.
    if (_needs_serialize && _block_mgr_client != NULL) {
        _serialize_stream.reset(new BufferedTupleStream2(state, *_intermediate_row_desc,
                    state->block_mgr2(), _block_mgr_client, false /* use_initial_small_buffers */,
                    false /* read_write */));
        RETURN_IF_ERROR(_serialize_stream->init(id(), runtime_profile(), false));
        DCHECK(_serialize_stream->has_write_block());
    }

    return Status::OK();
}

Status PartitionedAggregationNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));

    RETURN_IF_ERROR(Expr::open(_probe_expr_ctxs, state));
    RETURN_IF_ERROR(Expr::open(_build_expr_ctxs, state));

    DCHECK_EQ(_aggregate_evaluators.size(), _agg_fn_ctxs.size());
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        RETURN_IF_ERROR(_aggregate_evaluators[i]->open(state, _agg_fn_ctxs[i]));
    }

    // Read all the rows from the child and process them.
    RETURN_IF_ERROR(_children[0]->open(state));
    RowBatch batch(_children[0]->row_desc(), state->batch_size(), mem_tracker());
    bool eos = false;
    do {
        RETURN_IF_CANCELLED(state);
        RETURN_IF_ERROR(state->check_query_state("Partitioned aggregation, while getting next from child 0."));
        RETURN_IF_ERROR(_children[0]->get_next(state, &batch, &eos));

        if (UNLIKELY(VLOG_ROW_IS_ON)) {
            for (int i = 0; i < batch.num_rows(); ++i) {
                TupleRow* row = batch.get_row(i);
                VLOG_ROW << "partition-agg-node input row: "
                        << row->to_string(_children[0]->row_desc());
            }
        }

        SCOPED_TIMER(_build_timer);
        if (_process_row_batch_fn != NULL) {
            RETURN_IF_ERROR(_process_row_batch_fn(this, &batch, _ht_ctx.get()));
        } else if (_probe_expr_ctxs.empty()) {
            RETURN_IF_ERROR(process_batch_no_grouping(&batch));
        } else {
            // VLOG_ROW << "partition-agg-node batch: " << batch->to_string();
            // There is grouping, so we will do partitioned aggregation.
            RETURN_IF_ERROR(process_batch<false>(&batch, _ht_ctx.get()));
        }
        batch.reset();
    } while (!eos);

    // Unless we are inside a subplan expecting to call open()/get_next() on the child
    // again, the child can be closed at this point. We have consumed all of the input
    // from the child and transfered ownership of the resources we need.
    // if (!IsInSubplan()) {
    child(0)->close(state);
    // }

    // Done consuming child(0)'s input. Move all the partitions in _hash_partitions
    // to _spilled_partitions/_aggregated_partitions. We'll finish the processing in
    // get_next().
    if (!_probe_expr_ctxs.empty()) {
        RETURN_IF_ERROR(move_hash_partitions(child(0)->rows_returned()));
    }
    return Status::OK();
}

Status PartitionedAggregationNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::GETNEXT));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("Partitioned aggregation, before evaluating conjuncts."));

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    ExprContext** ctxs = &_conjunct_ctxs[0];
    int num_ctxs = _conjunct_ctxs.size();
    if (_probe_expr_ctxs.empty()) {
        // There was grouping, so evaluate the conjuncts and return the single result row.
        // We allow calling get_next() after eos, so don't return this row again.
        if (!_singleton_output_tuple_returned) {
            int row_idx = row_batch->add_row();
            TupleRow* row = row_batch->get_row(row_idx);
            Tuple* output_tuple = get_output_tuple(
                    _agg_fn_ctxs, _singleton_output_tuple, row_batch->tuple_data_pool());
            row->set_tuple(0, output_tuple);
            if (ExecNode::eval_conjuncts(ctxs, num_ctxs, row)) {
                row_batch->commit_last_row();
                ++_num_rows_returned;
            }
            _singleton_output_tuple_returned = true;
        }
        // Keep the current chunk to amortize the memory allocation over a series
        // of reset()/open()/get_next()* calls.
        row_batch->tuple_data_pool()->acquire_data(_mem_pool.get(), true);
        *eos = true;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
        return Status::OK();
    }

    if (_output_iterator.at_end()) {
        // Done with this partition, move onto the next one.
        if (_output_partition != NULL) {
            _output_partition->close(false);
            _output_partition = NULL;
        }
        if (_aggregated_partitions.empty() && _spilled_partitions.empty()) {
            // No more partitions, all done.
            *eos = true;
            return Status::OK();
        }
        // Process next partition.
        RETURN_IF_ERROR(next_partition());
        DCHECK(_output_partition != NULL);
    }

    SCOPED_TIMER(_get_results_timer);
    int count = 0;
    const int N = BitUtil::next_power_of_two(state->batch_size());
    // Keeping returning rows from the current partition.
    while (!_output_iterator.at_end() && !row_batch->at_capacity()) {
        // This loop can go on for a long time if the conjuncts are very selective. Do query
        // maintenance every N iterations.
        if ((count++ & (N - 1)) == 0) {
            RETURN_IF_CANCELLED(state);
            RETURN_IF_ERROR(state->check_query_state("Partitioned aggregation, while evaluating conjuncts."));
        }

        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        Tuple* intermediate_tuple = _output_iterator.get_tuple();
        Tuple* output_tuple = get_output_tuple(
                _output_partition->agg_fn_ctxs, intermediate_tuple, row_batch->tuple_data_pool());
        _output_iterator.next();
        row->set_tuple(0, output_tuple);
        if (ExecNode::eval_conjuncts(ctxs, num_ctxs, row)) {
            row_batch->commit_last_row();
            ++_num_rows_returned;
            if (reached_limit()) {
                break; // TODO: remove this check? is this expensive?
            }
        }
    }
    COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    *eos = reached_limit();
    if (_output_iterator.at_end()) {
        row_batch->mark_need_to_return();
    }
    return Status::OK();
}

void PartitionedAggregationNode::cleanup_hash_tbl(
        const vector<FunctionContext*>& agg_fn_ctxs, PartitionedHashTable::Iterator it) {
    if (!_needs_finalize && !_needs_serialize) {
        return;
    }

    // Iterate through the remaining rows in the hash table and call serialize/finalize on
    // them in order to free any memory allocated by UDAs.
    if (_needs_finalize) {
        // finalize() requires a dst tuple but we don't actually need the result,
        // so allocate a single dummy tuple to avoid accumulating memory.
        Tuple* dummy_dst = NULL;
        dummy_dst = Tuple::create(_output_tuple_desc->byte_size(), _mem_pool.get());
        while (!it.at_end()) {
            Tuple* tuple = it.get_tuple();
            AggFnEvaluator::finalize(_aggregate_evaluators, agg_fn_ctxs, tuple, dummy_dst);
            it.next();
        }
    } else {
        while (!it.at_end()) {
            Tuple* tuple = it.get_tuple();
            AggFnEvaluator::serialize(_aggregate_evaluators, agg_fn_ctxs, tuple);
            it.next();
        }
    }
}

Status PartitionedAggregationNode::reset(RuntimeState* state) {
    if (_probe_expr_ctxs.empty()) {
        // Re-create the single output tuple for this non-grouping agg.
        _singleton_output_tuple =
            construct_intermediate_tuple(_agg_fn_ctxs, _mem_pool.get(), NULL, NULL);
        // Check for failures during AggFnEvaluator::init().
        RETURN_IF_ERROR(_state->query_status());
        _singleton_output_tuple_returned = false;
    } else {
        // Reset the HT and the partitions for this grouping agg.
        _ht_ctx->set_level(0);
        close_partitions();
        create_hash_partitions(0);
    }
    // return ExecNode::reset(state);
    return Status::OK();
}

Status PartitionedAggregationNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }

    if (!_singleton_output_tuple_returned) {
        DCHECK_EQ(_agg_fn_ctxs.size(), _aggregate_evaluators.size());
        get_output_tuple(_agg_fn_ctxs, _singleton_output_tuple, _mem_pool.get());
    }

    // Iterate through the remaining rows in the hash table and call serialize/finalize on
    // them in order to free any memory allocated by UDAs
    if (_output_partition != NULL) {
        cleanup_hash_tbl(_output_partition->agg_fn_ctxs, _output_iterator);
        _output_partition->close(false);
    }

    close_partitions();

    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        _aggregate_evaluators[i]->close(state);
    }
    for (int i = 0; i < _agg_fn_ctxs.size(); ++i) {
        _agg_fn_ctxs[i]->impl()->close();
    }
    if (_agg_fn_pool.get() != NULL) {
        _agg_fn_pool->free_all();
    }
    if (_mem_pool.get() != NULL) {
        _mem_pool->free_all();
    }
    if (_ht_ctx.get() != NULL) {
        _ht_ctx->close();
    }
    if (_serialize_stream.get() != NULL) {
        _serialize_stream->close();
    }

    if (_block_mgr_client != NULL) {
        state->block_mgr2()->clear_reservations(_block_mgr_client);
    }

    Expr::close(_probe_expr_ctxs, state);
    Expr::close(_build_expr_ctxs, state);
    return ExecNode::close(state);
}

Status PartitionedAggregationNode::Partition::init_streams() {
    agg_fn_pool.reset(new MemPool(parent->expr_mem_tracker()));
    DCHECK_EQ(agg_fn_ctxs.size(), 0);
    for (int i = 0; i < parent->_agg_fn_ctxs.size(); ++i) {
        agg_fn_ctxs.push_back(parent->_agg_fn_ctxs[i]->impl()->clone(agg_fn_pool.get()));
        parent->_partition_pool->add(agg_fn_ctxs[i]);
    }

    aggregated_row_stream.reset(new BufferedTupleStream2(parent->_state,
                *parent->_intermediate_row_desc, parent->_state->block_mgr2(),
                parent->_block_mgr_client, true /* use_initial_small_buffers */,
                false /* read_write */));
    RETURN_IF_ERROR(aggregated_row_stream->init(parent->id(), parent->runtime_profile(), true));

    unaggregated_row_stream.reset(new BufferedTupleStream2(parent->_state,
                parent->child(0)->row_desc(), parent->_state->block_mgr2(),
                parent->_block_mgr_client, true /* use_initial_small_buffers */,
                false /* read_write */));
    // This stream is only used to spill, no need to ever have this pinned.
    RETURN_IF_ERROR(unaggregated_row_stream->init(parent->id(), parent->runtime_profile(), false));
    DCHECK(unaggregated_row_stream->has_write_block());
    return Status::OK();
}

bool PartitionedAggregationNode::Partition::init_hash_table() {
    DCHECK(hash_tbl.get() == NULL);
    // We use the upper PARTITION_FANOUT num bits to pick the partition so only the
    // remaining bits can be used for the hash table.
    // TODO: we could switch to 64 bit hashes and then we don't need a max size.
    // It might be reasonable to limit individual hash table size for other reasons
    // though. Always start with small buffers.
    // TODO: How many buckets? We currently use a default value, 1024.
    static const int64_t PAGG_DEFAULT_HASH_TABLE_SZ = 1024;
    hash_tbl.reset(PartitionedHashTable::create(parent->_state, parent->_block_mgr_client, 1,
                NULL, 1 << (32 - NUM_PARTITIONING_BITS), PAGG_DEFAULT_HASH_TABLE_SZ));
    return hash_tbl->init();
}

Status PartitionedAggregationNode::Partition::clean_up() {
    if (parent->_needs_serialize && aggregated_row_stream->num_rows() != 0) {
        // We need to do a lot more work in this case. This step effectively does a merge
        // aggregation in this node. We need to serialize the intermediates, spill the
        // intermediates and then feed them into the aggregate function's merge step.
        // This is often used when the intermediate is a string type, meaning the current
        // (before serialization) in-memory layout is not the on-disk block layout.
        // The disk layout does not support mutable rows. We need to rewrite the stream
        // into the on disk format.
        // TODO: if it happens to not be a string, we could serialize in place. This is
        // a future optimization since it is very unlikely to have a serialize phase
        // for those UDAs.
        DCHECK(parent->_serialize_stream.get() != NULL);
        DCHECK(!parent->_serialize_stream->is_pinned());
        DCHECK(parent->_serialize_stream->has_write_block());

        const vector<AggFnEvaluator*>& evaluators = parent->_aggregate_evaluators;

        // serialize and copy the spilled partition's stream into the new stream.
        Status status = Status::OK();
        bool failed_to_add = false;
        BufferedTupleStream2* new_stream = parent->_serialize_stream.get();
        PartitionedHashTable::Iterator it = hash_tbl->begin(parent->_ht_ctx.get());
        while (!it.at_end()) {
            Tuple* tuple = it.get_tuple();
            it.next();
            AggFnEvaluator::serialize(evaluators, agg_fn_ctxs, tuple);
            if (UNLIKELY(!new_stream->add_row(reinterpret_cast<TupleRow*>(&tuple), &status))) {
                failed_to_add = true;
                break;
            }
        }

        // Even if we can't add to new_stream, finish up processing this agg stream to make
        // clean up easier (someone has to finalize this stream and we don't want to remember
        // where we are).
        if (failed_to_add) {
            parent->cleanup_hash_tbl(agg_fn_ctxs, it);
            hash_tbl->close();
            hash_tbl.reset();
            aggregated_row_stream->close();
            RETURN_IF_ERROR(status);
            return parent->_state->block_mgr2()->mem_limit_too_low_error(parent->_block_mgr_client,
                    parent->id());
        }
        DCHECK(status.ok());

        aggregated_row_stream->close();
        aggregated_row_stream.swap(parent->_serialize_stream);
        // Recreate the serialize_stream (and reserve 1 buffer) now in preparation for
        // when we need to spill again. We need to have this available before we need
        // to spill to make sure it is available. This should be acquirable since we just
        // freed at least one buffer from this partition's (old) aggregated_row_stream.
        parent->_serialize_stream.reset(new BufferedTupleStream2(parent->_state,
                    *parent->_intermediate_row_desc, parent->_state->block_mgr2(),
                    parent->_block_mgr_client, false /* use_initial_small_buffers */,
                    false /* read_write */));
        status = parent->_serialize_stream->init(parent->id(), parent->runtime_profile(), false);
        if (!status.ok()) {
            hash_tbl->close();
            hash_tbl.reset();
            return status;
        }
        DCHECK(parent->_serialize_stream->has_write_block());
    }
    return Status::OK();
}

Status PartitionedAggregationNode::Partition::spill() {
    DCHECK(!is_closed);
    DCHECK(!is_spilled());

    RETURN_IF_ERROR(clean_up());

    // Free the in-memory result data.
    for (int i = 0; i < agg_fn_ctxs.size(); ++i) {
        agg_fn_ctxs[i]->impl()->close();
    }

    if (agg_fn_pool.get() != NULL) {
        agg_fn_pool->free_all();
        agg_fn_pool.reset();
    }

    hash_tbl->close();
    hash_tbl.reset();

    // Try to switch both streams to IO-sized buffers to avoid allocating small buffers
    // for spilled partition.
    bool got_buffer = true;
    if (aggregated_row_stream->using_small_buffers()) {
        RETURN_IF_ERROR(aggregated_row_stream->switch_to_io_buffers(&got_buffer));
    }
    // Unpin the stream as soon as possible to increase the changes that the
    // switch_to_io_buffers() call below will succeed.
    DCHECK(!got_buffer || aggregated_row_stream->has_write_block())
            << aggregated_row_stream->debug_string();
    RETURN_IF_ERROR(aggregated_row_stream->unpin_stream(false));

    if (got_buffer && unaggregated_row_stream->using_small_buffers()) {
        RETURN_IF_ERROR(unaggregated_row_stream->switch_to_io_buffers(&got_buffer));
    }
    if (!got_buffer) {
        // We'll try again to get the buffers when the stream fills up the small buffers.
        VLOG_QUERY << "Not enough memory to switch to IO-sized buffer for partition "
            << this << " of agg=" << parent->_id << " agg small buffers="
            << aggregated_row_stream->using_small_buffers()
            << " unagg small buffers="
            << unaggregated_row_stream->using_small_buffers();
        VLOG_FILE << get_stack_trace();
    }

    COUNTER_UPDATE(parent->_num_spilled_partitions, 1);
    if (parent->_num_spilled_partitions->value() == 1) {
        parent->add_runtime_exec_option("Spilled");
    }
    return Status::OK();
}

void PartitionedAggregationNode::Partition::close(bool finalize_rows) {
    if (is_closed) {
        return;
    }
    is_closed = true;
    if (aggregated_row_stream.get() != NULL) {
        if (finalize_rows && hash_tbl.get() != NULL) {
            // We need to walk all the rows and finalize them here so the UDA gets a chance
            // to cleanup. If the hash table is gone (meaning this was spilled), the rows
            // should have been finalized/serialized in spill().
            parent->cleanup_hash_tbl(agg_fn_ctxs, hash_tbl->begin(parent->_ht_ctx.get()));
        }
        aggregated_row_stream->close();
    }
    if (hash_tbl.get() != NULL) {
        hash_tbl->close();
    }
    if (unaggregated_row_stream.get() != NULL) {
        unaggregated_row_stream->close();
    }

    for (int i = 0; i < agg_fn_ctxs.size(); ++i) {
        agg_fn_ctxs[i]->impl()->close();
    }
    if (agg_fn_pool.get() != NULL) {
        agg_fn_pool->free_all();
    }
}

Tuple* PartitionedAggregationNode::construct_intermediate_tuple(
        const vector<FunctionContext*>& agg_fn_ctxs, MemPool* pool,
        BufferedTupleStream2* stream, Status* status) {
    Tuple* intermediate_tuple = NULL;
    uint8_t* buffer = NULL;
    if (pool != NULL) {
        DCHECK(stream == NULL && status == NULL);
        intermediate_tuple = Tuple::create(_intermediate_tuple_desc->byte_size(), pool);
    } else {
        DCHECK(stream != NULL && status != NULL);
        // Figure out how big it will be to copy the entire tuple. We need the tuple to end
        // up in one block in the stream.
        int size = _intermediate_tuple_desc->byte_size();
        if (_contains_var_len_grouping_exprs) {
            // TODO: This is likely to be too slow. The hash table could maintain this as
            // it hashes.
            for (int i = 0; i < _probe_expr_ctxs.size(); ++i) {
                if (!_probe_expr_ctxs[i]->root()->type().is_string_type()) {
                    continue;
                }
                if (_ht_ctx->last_expr_value_null(i)) {
                    continue;
                }
                StringValue* sv = reinterpret_cast<StringValue*>(_ht_ctx->last_expr_value(i));
                size += sv->len;
            }
        }

        // Now that we know the size of the row, allocate space for it in the stream.
        buffer = stream->allocate_row(size, status);
        if (buffer == NULL) {
            if (!status->ok() || !stream->using_small_buffers()) {
                return NULL;
            }
            // IMPALA-2352: Make a best effort to switch to IO buffers and re-allocate.
            // If switch_to_io_buffers() fails the caller of this function can try to free
            // some space, e.g. through spilling, and re-attempt to allocate space for
            // this row.
            bool got_buffer = false;
            *status = stream->switch_to_io_buffers(&got_buffer);
            if (!status->ok() || !got_buffer) {
                return NULL;
            }
            buffer = stream->allocate_row(size, status);
            if (buffer == NULL) {
                return NULL;
            }
        }
        intermediate_tuple = reinterpret_cast<Tuple*>(buffer);
        // TODO: remove this. we shouldn't need to zero the entire tuple.
        intermediate_tuple->init(size);
        buffer += _intermediate_tuple_desc->byte_size();
    }

    // Copy grouping values.
    vector<SlotDescriptor*>::const_iterator slot_desc = _intermediate_tuple_desc->slots().begin();
    for (int i = 0; i < _probe_expr_ctxs.size(); ++i, ++slot_desc) {
        if (_ht_ctx->last_expr_value_null(i)) {
            intermediate_tuple->set_null((*slot_desc)->null_indicator_offset());
        } else {
            void* src = _ht_ctx->last_expr_value(i);
            void* dst = intermediate_tuple->get_slot((*slot_desc)->tuple_offset());
            if (stream == NULL) {
                RawValue::write(src, dst, (*slot_desc)->type(), pool);
            } else {
                RawValue::write(src, (*slot_desc)->type(), dst, &buffer);
            }
        }
    }

    // Initialize aggregate output.
    for (int i = 0; i < _aggregate_evaluators.size(); ++i, ++slot_desc) {
        while (!(*slot_desc)->is_materialized()) {
            ++slot_desc;
        }
        AggFnEvaluator* evaluator = _aggregate_evaluators[i];
        evaluator->init(agg_fn_ctxs[i], intermediate_tuple);
        // Codegen specific path for min/max.
        // To minimize branching on the update_tuple path, initialize the result value
        // so that update_tuple doesn't have to check if the aggregation
        // dst slot is null.
        // TODO: remove when we don't use the irbuilder for codegen here.  This optimization
        // will no longer be necessary when all aggregates are implemented with the UDA
        // interface.
        // if ((*slot_desc)->type().type != TYPE_STRING &&
        //         (*slot_desc)->type().type != TYPE_VARCHAR &&
        //         (*slot_desc)->type().type != TYPE_TIMESTAMP &&
        //         (*slot_desc)->type().type != TYPE_CHAR &&
        //         (*slot_desc)->type().type != TYPE_DECIMAL) {
        if (!(*slot_desc)->type().is_string_type()
                && !(*slot_desc)->type().is_date_type()) {
            ExprValue default_value;
            void* default_value_ptr = NULL;
            switch (evaluator->agg_op()) {
                case AggFnEvaluator::MIN:
                    default_value_ptr = default_value.set_to_max((*slot_desc)->type());
                    RawValue::write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
                    break;
                case AggFnEvaluator::MAX:
                    default_value_ptr = default_value.set_to_min((*slot_desc)->type());
                    RawValue::write(default_value_ptr, intermediate_tuple, *slot_desc, NULL);
                    break;
                default:
                    break;
            }
        }
    }
    return intermediate_tuple;
}

void PartitionedAggregationNode::update_tuple(FunctionContext** agg_fn_ctxs,
        Tuple* tuple, TupleRow* row, bool is_merge) {
    DCHECK(tuple != NULL || _aggregate_evaluators.empty());
    for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
        if (is_merge) {
            _aggregate_evaluators[i]->merge(agg_fn_ctxs[i], row->get_tuple(0), tuple);
        } else {
            _aggregate_evaluators[i]->add(agg_fn_ctxs[i], row, tuple);
        }
    }
}

Tuple* PartitionedAggregationNode::get_output_tuple(
        const vector<FunctionContext*>& agg_fn_ctxs, Tuple* tuple, MemPool* pool) {
    DCHECK(tuple != NULL || _aggregate_evaluators.empty()) << tuple;
    Tuple* dst = tuple;
    // if (_needs_finalize && _intermediate_tuple_id != _output_tuple_id) {
    if (_needs_finalize) {
        dst = Tuple::create(_output_tuple_desc->byte_size(), pool);
    }
    if (_needs_finalize) {
        AggFnEvaluator::finalize(_aggregate_evaluators, agg_fn_ctxs, tuple, dst);
    } else {
        AggFnEvaluator::serialize(_aggregate_evaluators, agg_fn_ctxs, tuple);
    }
    // Copy grouping values from tuple to dst.
    // TODO: Codegen this.
    if (dst != tuple) {
        int num_grouping_slots = _probe_expr_ctxs.size();
        for (int i = 0; i < num_grouping_slots; ++i) {
            SlotDescriptor* src_slot_desc = _intermediate_tuple_desc->slots()[i];
            SlotDescriptor* dst_slot_desc = _output_tuple_desc->slots()[i];
            bool src_slot_null = tuple->is_null(src_slot_desc->null_indicator_offset());
            void* src_slot = NULL;
            if (!src_slot_null) {
                src_slot = tuple->get_slot(src_slot_desc->tuple_offset());
            }
            RawValue::write(src_slot, dst, dst_slot_desc, NULL);
        }
    }
    return dst;
}

Status PartitionedAggregationNode::append_spilled_row(BufferedTupleStream2* stream, TupleRow* row) {
    DCHECK(stream != NULL);
    DCHECK(!stream->is_pinned());
    DCHECK(stream->has_write_block());
    if (LIKELY(stream->add_row(row, &_process_batch_status))) {
        return Status::OK();
    }

    // Adding fails iff either we hit an error or haven't switched to I/O buffers.
    RETURN_IF_ERROR(_process_batch_status);
    while (true) {
        bool got_buffer = false;
        RETURN_IF_ERROR(stream->switch_to_io_buffers(&got_buffer));
        if (got_buffer) {
            break;
        }
        RETURN_IF_ERROR(spill_partition());
    }

    // Adding the row should succeed after the I/O buffer switch.
    if (stream->add_row(row, &_process_batch_status)) {
        return Status::OK();
    }
    DCHECK(!_process_batch_status.ok());
    return _process_batch_status;
}

void PartitionedAggregationNode::debug_string(int indentation_level, stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "PartitionedAggregationNode("
        << "intermediate_tuple_id=" << _intermediate_tuple_id
        << " output_tuple_id=" << _output_tuple_id
        << " needs_finalize=" << _needs_finalize
        << " probe_exprs=" << Expr::debug_string(_probe_expr_ctxs)
        << " agg_exprs=" << AggFnEvaluator::debug_string(_aggregate_evaluators);
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

Status PartitionedAggregationNode::create_hash_partitions(int level) {
    if (level >= MAX_PARTITION_DEPTH) {
        stringstream error_msg;
        error_msg << "Cannot perform aggregation at hash aggregation node with id "
                << _id << '.'
                << " The input data was partitioned the maximum number of "
                << MAX_PARTITION_DEPTH << " times."
                << " This could mean there is significant skew in the data or the memory limit is"
                << " set too low.";
        return _state->set_mem_limit_exceeded(error_msg.str());
    }
    _ht_ctx->set_level(level);

    DCHECK(_hash_partitions.empty());
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
        Partition* new_partition = new Partition(this, level);
        DCHECK(new_partition != NULL);
        _hash_partitions.push_back(_partition_pool->add(new_partition));
        RETURN_IF_ERROR(new_partition->init_streams());
    }
    DCHECK_GT(_state->block_mgr2()->num_reserved_buffers_remaining(_block_mgr_client), 0);

    // Now that all the streams are reserved (meaning we have enough memory to execute
    // the algorithm), allocate the hash tables. These can fail and we can still continue.
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
        if (!_hash_partitions[i]->init_hash_table()) {
            RETURN_IF_ERROR(_hash_partitions[i]->spill());
        }
    }
    COUNTER_UPDATE(_partitions_created, PARTITION_FANOUT);
    // COUNTER_SET(_max_partition_level, level);
    return Status::OK();
}

Status PartitionedAggregationNode::check_and_resize_hash_partitions(int num_rows,
        PartitionedHashTableCtx* ht_ctx) {
    for (int i = 0; i < PARTITION_FANOUT; ++i) {
        Partition* partition = _hash_partitions[i];
        while (!partition->is_spilled()) {
            {
                SCOPED_TIMER(_ht_resize_timer);
                if (partition->hash_tbl->check_and_resize(num_rows, ht_ctx)) {
                    break;
                }
            }
            // There was not enough memory for the resize. Spill a partition and retry.
            RETURN_IF_ERROR(spill_partition());
        }
    }
    return Status::OK();
}

int64_t PartitionedAggregationNode::largest_spilled_partition() const {
    int64_t max_rows = 0;
    for (int i = 0; i < _hash_partitions.size(); ++i) {
        Partition* partition = _hash_partitions[i];
        if (partition->is_closed || !partition->is_spilled()) {
            continue;
        }
        int64_t rows = partition->aggregated_row_stream->num_rows() +
                partition->unaggregated_row_stream->num_rows();
        if (rows > max_rows) {
            max_rows = rows;
        }
    }
    return max_rows;
}

Status PartitionedAggregationNode::next_partition() {
    DCHECK(_output_partition == NULL);

    // Keep looping until we get to a partition that fits in memory.
    Partition* partition = NULL;
    while (true) {
        partition = NULL;
        // First return partitions that are fully aggregated (and in memory).
        if (!_aggregated_partitions.empty()) {
            partition = _aggregated_partitions.front();
            DCHECK(!partition->is_spilled());
            _aggregated_partitions.pop_front();
            break;
        }

        if (partition == NULL) {
            DCHECK(!_spilled_partitions.empty());
            DCHECK_EQ(_state->block_mgr2()->num_pinned_buffers(_block_mgr_client),
                    _needs_serialize ? 1 : 0);

            // TODO: we can probably do better than just picking the first partition. We
            // can base this on the amount written to disk, etc.
            partition = _spilled_partitions.front();
            DCHECK(partition->is_spilled());

            // Create the new hash partitions to repartition into.
            // TODO: we don't need to repartition here. We are now working on 1 / FANOUT
            // of the input so it's reasonably likely it can fit. We should look at this
            // partitions size and just do the aggregation if it fits in memory.
            RETURN_IF_ERROR(create_hash_partitions(partition->level + 1));
            COUNTER_UPDATE(_num_repartitions, 1);

            // Rows in this partition could have been spilled into two streams, depending
            // on if it is an aggregated intermediate, or an unaggregated row.
            // Note: we must process the aggregated rows first to save a hash table lookup
            // in process_batch().
            RETURN_IF_ERROR(process_stream<true>(partition->aggregated_row_stream.get()));
            RETURN_IF_ERROR(process_stream<false>(partition->unaggregated_row_stream.get()));

            COUNTER_UPDATE(_num_row_repartitioned, partition->aggregated_row_stream->num_rows());
            COUNTER_UPDATE(_num_row_repartitioned, partition->unaggregated_row_stream->num_rows());

            partition->close(false);
            _spilled_partitions.pop_front();

            // Done processing this partition. Move the new partitions into
            // _spilled_partitions/_aggregated_partitions.
            int64_t num_input_rows = partition->aggregated_row_stream->num_rows() +
                partition->unaggregated_row_stream->num_rows();

            // Check if there was any reduction in the size of partitions after repartitioning.
            int64_t largest_partition = largest_spilled_partition();
            DCHECK_GE(num_input_rows, largest_partition) << "Cannot have a partition with "
                "more rows than the input";
            if (num_input_rows == largest_partition) {
                // Status status = Status::MemTrackerExceeded();
                // status.AddDetail(Substitute("Cannot perform aggregation at node with id $0. "
                //             "Repartitioning did not reduce the size of a spilled partition. "
                //             "Repartitioning level $1. Number of rows $2.",
                //             _id, partition->level + 1, num_input_rows));
                // _state->SetMemTrackerExceeded();
                stringstream error_msg;
                error_msg << "Cannot perform aggregation at node with id " << _id << ". "
                        << "Repartitioning did not reduce the size of a spilled partition. "
                        << "Repartitioning level " << partition->level + 1
                        << ". Number of rows " << num_input_rows << " .";
                return Status::MemoryLimitExceeded(error_msg.str());
            }
            RETURN_IF_ERROR(move_hash_partitions(num_input_rows));
        }
    }

    DCHECK(partition->hash_tbl.get() != NULL);
    DCHECK(partition->aggregated_row_stream->is_pinned());

    _output_partition = partition;
    _output_iterator = _output_partition->hash_tbl->begin(_ht_ctx.get());
    COUNTER_UPDATE(_num_hash_buckets, _output_partition->hash_tbl->num_buckets());
    return Status::OK();
}

template<bool AGGREGATED_ROWS>
Status PartitionedAggregationNode::process_stream(BufferedTupleStream2* input_stream) {
    if (input_stream->num_rows() > 0) {
        while (true) {
            bool got_buffer = false;
            RETURN_IF_ERROR(input_stream->prepare_for_read(true, &got_buffer));
            if (got_buffer) {
                break;
            }
            // Did not have a buffer to read the input stream. Spill and try again.
            RETURN_IF_ERROR(spill_partition());
        }

        bool eos = false;
        RowBatch batch(AGGREGATED_ROWS ? *_intermediate_row_desc : _children[0]->row_desc(),
                _state->batch_size(), mem_tracker());
        do {
            RETURN_IF_ERROR(input_stream->get_next(&batch, &eos));
            RETURN_IF_ERROR(process_batch<AGGREGATED_ROWS>(&batch, _ht_ctx.get()));
            RETURN_IF_ERROR(_state->query_status());
            // free_local_allocations();
            batch.reset();
        } while (!eos);
    }
    input_stream->close();
    return Status::OK();
}

Status PartitionedAggregationNode::spill_partition() {
    int64_t max_freed_mem = 0;
    int partition_idx = -1;

    // Iterate over the partitions and pick the largest partition that is not spilled.
    for (int i = 0; i < _hash_partitions.size(); ++i) {
        if (_hash_partitions[i]->is_closed) {
            continue;
        }
        if (_hash_partitions[i]->is_spilled()) {
            continue;
        }
        // TODO: In PHJ the bytes_in_mem() call also calculates the mem used by the
        // _write_block, why do we ignore it here?
        int64_t mem = _hash_partitions[i]->aggregated_row_stream->bytes_in_mem(true);
        mem += _hash_partitions[i]->hash_tbl->byte_size();
        mem += _hash_partitions[i]->agg_fn_pool->total_reserved_bytes();
        if (mem > max_freed_mem) {
            max_freed_mem = mem;
            partition_idx = i;
        }
    }
    if (partition_idx == -1) {
        // Could not find a partition to spill. This means the mem limit was just too low.
        return _state->block_mgr2()->mem_limit_too_low_error(_block_mgr_client, id());
    }

    return _hash_partitions[partition_idx]->spill();
}

Status PartitionedAggregationNode::move_hash_partitions(int64_t num_input_rows) {
    DCHECK(!_hash_partitions.empty());
    stringstream ss;
    ss << "PA(node_id=" << id() << ") partitioned(level="
        << _hash_partitions[0]->level << ") "
        << num_input_rows << " rows into:" << std::endl;
    for (int i = 0; i < _hash_partitions.size(); ++i) {
        Partition* partition = _hash_partitions[i];
        int64_t aggregated_rows = partition->aggregated_row_stream->num_rows();
        int64_t unaggregated_rows = partition->unaggregated_row_stream->num_rows();
        int64_t total_rows = aggregated_rows + unaggregated_rows;
        double percent = static_cast<double>(total_rows * 100) / num_input_rows;
        ss << "  " << i << " "  << (partition->is_spilled() ? "spilled" : "not spilled")
            << " (fraction=" << std::fixed << std::setprecision(2) << percent << "%)" << std::endl
            << "    #aggregated rows:" << aggregated_rows << std::endl
            << "    #unaggregated rows: " << unaggregated_rows << std::endl;

        // TODO: update counters to support doubles.
        // COUNTER_SET(_largest_partition_percent, static_cast<int64_t>(percent));

        if (total_rows == 0) {
            partition->close(false);
        } else if (partition->is_spilled()) {
            DCHECK(partition->hash_tbl.get() == NULL);
            // We need to unpin all the spilled partitions to make room to allocate new
            // _hash_partitions when we repartition the spilled partitions.
            // TODO: we only need to do this when we have memory pressure. This might be
            // okay though since the block mgr should only write these to disk if there
            // is memory pressure.
            RETURN_IF_ERROR(partition->aggregated_row_stream->unpin_stream(true));
            RETURN_IF_ERROR(partition->unaggregated_row_stream->unpin_stream(true));

            // Push new created partitions at the front. This means a depth first walk
            // (more finely partitioned partitions are processed first). This allows us
            // to delete blocks earlier and bottom out the recursion earlier.
            _spilled_partitions.push_front(partition);
        } else {
            _aggregated_partitions.push_back(partition);
        }

    }
    VLOG(2) << ss.str();
    _hash_partitions.clear();
    return Status::OK();
}

void PartitionedAggregationNode::close_partitions() {
    for (int i = 0; i < _hash_partitions.size(); ++i) {
        _hash_partitions[i]->close(true);
    }
    for (list<Partition*>::iterator it = _aggregated_partitions.begin();
            it != _aggregated_partitions.end(); ++it) {
        (*it)->close(true);
    }
    for (list<Partition*>::iterator it = _spilled_partitions.begin();
            it != _spilled_partitions.end(); ++it) {
        (*it)->close(true);
    }
    _aggregated_partitions.clear();
    _spilled_partitions.clear();
    _hash_partitions.clear();
    _partition_pool->clear();
}

#if 0
// Status PartitionedAggregationNode::QueryMaintenance(RuntimeState* state) {
//   for (int i = 0; i < _aggregate_evaluators.size(); ++i) {
//     ExprContext::free_local_allocations(_aggregate_evaluators[i]->input_expr_ctxs());
//   }
//   ExprContext::free_local_allocations(_agg_fn_ctxs);
//   for (int i = 0; i < _hash_partitions.size(); ++i) {
//     ExprContext::free_local_allocations(_hash_partitions[i]->agg_fn_ctxs);
//   }
//   return ExecNode::QueryMaintenance(state);
// }
//
#endif

}
