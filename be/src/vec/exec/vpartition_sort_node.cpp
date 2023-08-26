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

#include "vec/exec/vpartition_sort_node.h"

#include <glog/logging.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>

#include "common/logging.h"
#include "common/object_pool.h"
#include "runtime/runtime_state.h"
#include "vec/common/hash_table/hash.h"
#include "vec/common/hash_table/hash_set.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris::vectorized {

VPartitionSortNode::VPartitionSortNode(ObjectPool* pool, const TPlanNode& tnode,
                                       const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs), _hash_table_size_counter(nullptr) {
    _partitioned_data = std::make_unique<PartitionedHashMapVariants>();
    _agg_arena_pool = std::make_unique<Arena>();
    _previous_row = std::make_unique<SortCursorCmp>();
}

Status VPartitionSortNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));

    //order by key
    if (tnode.partition_sort_node.__isset.sort_info) {
        RETURN_IF_ERROR(_vsort_exec_exprs.init(tnode.partition_sort_node.sort_info, _pool));
        _is_asc_order = tnode.partition_sort_node.sort_info.is_asc_order;
        _nulls_first = tnode.partition_sort_node.sort_info.nulls_first;
    }
    //partition by key
    if (tnode.partition_sort_node.__isset.partition_exprs) {
        RETURN_IF_ERROR(VExpr::create_expr_trees(tnode.partition_sort_node.partition_exprs,
                                                 _partition_expr_ctxs));
        _partition_exprs_num = _partition_expr_ctxs.size();
        _partition_columns.resize(_partition_exprs_num);
    }

    _has_global_limit = tnode.partition_sort_node.has_global_limit;
    _top_n_algorithm = tnode.partition_sort_node.top_n_algorithm;
    _partition_inner_limit = tnode.partition_sort_node.partition_inner_limit;
    return Status::OK();
}

Status VPartitionSortNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << "VPartitionSortNode::prepare";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    _hash_table_size_counter = ADD_COUNTER(_runtime_profile, "HashTableSize", TUnit::UNIT);
    _build_timer = ADD_TIMER(runtime_profile(), "HashTableBuildTime");
    _partition_sort_timer = ADD_TIMER(runtime_profile(), "PartitionSortTime");
    _get_sorted_timer = ADD_TIMER(runtime_profile(), "GetSortedTime");
    _selector_block_timer = ADD_TIMER(runtime_profile(), "SelectorBlockTime");
    _emplace_key_timer = ADD_TIMER(runtime_profile(), "EmplaceKeyTime");

    RETURN_IF_ERROR(ExecNode::prepare(state));
    RETURN_IF_ERROR(_vsort_exec_exprs.prepare(state, child(0)->row_desc(), _row_descriptor));
    RETURN_IF_ERROR(VExpr::prepare(_partition_expr_ctxs, state, child(0)->row_desc()));
    _init_hash_method();

    return Status::OK();
}

Status VPartitionSortNode::_split_block_by_partition(vectorized::Block* input_block,
                                                     int batch_size) {
    for (int i = 0; i < _partition_exprs_num; ++i) {
        int result_column_id = -1;
        RETURN_IF_ERROR(_partition_expr_ctxs[i]->execute(input_block, &result_column_id));
        DCHECK(result_column_id != -1);
        _partition_columns[i] = input_block->get_by_position(result_column_id).column.get();
    }
    _emplace_into_hash_table(_partition_columns, input_block, batch_size);
    return Status::OK();
}

void VPartitionSortNode::_emplace_into_hash_table(const ColumnRawPtrs& key_columns,
                                                  const vectorized::Block* input_block,
                                                  int batch_size) {
    std::visit(
            [&](auto&& agg_method) -> void {
                SCOPED_TIMER(_build_timer);
                using HashMethodType = std::decay_t<decltype(agg_method)>;
                using HashTableType = std::decay_t<decltype(agg_method.data)>;
                using AggState = typename HashMethodType::State;

                AggState state(key_columns, _partition_key_sz, nullptr);
                size_t num_rows = input_block->rows();
                _pre_serialize_key_if_need(state, agg_method, key_columns, num_rows);

                //PHHashMap
                if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                    if (_hash_values.size() < num_rows) {
                        _hash_values.resize(num_rows);
                    }
                    if constexpr (ColumnsHashing::IsPreSerializedKeysHashMethodTraits<
                                          AggState>::value) {
                        for (size_t i = 0; i < num_rows; ++i) {
                            _hash_values[i] = agg_method.data.hash(agg_method.keys[i]);
                        }
                    } else {
                        for (size_t i = 0; i < num_rows; ++i) {
                            _hash_values[i] =
                                    agg_method.data.hash(state.get_key_holder(i, *_agg_arena_pool));
                        }
                    }
                }

                for (size_t row = 0; row < num_rows; ++row) {
                    SCOPED_TIMER(_emplace_key_timer);
                    PartitionDataPtr aggregate_data = nullptr;
                    auto emplace_result = [&]() {
                        if constexpr (HashTableTraits<HashTableType>::is_phmap) {
                            if (LIKELY(row + HASH_MAP_PREFETCH_DIST < num_rows)) {
                                agg_method.data.prefetch_by_hash(
                                        _hash_values[row + HASH_MAP_PREFETCH_DIST]);
                            }
                            return state.emplace_key(agg_method.data, _hash_values[row], row,
                                                     *_agg_arena_pool);
                        } else {
                            return state.emplace_key(agg_method.data, row, *_agg_arena_pool);
                        }
                    }();

                    /// If a new key is inserted, initialize the states of the aggregate functions, and possibly something related to the key.
                    if (emplace_result.is_inserted()) {
                        /// exception-safety - if you can not allocate memory or create states, then destructors will not be called.
                        emplace_result.set_mapped(nullptr);
                        aggregate_data = _pool->add(new PartitionBlocks());
                        emplace_result.set_mapped(aggregate_data);
                        _value_places.push_back(aggregate_data);
                        _num_partition++;
                    } else {
                        aggregate_data = emplace_result.get_mapped();
                    }
                    assert(aggregate_data != nullptr);
                    aggregate_data->add_row_idx(row);
                }
                for (auto place : _value_places) {
                    SCOPED_TIMER(_selector_block_timer);
                    place->append_block_by_selector(input_block, child(0)->row_desc(),
                                                    _has_global_limit, _partition_inner_limit,
                                                    batch_size);
                }
            },
            _partitioned_data->method_variant);
}

Status VPartitionSortNode::sink(RuntimeState* state, vectorized::Block* input_block, bool eos) {
    auto current_rows = input_block->rows();
    if (current_rows > 0) {
        child_input_rows = child_input_rows + current_rows;
        if (UNLIKELY(_partition_exprs_num == 0)) {
            if (UNLIKELY(_value_places.empty())) {
                _value_places.push_back(_pool->add(new PartitionBlocks()));
            }
            //no partition key
            _value_places[0]->append_whole_block(input_block, child(0)->row_desc());
        } else {
            //just simply use partition num to check
            if (_num_partition > config::partition_topn_partition_threshold &&
                child_input_rows < 10000 * _num_partition) {
                {
                    std::lock_guard<std::mutex> lock(_buffer_mutex);
                    _blocks_buffer.push(std::move(*input_block));
                }
            } else {
                RETURN_IF_ERROR(_split_block_by_partition(input_block, state->batch_size()));
                RETURN_IF_CANCELLED(state);
                RETURN_IF_ERROR(
                        state->check_query_state("VPartitionSortNode, while split input block."));
                input_block->clear_column_data();
            }
        }
    }

    if (eos) {
        //seems could free for hashtable
        _agg_arena_pool.reset(nullptr);
        _partitioned_data.reset(nullptr);
        SCOPED_TIMER(_partition_sort_timer);
        for (int i = 0; i < _value_places.size(); ++i) {
            auto sorter = PartitionSorter::create_unique(
                    _vsort_exec_exprs, _limit, 0, _pool, _is_asc_order, _nulls_first,
                    child(0)->row_desc(), state, i == 0 ? _runtime_profile.get() : nullptr,
                    _has_global_limit, _partition_inner_limit, _top_n_algorithm,
                    _previous_row.get());

            DCHECK(child(0)->row_desc().num_materialized_slots() ==
                   _value_places[i]->blocks.back()->columns());
            //get blocks from every partition, and sorter get those data.
            for (const auto& block : _value_places[i]->blocks) {
                RETURN_IF_ERROR(sorter->append_block(block.get()));
            }
            sorter->init_profile(_runtime_profile.get());
            RETURN_IF_ERROR(sorter->prepare_for_read());
            _partition_sorts.push_back(std::move(sorter));
        }
        if (state->enable_profile()) {
            debug_profile();
        }
        COUNTER_SET(_hash_table_size_counter, int64_t(_num_partition));
        //so all data from child have sink completed
        _can_read = true;
    }
    return Status::OK();
}

Status VPartitionSortNode::open(RuntimeState* state) {
    VLOG_CRITICAL << "VPartitionSortNode::open";
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_ERROR(child(0)->open(state));

    bool eos = false;
    std::unique_ptr<Block> input_block = Block::create_unique();
    do {
        RETURN_IF_ERROR(child(0)->get_next_after_projects(
                state, input_block.get(), &eos,
                std::bind((Status(ExecNode::*)(RuntimeState*, vectorized::Block*, bool*)) &
                                  ExecNode::get_next,
                          _children[0], std::placeholders::_1, std::placeholders::_2,
                          std::placeholders::_3)));
        RETURN_IF_ERROR(sink(state, input_block.get(), eos));
    } while (!eos);

    child(0)->close(state);

    return Status::OK();
}

Status VPartitionSortNode::alloc_resource(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::alloc_resource(state));
    RETURN_IF_ERROR(VExpr::open(_partition_expr_ctxs, state));
    RETURN_IF_ERROR(_vsort_exec_exprs.open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->check_query_state("VPartitionSortNode, while open."));
    return Status::OK();
}

bool VPartitionSortNode::can_read() {
    std::lock_guard<std::mutex> lock(_buffer_mutex);
    return !_blocks_buffer.empty() || _can_read;
}

Status VPartitionSortNode::pull(doris::RuntimeState* state, vectorized::Block* output_block,
                                bool* eos) {
    RETURN_IF_CANCELLED(state);
    output_block->clear_column_data();
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        if (_blocks_buffer.empty() == false) {
            _blocks_buffer.front().swap(*output_block);
            _blocks_buffer.pop();
            return Status::OK();
        }
    }

    if (_can_read) {
        bool current_eos = false;
        RETURN_IF_ERROR(get_sorted_block(state, output_block, &current_eos));
    }
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        if (_blocks_buffer.empty() && _sort_idx >= _partition_sorts.size()) {
            _can_read = false;
            *eos = true;
        }
    }
    return Status::OK();
}

Status VPartitionSortNode::get_next(RuntimeState* state, Block* output_block, bool* eos) {
    if (state == nullptr || output_block == nullptr || eos == nullptr) {
        return Status::InternalError("input is nullptr");
    }
    VLOG_CRITICAL << "VPartitionSortNode::get_next";
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return pull(state, output_block, eos);
}

Status VPartitionSortNode::get_sorted_block(RuntimeState* state, Block* output_block,
                                            bool* current_eos) {
    SCOPED_TIMER(_get_sorted_timer);
    //sorter output data one by one
    if (_sort_idx < _partition_sorts.size()) {
        RETURN_IF_ERROR(_partition_sorts[_sort_idx]->get_next(state, output_block, current_eos));
    }
    if (*current_eos) {
        //current sort have eos, so get next idx
        _previous_row->reset();
        auto rows = _partition_sorts[_sort_idx]->get_output_rows();
        partition_profile_output_rows.push_back(rows);
        _num_rows_returned += rows;
        _partition_sorts[_sort_idx].reset(nullptr);
        _sort_idx++;
    }

    return Status::OK();
}

Status VPartitionSortNode::close(RuntimeState* state) {
    VLOG_CRITICAL << "VPartitionSortNode::close";
    if (is_closed()) {
        return Status::OK();
    }
    if (state->enable_profile()) {
        debug_profile();
    }
    return ExecNode::close(state);
}

void VPartitionSortNode::release_resource(RuntimeState* state) {
    _vsort_exec_exprs.close(state);
    ExecNode::release_resource(state);
}

void VPartitionSortNode::_init_hash_method() {
    if (_partition_exprs_num == 0) {
        return;
    } else if (_partition_exprs_num == 1) {
        auto is_nullable = _partition_expr_ctxs[0]->root()->is_nullable();
        switch (_partition_expr_ctxs[0]->root()->result_type()) {
        case TYPE_TINYINT:
        case TYPE_BOOLEAN:
            _partitioned_data->init(PartitionedHashMapVariants::Type::int8_key, is_nullable);
            return;
        case TYPE_SMALLINT:
            _partitioned_data->init(PartitionedHashMapVariants::Type::int16_key, is_nullable);
            return;
        case TYPE_INT:
        case TYPE_FLOAT:
        case TYPE_DATEV2:
            _partitioned_data->init(PartitionedHashMapVariants::Type::int32_key, is_nullable);
            return;
        case TYPE_BIGINT:
        case TYPE_DOUBLE:
        case TYPE_DATE:
        case TYPE_DATETIME:
        case TYPE_DATETIMEV2:
            _partitioned_data->init(PartitionedHashMapVariants::Type::int64_key, is_nullable);
            return;
        case TYPE_LARGEINT: {
            _partitioned_data->init(PartitionedHashMapVariants::Type::int128_key, is_nullable);
            return;
        }
        case TYPE_DECIMALV2:
        case TYPE_DECIMAL32:
        case TYPE_DECIMAL64:
        case TYPE_DECIMAL128I: {
            DataTypePtr& type_ptr = _partition_expr_ctxs[0]->root()->data_type();
            TypeIndex idx = is_nullable ? assert_cast<const DataTypeNullable&>(*type_ptr)
                                                  .get_nested_type()
                                                  ->get_type_id()
                                        : type_ptr->get_type_id();
            WhichDataType which(idx);
            if (which.is_decimal32()) {
                _partitioned_data->init(PartitionedHashMapVariants::Type::int32_key, is_nullable);
            } else if (which.is_decimal64()) {
                _partitioned_data->init(PartitionedHashMapVariants::Type::int64_key, is_nullable);
            } else {
                _partitioned_data->init(PartitionedHashMapVariants::Type::int128_key, is_nullable);
            }
            return;
        }
        case TYPE_CHAR:
        case TYPE_VARCHAR:
        case TYPE_STRING: {
            _partitioned_data->init(PartitionedHashMapVariants::Type::string_key, is_nullable);
            break;
        }
        default:
            _partitioned_data->init(PartitionedHashMapVariants::Type::serialized);
        }
    } else {
        bool use_fixed_key = true;
        bool has_null = false;
        size_t key_byte_size = 0;
        size_t bitmap_size = get_bitmap_size(_partition_exprs_num);

        _partition_key_sz.resize(_partition_exprs_num);
        for (int i = 0; i < _partition_exprs_num; ++i) {
            const auto& data_type = _partition_expr_ctxs[i]->root()->data_type();

            if (!data_type->have_maximum_size_of_value()) {
                use_fixed_key = false;
                break;
            }

            auto is_null = data_type->is_nullable();
            has_null |= is_null;
            _partition_key_sz[i] =
                    data_type->get_maximum_size_of_value_in_memory() - (is_null ? 1 : 0);
            key_byte_size += _partition_key_sz[i];
        }

        if (bitmap_size + key_byte_size > sizeof(UInt256)) {
            use_fixed_key = false;
        }

        if (use_fixed_key) {
            if (has_null) {
                if (bitmap_size + key_byte_size <= sizeof(UInt64)) {
                    _partitioned_data->init(PartitionedHashMapVariants::Type::int64_keys, has_null);
                } else if (bitmap_size + key_byte_size <= sizeof(UInt128)) {
                    _partitioned_data->init(PartitionedHashMapVariants::Type::int128_keys,
                                            has_null);
                } else {
                    _partitioned_data->init(PartitionedHashMapVariants::Type::int256_keys,
                                            has_null);
                }
            } else {
                if (key_byte_size <= sizeof(UInt64)) {
                    _partitioned_data->init(PartitionedHashMapVariants::Type::int64_keys, has_null);
                } else if (key_byte_size <= sizeof(UInt128)) {
                    _partitioned_data->init(PartitionedHashMapVariants::Type::int128_keys,
                                            has_null);
                } else {
                    _partitioned_data->init(PartitionedHashMapVariants::Type::int256_keys,
                                            has_null);
                }
            }
        } else {
            _partitioned_data->init(PartitionedHashMapVariants::Type::serialized);
        }
    }
}

void VPartitionSortNode::debug_profile() {
    fmt::memory_buffer partition_rows_read, partition_blocks_read;
    fmt::format_to(partition_rows_read, "[");
    fmt::format_to(partition_blocks_read, "[");
    for (auto place : _value_places) {
        fmt::format_to(partition_rows_read, "{}, ", place->get_total_rows());
        fmt::format_to(partition_blocks_read, "{}, ", place->blocks.size());
    }
    fmt::format_to(partition_rows_read, "]");
    fmt::format_to(partition_blocks_read, "]");

    runtime_profile()->add_info_string("PerPartitionBlocksRead",
                                       fmt::to_string(partition_blocks_read));
    runtime_profile()->add_info_string("PerPartitionRowsRead", fmt::to_string(partition_rows_read));
    fmt::memory_buffer partition_output_rows;
    fmt::format_to(partition_output_rows, "[");
    for (auto row : partition_profile_output_rows) {
        fmt::format_to(partition_output_rows, "{}, ", row);
    }
    fmt::format_to(partition_output_rows, "]");
    runtime_profile()->add_info_string("PerPartitionOutputRows",
                                       fmt::to_string(partition_output_rows));
}

} // namespace doris::vectorized
