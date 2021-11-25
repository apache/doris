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

#include "exec/repeat_node.h"

#include "exprs/expr.h"
#include "gutil/strings/join.h"
#include "runtime/raw_value.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"

namespace doris {

RepeatNode::RepeatNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ExecNode(pool, tnode, descs),
          _slot_id_set_list(tnode.repeat_node.slot_id_set_list),
          _all_slot_ids(tnode.repeat_node.all_slot_ids),
          _repeat_id_list(tnode.repeat_node.repeat_id_list),
          _grouping_list(tnode.repeat_node.grouping_list),
          _output_tuple_id(tnode.repeat_node.output_tuple_id),
          _tuple_desc(nullptr),
          _child_row_batch(nullptr),
          _child_eos(false),
          _repeat_id_idx(0),
          _runtime_state(nullptr) {}

RepeatNode::~RepeatNode() {}

Status RepeatNode::prepare(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::prepare(state));
    _runtime_state = state;
    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_output_tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    return Status::OK();
}

Status RepeatNode::open(RuntimeState* state) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(ExecNode::open(state));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(child(0)->open(state));
    return Status::OK();
}

/**
 * copy the rows to new tuple based on repeat_id_idx and _repeat_id_list and fill in row_batch,
 *  and then set grouping_id and other grouping function slot in child_row_batch
 *  e.g. _repeat_id_list = [0, 3, 1, 2], _repeat_id_idx = 2, _grouping_list [[0, 3, 1, 2], [0, 1, 1, 0]],
 *  row_batch tuple 0 ['a', 'b', 1] -> [['a', null, 1] tuple 1 [1, 1]]
 */
Status RepeatNode::get_repeated_batch(RowBatch* child_row_batch, int repeat_id_idx,
                                      RowBatch* row_batch) {
    DCHECK(child_row_batch != nullptr);
    DCHECK_EQ(row_batch->num_rows(), 0);

    // Fill all slots according to child
    MemPool* tuple_pool = row_batch->tuple_data_pool();
    const std::vector<TupleDescriptor*>& src_tuple_descs =
            child_row_batch->row_desc().tuple_descriptors();
    const std::vector<TupleDescriptor*>& dst_tuple_descs =
            row_batch->row_desc().tuple_descriptors();
    std::vector<Tuple*> dst_tuples(src_tuple_descs.size(), nullptr);
    for (int i = 0; i < child_row_batch->num_rows(); ++i) {
        int row_idx = row_batch->add_row();
        TupleRow* dst_row = row_batch->get_row(row_idx);
        TupleRow* src_row = child_row_batch->get_row(i);

        auto src_it = src_tuple_descs.begin();
        auto dst_it = dst_tuple_descs.begin();
        for (int j = 0; src_it != src_tuple_descs.end() && dst_it != dst_tuple_descs.end();
             ++src_it, ++dst_it, ++j) {
            Tuple* src_tuple = src_row->get_tuple(j);
            if (src_tuple == nullptr) {
                dst_row->set_tuple(j, nullptr);
                continue;
            }

            if (dst_tuples[j] == nullptr) {
                int size = row_batch->capacity() * (*dst_it)->byte_size();
                void* tuple_buffer = tuple_pool->allocate(size);
                if (tuple_buffer == nullptr) {
                    return Status::InternalError("Allocate memory for row batch failed.");
                }
                dst_tuples[j] = reinterpret_cast<Tuple*>(tuple_buffer);
            } else {
                char* new_tuple = reinterpret_cast<char*>(dst_tuples[j]);
                new_tuple += (*dst_it)->byte_size();
                dst_tuples[j] = reinterpret_cast<Tuple*>(new_tuple);
            }
            dst_row->set_tuple(j, dst_tuples[j]);
            memset(dst_tuples[j], 0, (*dst_it)->num_null_bytes());
            src_tuple->deep_copy(dst_tuples[j], **dst_it, tuple_pool);
            for (int k = 0; k < (*src_it)->slots().size(); k++) {
                SlotDescriptor* src_slot_desc = (*src_it)->slots()[k];
                SlotDescriptor* dst_slot_desc = (*dst_it)->slots()[k];
                DCHECK_EQ(src_slot_desc->type().type, dst_slot_desc->type().type);
                DCHECK_EQ(src_slot_desc->col_name(), dst_slot_desc->col_name());
                // set null base on repeated list
                if (_all_slot_ids.find(src_slot_desc->id()) != _all_slot_ids.end()) {
                    std::set<SlotId>& repeat_ids = _slot_id_set_list[repeat_id_idx];
                    if (repeat_ids.find(src_slot_desc->id()) == repeat_ids.end()) {
                        dst_tuples[j]->set_null(dst_slot_desc->null_indicator_offset());
                        continue;
                    }
                }
            }
        }
        row_batch->commit_last_row();
    }
    Tuple* tuple = nullptr;
    // Fill grouping ID to tuple
    for (int i = 0; i < child_row_batch->num_rows(); ++i) {
        int row_idx = i;
        TupleRow* row = row_batch->get_row(row_idx);

        if (tuple == nullptr) {
            int size = row_batch->capacity() * _tuple_desc->byte_size();
            void* tuple_buffer = tuple_pool->allocate(size);
            if (tuple_buffer == nullptr) {
                return Status::InternalError("Allocate memory for row batch failed.");
            }
            tuple = reinterpret_cast<Tuple*>(tuple_buffer);
        } else {
            char* new_tuple = reinterpret_cast<char*>(tuple);
            new_tuple += _tuple_desc->byte_size();
            tuple = reinterpret_cast<Tuple*>(new_tuple);
        }

        row->set_tuple(src_tuple_descs.size(), tuple);
        memset(tuple, 0, _tuple_desc->num_null_bytes());

        for (size_t slot_idx = 0; slot_idx < _grouping_list.size(); ++slot_idx) {
            int64_t val = _grouping_list[slot_idx][repeat_id_idx];
            DCHECK_LT(slot_idx, _tuple_desc->slots().size())
                    << "TupleDescriptor: " << _tuple_desc->debug_string();
            const SlotDescriptor* slot_desc = _tuple_desc->slots()[slot_idx];
            tuple->set_not_null(slot_desc->null_indicator_offset());
            RawValue::write(&val, tuple, slot_desc, tuple_pool);
        }
    }

    return Status::OK();
}

Status RepeatNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    DCHECK(_repeat_id_idx >= 0);
    for (const std::vector<int64_t>& v : _grouping_list) {
        DCHECK(_repeat_id_idx <= (int)v.size());
    }
    // current child has finished its repeat, get child's next batch
    if (_child_row_batch.get() == nullptr) {
        if (_child_eos) {
            *eos = true;
            return Status::OK();
        }

        _child_row_batch.reset(
                new RowBatch(child(0)->row_desc(), state->batch_size(), mem_tracker().get()));
        RETURN_IF_ERROR(child(0)->get_next(state, _child_row_batch.get(), &_child_eos));

        if (_child_row_batch->num_rows() <= 0) {
            _child_row_batch.reset(nullptr);
            *eos = true;
            return Status::OK();
        }
    }

    DCHECK_EQ(row_batch->num_rows(), 0);
    RETURN_IF_ERROR(get_repeated_batch(_child_row_batch.get(), _repeat_id_idx, row_batch));
    _repeat_id_idx++;

    int size = _repeat_id_list.size();
    if (_repeat_id_idx >= size) {
        _child_row_batch.reset(nullptr);
        _repeat_id_idx = 0;
    }

    return Status::OK();
}

Status RepeatNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    _child_row_batch.reset(nullptr);
    RETURN_IF_ERROR(child(0)->close(state));
    return ExecNode::close(state);
}

void RepeatNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "RepeatNode(";
    *out << "repeat pattern: [" << JoinElements(_repeat_id_list, ",") << "]\n";
    *out << "add " << _grouping_list.size() << " columns. \n";
    *out << "added column values: ";
    for (const std::vector<int64_t>& v : _grouping_list) {
        *out << "[" << JoinElements(v, ",") << "] ";
    }
    *out << "\n";
    ExecNode::debug_string(indentation_level, out);
    *out << ")";
}

} // namespace doris