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


#include "base_scanner.h"

#include "runtime/descriptors.h"
#include "runtime/mem_tracker.h"
#include "runtime/raw_value.h"
#include "runtime/tuple.h"
#include "runtime/runtime_state.h"

namespace doris {

BaseScanner::BaseScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params, ScannerCounter* counter) :
        _state(state), _params(params), _counter(counter),
        _src_tuple(nullptr),
        _src_tuple_row(nullptr),
#if BE_TEST
        _mem_tracker(new MemTracker()),
        _mem_pool(_mem_tracker.get()),
#else
        _mem_tracker(new MemTracker(-1, "Broker Scanner", state->instance_mem_tracker())),
        _mem_pool(_state->instance_mem_tracker()),
#endif
        _dest_tuple_desc(nullptr),
        _strict_mode(false),
        _profile(profile),
        _rows_read_counter(nullptr),
        _read_timer(nullptr),
        _materialize_timer(nullptr) {
}

Status BaseScanner::open() {
    RETURN_IF_ERROR(init_expr_ctxes());
    if (_params.__isset.strict_mode) {
        _strict_mode = _params.strict_mode;
    }
    if (_strict_mode && !_params.__isset.dest_sid_to_src_sid_without_trans) {
        return Status::InternalError("Slot map of dest to src must be set in strict mode");
    }
    _rows_read_counter = ADD_COUNTER(_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_profile, "MaterializeTupleTime(*)");
    return Status::OK();
}

Status BaseScanner::init_expr_ctxes() {
    // Constcut _src_slot_descs
    const TupleDescriptor* src_tuple_desc =
        _state->desc_tbl().get_tuple_descriptor(_params.src_tuple_id);
    if (src_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown source tuple descriptor, tuple_id=" << _params.src_tuple_id;
        return Status::InternalError(ss.str());
    }

    std::map<SlotId, SlotDescriptor*> src_slot_desc_map;
    for (auto slot_desc : src_tuple_desc->slots()) {
        src_slot_desc_map.emplace(slot_desc->id(), slot_desc);
    }
    for (auto slot_id : _params.src_slot_ids) {
        auto it = src_slot_desc_map.find(slot_id);
        if (it == std::end(src_slot_desc_map)) {
            std::stringstream ss;
            ss << "Unknown source slot descriptor, slot_id=" << slot_id;
            return Status::InternalError(ss.str());
        }
        _src_slot_descs.emplace_back(it->second);
    }
    // Construct source tuple and tuple row
    _src_tuple = (Tuple*) _mem_pool.allocate(src_tuple_desc->byte_size());
    _src_tuple_row = (TupleRow*) _mem_pool.allocate(sizeof(Tuple*));
    _src_tuple_row->set_tuple(0, _src_tuple);
    _row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                      std::vector<TupleId>({_params.src_tuple_id}),
                                      std::vector<bool>({false})));

    // Construct dest slots information
    _dest_tuple_desc = _state->desc_tbl().get_tuple_descriptor(_params.dest_tuple_id);
    if (_dest_tuple_desc == nullptr) {
        std::stringstream ss;
        ss << "Unknown dest tuple descriptor, tuple_id=" << _params.dest_tuple_id;
        return Status::InternalError(ss.str());
    }

    bool has_slot_id_map = _params.__isset.dest_sid_to_src_sid_without_trans;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto it = _params.expr_of_dest_slot.find(slot_desc->id());
        if (it == std::end(_params.expr_of_dest_slot)) {
            std::stringstream ss;
            ss << "No expr for dest slot, id=" << slot_desc->id()
                << ", name=" << slot_desc->col_name();
            return Status::InternalError(ss.str());
        }
        ExprContext* ctx = nullptr;
        RETURN_IF_ERROR(Expr::create_expr_tree(_state->obj_pool(), it->second, &ctx));
        RETURN_IF_ERROR(ctx->prepare(_state, *_row_desc.get(), _mem_tracker.get()));
        RETURN_IF_ERROR(ctx->open(_state));
        _dest_expr_ctx.emplace_back(ctx);
        if (has_slot_id_map) {
            auto it = _params.dest_sid_to_src_sid_without_trans.find(slot_desc->id());
            if (it == std::end(_params.dest_sid_to_src_sid_without_trans)) {
                _src_slot_descs_order_by_dest.emplace_back(nullptr);
            } else {
                auto _src_slot_it = src_slot_desc_map.find(it->second);
                if (_src_slot_it == std::end(src_slot_desc_map)) {
                     std::stringstream ss;
                     ss << "No src slot " << it->second << " in src slot descs";
                     return Status::InternalError(ss.str());
                }
                _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
            }
        }
    }
    return Status::OK();
}

bool BaseScanner::fill_dest_tuple(const Slice& line, Tuple* dest_tuple, MemPool* mem_pool) {
    int ctx_idx = 0;
    for (auto slot_desc : _dest_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }

        int dest_index = ctx_idx++;
        ExprContext* ctx = _dest_expr_ctx[dest_index];
        void* value = ctx->get_value(_src_tuple_row);
        if (value == nullptr) {
            if (_strict_mode && (_src_slot_descs_order_by_dest[dest_index] != nullptr)
                && !_src_tuple->is_null(_src_slot_descs_order_by_dest[dest_index]->null_indicator_offset())) {
                std::stringstream error_msg;
                error_msg << "column(" << slot_desc->col_name() << ") value is incorrect "
                    << "while strict mode is " << std::boolalpha << _strict_mode;
                _state->append_error_msg_to_file("", error_msg.str());
                _counter->num_rows_filtered++;
                return false;
            }
            if (!slot_desc->is_nullable()) {
                std::stringstream error_msg;
                error_msg << "column(" << slot_desc->col_name() << ") value is null "
                          << "while columns is not nullable";
                _state->append_error_msg_to_file("", error_msg.str());
                _counter->num_rows_filtered++;
                return false;
            }
            dest_tuple->set_null(slot_desc->null_indicator_offset());
            continue;
        }
        dest_tuple->set_not_null(slot_desc->null_indicator_offset());
        void* slot = dest_tuple->get_slot(slot_desc->tuple_offset());
        RawValue::write(value, slot, slot_desc->type(), mem_pool);
        continue;
    }
    return true;
}
}
