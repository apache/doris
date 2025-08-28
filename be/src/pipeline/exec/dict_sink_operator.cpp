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

#include "dict_sink_operator.h"

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/functions/complex_hash_map_dictionary.h"
#include "vec/functions/dictionary_factory.h"
#include "vec/functions/dictionary_util.h"
#include "vec/functions/ip_address_dictionary.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"

Status DictSinkLocalState::init(RuntimeState* state, LocalSinkStateInfo& info) {
    RETURN_IF_ERROR(Base::init(state, info));
    auto& p = _parent->cast<DictSinkOperatorX>();
    _output_vexpr_ctxs.resize(p._output_vexpr_ctxs.size());
    for (size_t i = 0; i < _output_vexpr_ctxs.size(); i++) {
        RETURN_IF_ERROR(p._output_vexpr_ctxs[i]->clone(state, _output_vexpr_ctxs[i]));
    }
    return Status::OK();
}

Status DictSinkLocalState::load_dict(RuntimeState* state) {
    const auto& p = _parent->cast<DictSinkOperatorX>();

    // now key_output_expr_slots size only 1
    auto input_block = _dict_input_block.to_block();

    for (auto& data : input_block) {
        data.column = std::move(*data.column).mutate()->convert_column_if_overflow();
    }

    vectorized::ColumnsWithTypeAndName key_data;

    vectorized::ColumnsWithTypeAndName value_data;

    for (long key_expr_id : p._key_output_expr_slots) {
        auto key_expr_ctx = _output_vexpr_ctxs[key_expr_id];
        int key_column_id = -1;
        RETURN_IF_ERROR(key_expr_ctx->execute(&input_block, &key_column_id));
        key_data.push_back(input_block.get_by_position(key_column_id));
    }

    for (size_t i = 0; i < p._value_output_expr_slots.size(); i++) {
        auto value_expr_id = p._value_output_expr_slots[i];
        auto value_name = p._value_names[i];
        auto value_expr_ctx = _output_vexpr_ctxs[value_expr_id];
        int value_column_id = -1;
        RETURN_IF_ERROR(value_expr_ctx->execute(&input_block, &value_column_id));
        auto att_data = input_block.get_by_position(value_column_id);
        att_data.name = value_name;
        value_data.push_back(att_data);
    }

    RETURN_IF_ERROR(check_dict_input_data(key_data, value_data, p._skip_null_key));
    const auto& dict_name = p._dictionary_name;

    vectorized::DictionaryPtr dict = nullptr;

    switch (p._layout_type) {
    case TDictLayoutType::type::IP_TRIE: {
        if (key_data.size() != 1) {
            return Status::InvalidArgument("IP_TRIE dict key size must be 1");
        }
        dict = create_ip_trie_dict_from_column(dict_name, key_data[0], value_data);
        break;
    }
    case TDictLayoutType::type::HASH_MAP: {
        dict = create_complex_hash_map_dict_from_column(dict_name, key_data, value_data);
        break;
    }
    default:
        return Status::InvalidArgument("Unknown layout type");
    }
    if (dict == nullptr) {
        return Status::InternalError("Failed to create dictionary");
    }

    if (dict->allocated_bytes() > p._memory_limit) {
        return Status::InvalidArgument(
                "load dict memory limit exceeded , current memory usage: {} , memory limit: {}",
                dict->allocated_bytes(), p._memory_limit);
    }

    LOG(INFO) << fmt::format("Refresh dictionary {}, version: {}", p._dictionary_id, p._version_id);
    RETURN_IF_ERROR(ExecEnv::GetInstance()->dict_factory()->refresh_dict(p._dictionary_id,
                                                                         p._version_id, dict));
    return Status::OK();
}

DictSinkOperatorX::DictSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                                     const std::vector<TExpr>& dict_input_expr,
                                     const TDictionarySink& dict_sink)
        : Base(operator_id, 0, 0),
          _dictionary_id(dict_sink.dictionary_id),
          _version_id(dict_sink.version_id),
          _dictionary_name(dict_sink.dictionary_name),
          _layout_type(dict_sink.layout_type),
          _key_output_expr_slots(dict_sink.key_output_expr_slots),
          _value_output_expr_slots(dict_sink.value_output_expr_slots),
          _value_names(dict_sink.value_names),
          _row_desc(row_desc),
          _t_output_expr(dict_input_expr),
          _skip_null_key(dict_sink.skip_null_key),
          _memory_limit(dict_sink.memory_limit) {}

Status DictSinkOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));
    if (_value_output_expr_slots.size() != _value_names.size()) {
        return Status::InternalError("value_output_expr_slots.size() != value_names.size()");
    }
    if (_child->parallel_tasks() != 1) {
        return Status::InternalError("DictSinkOperatorX parallel must be 1");
    }
    // prepare output_expr
    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(vectorized::VExpr::create_expr_trees(_t_output_expr, _output_vexpr_ctxs));
    // Prepare the exprs to run.
    RETURN_IF_ERROR(vectorized::VExpr::prepare(_output_vexpr_ctxs, state, _row_desc));
    RETURN_IF_ERROR(vectorized::VExpr::open(_output_vexpr_ctxs, state));

    for (auto key_expr_id : _key_output_expr_slots) {
        auto key_expr = _output_vexpr_ctxs[key_expr_id]->root();
        if (!key_expr->is_slot_ref()) {
            return Status::InvalidArgument(
                    "DictSinkOperatorX expr must be slot ref , but now is {}",
                    key_expr->expr_name());
        }
    }

    for (auto value_expr_id : _value_output_expr_slots) {
        auto value_expr = _output_vexpr_ctxs[value_expr_id]->root();
        if (!value_expr->is_slot_ref()) {
            return Status::InvalidArgument(
                    "DictSinkOperatorX expr must be slot ref , but now is {}",
                    value_expr->expr_name());
        }
    }

    return Status::OK();
}

Status DictSinkOperatorX::sink(RuntimeState* state, vectorized::Block* in_block, bool eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    COUNTER_UPDATE(local_state.rows_input_counter(), (int64_t)in_block->rows());

    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(ExecEnv::GetInstance()->dict_factory()->mem_tracker());

    if (local_state._dict_input_block.columns() == 0) {
        local_state._dict_input_block =
                vectorized::Block(vectorized::VectorizedUtils::create_empty_block(_row_desc));
    }

    if (in_block->rows() != 0) {
        RETURN_IF_ERROR(local_state._dict_input_block.merge_ignore_overflow(std::move(*in_block)));
    }

    if (eos) {
        RETURN_IF_ERROR(local_state.load_dict(state));
    }

    return Status::OK();
}

} // namespace doris::pipeline
#include "common/compile_check_end.h"
