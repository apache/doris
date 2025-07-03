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

#include <gen_cpp/DataSinks_types.h>

#include <cstdint>

#include "operator.h"

namespace doris::pipeline {
#include "common/compile_check_begin.h"
class DictSinkLocalState final : public PipelineXSinkLocalState<BasicSharedState> {
    ENABLE_FACTORY_CREATOR(DictSinkLocalState);
    using Base = PipelineXSinkLocalState<BasicSharedState>;

public:
    DictSinkLocalState(DataSinkOperatorXBase* parent, RuntimeState* state) : Base(parent, state) {}

    Status init(RuntimeState* state, LocalSinkStateInfo& info) override;

private:
    Status load_dict(RuntimeState* state);
    friend class DictSinkOperatorX;

    vectorized::MutableBlock _dict_input_block;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;
};

class DictSinkOperatorX final : public DataSinkOperatorX<DictSinkLocalState> {
public:
    using Base = DataSinkOperatorX<DictSinkLocalState>;
    DictSinkOperatorX(int operator_id, const RowDescriptor& row_desc,
                      const std::vector<TExpr>& dict_input_expr, const TDictionarySink& dict_sink);
    Status prepare(RuntimeState* state) override;

    Status sink(RuntimeState* state, vectorized::Block* in_block, bool eos) override;

private:
    friend class DictSinkLocalState;
    // ID of the dictionary, used to distinguish dictionaries
    const int64_t _dictionary_id;

    // Version ID of the dictionary, used to ensure each dictionary is the latest version when used
    const int64_t _version_id;

    // Name of the dictionary
    const std::string _dictionary_name;

    // Layout type of the dictionary, currently supports HashMap and IPtrie
    const TDictLayoutType::type _layout_type;

    // Slots for key output expressions, positions in _t_output_expr
    const std::vector<int64_t> _key_output_expr_slots;

    // Slots for value output expressions, positions in _t_output_expr
    const std::vector<int64_t> _value_output_expr_slots;

    // Names of the values, corresponding one-to-one with _value_output_expr_slots
    const std::vector<std::string> _value_names;

    // Owned by the RuntimeState.
    const RowDescriptor& _row_desc;

    // Owned by the RuntimeState.
    const std::vector<TExpr>& _t_output_expr;

    vectorized::VExprContextSPtrs _output_vexpr_ctxs;

    // If true, we will skip the row containing the null key, if false, directly report an error
    const bool _skip_null_key;

    const int64_t _memory_limit;
};

} // namespace doris::pipeline
#include "common/compile_check_end.h"
