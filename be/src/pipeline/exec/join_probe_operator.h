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

#include "operator.h"
#include "pipeline/pipeline_x/dependency.h"
#include "vec/exec/join/vjoin_node_base.h"

namespace doris {

namespace pipeline {

class JoinProbeOperatorX;
template <typename DependencyType>
class JoinProbeLocalState : public PipelineXLocalState<DependencyType> {
public:
    ENABLE_FACTORY_CREATOR(JoinProbeLocalState);
    JoinProbeLocalState(RuntimeState* state, OperatorXBase* parent)
            : PipelineXLocalState<DependencyType>(state, parent) {}
    virtual ~JoinProbeLocalState() = default;

    virtual Status init(RuntimeState* state, LocalStateInfo& info) override {
        RETURN_IF_ERROR(PipelineXLocalState<DependencyType>::init(state, info));
        auto& p = PipelineXLocalState<DependencyType>::_parent->template cast<JoinProbeOperatorX>();
        // only use in outer join as the bool column to mark for function of `tuple_is_null`
        if (p._is_outer_join) {
            _tuple_is_null_left_flag_column = vectorized::ColumnUInt8::create();
            _tuple_is_null_right_flag_column = vectorized::ColumnUInt8::create();
        }
        _output_expr_ctxs.resize(p._output_expr_ctxs.size());
        for (size_t i = 0; i < _output_expr_ctxs.size(); i++) {
            RETURN_IF_ERROR(p._output_expr_ctxs[i]->clone(state, _output_expr_ctxs[i]));
        }

        _probe_phase_profile = PipelineXLocalState<DependencyType>::profile()->create_child(
                "ProbePhase", true, true);
        _probe_timer = ADD_TIMER(_probe_phase_profile, "ProbeTime");
        _join_filter_timer = ADD_CHILD_TIMER(_probe_phase_profile, "JoinFilterTimer", "ProbeTime");
        _build_output_block_timer =
                ADD_CHILD_TIMER(_probe_phase_profile, "BuildOutputBlock", "ProbeTime");
        _probe_rows_counter = ADD_COUNTER(_probe_phase_profile, "ProbeRows", TUnit::UNIT);

        return Status::OK();
    }
    virtual Status close(RuntimeState* state) override {
        if (PipelineXLocalState<DependencyType>::_closed) {
            return Status::OK();
        }
        _join_block.clear();
        return PipelineXLocalState<DependencyType>::close(state);
    }

protected:
    void _construct_mutable_join_block() {
        auto& p = PipelineXLocalState<DependencyType>::_parent->template cast<JoinProbeOperatorX>();
        const auto& mutable_block_desc = p._intermediate_row_desc;
        for (const auto tuple_desc : mutable_block_desc->tuple_descriptors()) {
            for (const auto slot_desc : tuple_desc->slots()) {
                auto type_ptr = slot_desc->get_data_type_ptr();
                _join_block.insert({type_ptr->create_column(), type_ptr, slot_desc->col_name()});
            }
        }
        if (p._is_mark_join) {
            _join_block.replace_by_position(
                    _join_block.columns() - 1,
                    remove_nullable(_join_block.get_by_position(_join_block.columns() - 1).column));
        }
    }
    Status _build_output_block(vectorized::Block* origin_block, vectorized::Block* output_block,
                               bool keep_origin) {
        auto& p = PipelineXLocalState<DependencyType>::_parent->template cast<JoinProbeOperatorX>();
        SCOPED_TIMER(_build_output_block_timer);
        auto is_mem_reuse = output_block->mem_reuse();
        vectorized::MutableBlock mutable_block =
                is_mem_reuse
                        ? vectorized::MutableBlock(output_block)
                        : vectorized::MutableBlock(
                                  vectorized::VectorizedUtils::create_empty_columnswithtypename(
                                          p.row_desc()));
        auto rows = origin_block->rows();
        // TODO: After FE plan support same nullable of output expr and origin block and mutable column
        // we should replace `insert_column_datas` by `insert_range_from`

        auto insert_column_datas = [keep_origin](auto& to, vectorized::ColumnPtr& from,
                                                 size_t rows) {
            if (to->is_nullable() && !from->is_nullable()) {
                if (keep_origin || !from->is_exclusive()) {
                    auto& null_column = reinterpret_cast<vectorized::ColumnNullable&>(*to);
                    null_column.get_nested_column().insert_range_from(*from, 0, rows);
                    null_column.get_null_map_column().get_data().resize_fill(rows, 0);
                } else {
                    to = make_nullable(from, false)->assume_mutable();
                }
            } else {
                if (keep_origin || !from->is_exclusive()) {
                    to->insert_range_from(*from, 0, rows);
                } else {
                    to = from->assume_mutable();
                }
            }
        };
        if (rows != 0) {
            auto& mutable_columns = mutable_block.mutable_columns();
            if (_output_expr_ctxs.empty()) {
                DCHECK(mutable_columns.size() == p.row_desc().num_materialized_slots());
                for (int i = 0; i < mutable_columns.size(); ++i) {
                    insert_column_datas(mutable_columns[i], origin_block->get_by_position(i).column,
                                        rows);
                }
            } else {
                DCHECK(mutable_columns.size() == p.row_desc().num_materialized_slots());
                SCOPED_TIMER(PipelineXLocalState<DependencyType>::_projection_timer);
                for (int i = 0; i < mutable_columns.size(); ++i) {
                    auto result_column_id = -1;
                    RETURN_IF_ERROR(_output_expr_ctxs[i]->execute(origin_block, &result_column_id));
                    auto& origin_column = origin_block->get_by_position(result_column_id).column;

                    /// `convert_to_full_column_if_const` will create a pointer to the origin column if
                    /// the origin column is not ColumnConst/ColumnArray, this make the column be not
                    /// exclusive.
                    /// TODO: maybe need a method to check if a column need to be converted to full
                    /// column.
                    if (is_column_const(*origin_column) ||
                        check_column<vectorized::ColumnArray>(origin_column)) {
                        auto column_ptr = origin_column->convert_to_full_column_if_const();
                        insert_column_datas(mutable_columns[i], column_ptr, rows);
                    } else {
                        insert_column_datas(mutable_columns[i], origin_column, rows);
                    }
                }
            }

            if (!is_mem_reuse || !keep_origin) {
                output_block->swap(mutable_block.to_block());
            }
            DCHECK(output_block->rows() == rows);
        }

        return Status::OK();
    }
    void _reset_tuple_is_null_column() {
        if (PipelineXLocalState<DependencyType>::_parent->template cast<JoinProbeOperatorX>()
                    ._is_outer_join) {
            reinterpret_cast<vectorized::ColumnUInt8&>(*_tuple_is_null_left_flag_column).clear();
            reinterpret_cast<vectorized::ColumnUInt8&>(*_tuple_is_null_right_flag_column).clear();
        }
    }
    // output expr
    vectorized::VExprContextSPtrs _output_expr_ctxs;
    vectorized::Block _join_block;
    vectorized::MutableColumnPtr _tuple_is_null_left_flag_column;
    vectorized::MutableColumnPtr _tuple_is_null_right_flag_column;

    RuntimeProfile* _probe_phase_profile;
    RuntimeProfile::Counter* _probe_timer;
    RuntimeProfile::Counter* _probe_rows_counter;
    RuntimeProfile::Counter* _join_filter_timer;
    RuntimeProfile::Counter* _build_output_block_timer;
};

class JoinProbeOperatorX : public OperatorXBase {
public:
    JoinProbeOperatorX(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    virtual Status init(const TPlanNode& tnode, RuntimeState* state) override;

    Status open(doris::RuntimeState* state) override;
    [[nodiscard]] const RowDescriptor& row_desc() override { return *_output_row_desc; }

    [[nodiscard]] const RowDescriptor& intermediate_row_desc() const override {
        return *_intermediate_row_desc;
    }

    [[nodiscard]] bool is_source() const override { return false; }

    void set_build_side_child(OperatorXPtr& build_side_child) {
        _build_side_child = build_side_child;
    }

    Status set_child(OperatorXPtr child) override {
        if (_child_x) {
            set_build_side_child(child);
        } else {
            _child_x = std::move(child);
        }
        return Status::OK();
    }

protected:
    template <typename DependencyType>
    friend class JoinProbeLocalState;

    TJoinOp::type _join_op;
    const bool _have_other_join_conjunct;
    const bool _match_all_probe; // output all rows coming from the probe input. Full/Left Join
    const bool _match_all_build; // output all rows coming from the build input. Full/Right Join
    const bool _build_unique;    // build a hash table without duplicated rows. Left semi/anti Join

    const bool _is_right_semi_anti;
    const bool _is_left_semi_anti;
    const bool _is_outer_join;
    const bool _is_mark_join;

    std::unique_ptr<RowDescriptor> _output_row_desc;
    std::unique_ptr<RowDescriptor> _intermediate_row_desc;
    // output expr
    vectorized::VExprContextSPtrs _output_expr_ctxs;
    OperatorXPtr _build_side_child;
};

} // namespace pipeline
} // namespace doris
