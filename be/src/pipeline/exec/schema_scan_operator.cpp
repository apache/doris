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

#include "schema_scan_operator.h"

#include <gen_cpp/FrontendService_types.h>

#include <memory>

#include "pipeline/exec/operator.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type_factory.hpp"

namespace doris {
class RuntimeState;
} // namespace doris

namespace doris::pipeline {

Status SchemaScanLocalState::init(RuntimeState* state, LocalStateInfo& info) {
    RETURN_IF_ERROR(PipelineXLocalState<>::init(state, info));

    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_init_timer);
    auto& p = _parent->cast<SchemaScanOperatorX>();
    _scanner_param.common_param = p._common_scanner_param;
    // init schema scanner profile
    _scanner_param.profile = std::make_unique<RuntimeProfile>("SchemaScanner");
    profile()->add_child(_scanner_param.profile.get(), true, nullptr);

    // get src tuple desc
    const auto* schema_table =
            static_cast<const SchemaTableDescriptor*>(p._dest_tuple_desc->table_desc());
    // new one scanner
    _schema_scanner = SchemaScanner::create(schema_table->schema_table_type());

    _schema_scanner->set_dependency(_data_dependency, _finish_dependency);
    if (nullptr == _schema_scanner) {
        return Status::InternalError("schema scanner get nullptr pointer.");
    }

    return _schema_scanner->init(&_scanner_param, state->obj_pool());
}

Status SchemaScanLocalState::open(RuntimeState* state) {
    SCOPED_TIMER(exec_time_counter());
    SCOPED_TIMER(_open_timer);
    RETURN_IF_ERROR(PipelineXLocalState<>::open(state));
    return _schema_scanner->get_next_block_async(state);
}

SchemaScanOperatorX::SchemaScanOperatorX(ObjectPool* pool, const TPlanNode& tnode, int operator_id,
                                         const DescriptorTbl& descs)
        : Base(pool, tnode, operator_id, descs),
          _table_name(tnode.schema_scan_node.table_name),
          _common_scanner_param(new SchemaScannerCommonParam()),
          _tuple_id(tnode.schema_scan_node.tuple_id),
          _tuple_idx(0),
          _slot_num(0) {}

Status SchemaScanOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(Base::init(tnode, state));

    if (tnode.schema_scan_node.__isset.db) {
        _common_scanner_param->db =
                state->obj_pool()->add(new std::string(tnode.schema_scan_node.db));
    }

    if (tnode.schema_scan_node.__isset.table) {
        _common_scanner_param->table =
                state->obj_pool()->add(new std::string(tnode.schema_scan_node.table));
    }

    if (tnode.schema_scan_node.__isset.wild) {
        _common_scanner_param->wild =
                state->obj_pool()->add(new std::string(tnode.schema_scan_node.wild));
    }

    if (tnode.schema_scan_node.__isset.current_user_ident) {
        _common_scanner_param->current_user_ident = state->obj_pool()->add(
                new TUserIdentity(tnode.schema_scan_node.current_user_ident));
    } else {
        if (tnode.schema_scan_node.__isset.user) {
            _common_scanner_param->user =
                    state->obj_pool()->add(new std::string(tnode.schema_scan_node.user));
        }
        if (tnode.schema_scan_node.__isset.user_ip) {
            _common_scanner_param->user_ip =
                    state->obj_pool()->add(new std::string(tnode.schema_scan_node.user_ip));
        }
    }

    if (tnode.schema_scan_node.__isset.ip) {
        _common_scanner_param->ip =
                state->obj_pool()->add(new std::string(tnode.schema_scan_node.ip));
    }
    if (tnode.schema_scan_node.__isset.port) {
        _common_scanner_param->port = tnode.schema_scan_node.port;
    }

    if (tnode.schema_scan_node.__isset.thread_id) {
        _common_scanner_param->thread_id = tnode.schema_scan_node.thread_id;
    }

    if (tnode.schema_scan_node.__isset.catalog) {
        _common_scanner_param->catalog =
                state->obj_pool()->add(new std::string(tnode.schema_scan_node.catalog));
    }
    return Status::OK();
}

Status SchemaScanOperatorX::open(RuntimeState* state) {
    RETURN_IF_ERROR(Base::open(state));

    if (_common_scanner_param->user) {
        TSetSessionParams param;
        param.__set_user(*_common_scanner_param->user);
        //TStatus t_status;
        //RETURN_IF_ERROR(SchemaJniHelper::set_session(param, &t_status));
        //RETURN_IF_ERROR(Status(t_status));
    }

    return Status::OK();
}

Status SchemaScanOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(Base::prepare(state));

    // get dest tuple desc
    _dest_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _dest_tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _slot_num = _dest_tuple_desc->slots().size();
    // get src tuple desc
    const auto* schema_table =
            static_cast<const SchemaTableDescriptor*>(_dest_tuple_desc->table_desc());

    if (nullptr == schema_table) {
        return Status::InternalError("Failed to get schema table descriptor.");
    }

    // new one scanner
    _schema_scanner = SchemaScanner::create(schema_table->schema_table_type());

    if (nullptr == _schema_scanner) {
        return Status::InternalError("schema scanner get nullptr pointer.");
    }

    const std::vector<SchemaScanner::ColumnDesc>& columns_desc(_schema_scanner->get_column_desc());

    // if src columns size is zero, it's the dummy slots.
    if (columns_desc.empty()) {
        _slot_num = 0;
    }

    // check if type is ok.
    for (int i = 0; i < _slot_num; ++i) {
        int j = 0;
        for (; j < columns_desc.size(); ++j) {
            if (boost::iequals(_dest_tuple_desc->slots()[i]->col_name(), columns_desc[j].name)) {
                break;
            }
        }

        if (j >= columns_desc.size()) {
            return Status::InternalError("no match column for this column({})",
                                         _dest_tuple_desc->slots()[i]->col_name());
        }

        if (columns_desc[j].type != _dest_tuple_desc->slots()[i]->type().type) {
            return Status::InternalError("schema not match. input is {}({}) and output is {}({})",
                                         columns_desc[j].name, type_to_string(columns_desc[j].type),
                                         _dest_tuple_desc->slots()[i]->col_name(),
                                         type_to_string(_dest_tuple_desc->slots()[i]->type().type));
        }
    }

    _tuple_idx = 0;

    return Status::OK();
}

Status SchemaScanOperatorX::get_block(RuntimeState* state, vectorized::Block* block, bool* eos) {
    auto& local_state = get_local_state(state);
    SCOPED_TIMER(local_state.exec_time_counter());
    RETURN_IF_CANCELLED(state);
    bool schema_eos = false;

    const std::vector<SchemaScanner::ColumnDesc>& columns_desc(
            local_state._schema_scanner->get_column_desc());

    do {
        block->clear();
        for (int i = 0; i < _slot_num; ++i) {
            auto* dest_slot_desc = _dest_tuple_desc->slots()[i];
            block->insert(vectorized::ColumnWithTypeAndName(
                    dest_slot_desc->get_empty_mutable_column(), dest_slot_desc->get_data_type_ptr(),
                    dest_slot_desc->col_name()));
        }

        // src block columns desc is filled by schema_scanner->get_column_desc.
        vectorized::Block src_block;
        for (int i = 0; i < columns_desc.size(); ++i) {
            TypeDescriptor descriptor(columns_desc[i].type);
            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
            src_block.insert(vectorized::ColumnWithTypeAndName(data_type->create_column(),
                                                               data_type, columns_desc[i].name));
        }
        while (true) {
            RETURN_IF_CANCELLED(state);

            if (local_state._data_dependency->is_blocked_by() != nullptr) {
                break;
            }
            // get all slots from schema table.
            RETURN_IF_ERROR(
                    local_state._schema_scanner->get_next_block(state, &src_block, &schema_eos));

            if (schema_eos) {
                *eos = true;
                break;
            }

            if (src_block.rows() >= state->batch_size()) {
                break;
            }
        }

        if (src_block.rows()) {
            // block->check_number_of_rows();
            for (int i = 0; i < _slot_num; ++i) {
                auto* dest_slot_desc = _dest_tuple_desc->slots()[i];
                vectorized::MutableColumnPtr column_ptr =
                        std::move(*block->get_by_position(i).column).mutate();
                column_ptr->insert_range_from(
                        *src_block.get_by_name(dest_slot_desc->col_name()).column, 0,
                        src_block.rows());
            }
            RETURN_IF_ERROR(vectorized::VExprContext::filter_block(
                    local_state._conjuncts, block, _dest_tuple_desc->slots().size()));
            src_block.clear();
        }
    } while (block->rows() == 0 && !*eos);

    local_state.reached_limit(block, eos);
    return Status::OK();
}

} // namespace doris::pipeline
