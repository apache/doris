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

#include "vec/exec/vschema_scan_node.h"

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <opentelemetry/nostd/shared_ptr.h>

#include <boost/algorithm/string/predicate.hpp>
#include <ostream>
#include <utility>

#include "common/logging.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/exec_node.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "util/telemetry/telemetry.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exprs/vexpr_context.h"

namespace doris {
class TScanRangeParams;
} // namespace doris

namespace doris::vectorized {

VSchemaScanNode::VSchemaScanNode(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.schema_scan_node.table_name),
          _tuple_id(tnode.schema_scan_node.tuple_id),
          _dest_tuple_desc(nullptr),
          _tuple_idx(0),
          _slot_num(0),
          _schema_scanner(nullptr) {}

VSchemaScanNode::~VSchemaScanNode() {}

Status VSchemaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (tnode.schema_scan_node.__isset.db) {
        _scanner_param.common_param->db = _pool->add(new std::string(tnode.schema_scan_node.db));
    }

    if (tnode.schema_scan_node.__isset.table) {
        _scanner_param.common_param->table =
                _pool->add(new std::string(tnode.schema_scan_node.table));
    }

    if (tnode.schema_scan_node.__isset.wild) {
        _scanner_param.common_param->wild =
                _pool->add(new std::string(tnode.schema_scan_node.wild));
    }

    if (tnode.schema_scan_node.__isset.current_user_ident) {
        _scanner_param.common_param->current_user_ident =
                _pool->add(new TUserIdentity(tnode.schema_scan_node.current_user_ident));
    } else {
        if (tnode.schema_scan_node.__isset.user) {
            _scanner_param.common_param->user =
                    _pool->add(new std::string(tnode.schema_scan_node.user));
        }
        if (tnode.schema_scan_node.__isset.user_ip) {
            _scanner_param.common_param->user_ip =
                    _pool->add(new std::string(tnode.schema_scan_node.user_ip));
        }
    }

    if (tnode.schema_scan_node.__isset.ip) {
        _scanner_param.common_param->ip = _pool->add(new std::string(tnode.schema_scan_node.ip));
    }
    if (tnode.schema_scan_node.__isset.port) {
        _scanner_param.common_param->port = tnode.schema_scan_node.port;
    }

    if (tnode.schema_scan_node.__isset.thread_id) {
        _scanner_param.common_param->thread_id = tnode.schema_scan_node.thread_id;
    }

    if (tnode.schema_scan_node.__isset.catalog) {
        _scanner_param.common_param->catalog =
                _pool->add(new std::string(tnode.schema_scan_node.catalog));
    }
    return Status::OK();
}

Status VSchemaScanNode::open(RuntimeState* state) {
    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));

    if (_scanner_param.common_param->user) {
        TSetSessionParams param;
        param.__set_user(*_scanner_param.common_param->user);
        //TStatus t_status;
        //RETURN_IF_ERROR(SchemaJniHelper::set_session(param, &t_status));
        //RETURN_IF_ERROR(Status(t_status));
    }

    return _schema_scanner->start(state);
}

Status VSchemaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("state pointer is nullptr.");
    }
    RETURN_IF_ERROR(ScanNode::prepare(state));

    // get dest tuple desc
    _dest_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (nullptr == _dest_tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    _slot_num = _dest_tuple_desc->slots().size();
    // get src tuple desc
    const SchemaTableDescriptor* schema_table =
            static_cast<const SchemaTableDescriptor*>(_dest_tuple_desc->table_desc());

    if (nullptr == schema_table) {
        return Status::InternalError("Failed to get schema table descriptor.");
    }

    // init schema scanner profile
    _scanner_param.profile.reset(new RuntimeProfile("SchemaScanner"));
    _runtime_profile->add_child(_scanner_param.profile.get(), true, nullptr);

    // new one scanner
    _schema_scanner = SchemaScanner::create(schema_table->schema_table_type());

    if (nullptr == _schema_scanner) {
        return Status::InternalError("schema scanner get nullptr pointer.");
    }

    RETURN_IF_ERROR(_schema_scanner->init(&_scanner_param, _pool));
    const std::vector<SchemaScanner::ColumnDesc>& columns_desc(_schema_scanner->get_column_desc());

    // if src columns size is zero, it's the dummy slots.
    if (0 == columns_desc.size()) {
        _slot_num = 0;
    }

    // check if type is ok.
    for (int i = 0; i < _slot_num; ++i) {
        // TODO(zhaochun): Is this slow?
        int j = 0;
        for (; j < columns_desc.size(); ++j) {
            if (boost::iequals(_dest_tuple_desc->slots()[i]->col_name(), columns_desc[j].name)) {
                break;
            }
        }

        if (j >= columns_desc.size()) {
            LOG(WARNING) << "no match column for this column("
                         << _dest_tuple_desc->slots()[i]->col_name() << ")";
            return Status::InternalError("no match column for this column.");
        }

        if (columns_desc[j].type != _dest_tuple_desc->slots()[i]->type().type) {
            LOG(WARNING) << "schema not match. input is " << columns_desc[j].name << "("
                         << columns_desc[j].type << ") and output is "
                         << _dest_tuple_desc->slots()[i]->col_name() << "("
                         << _dest_tuple_desc->slots()[i]->type() << ")";
            return Status::InternalError("schema not match.");
        }
    }

    // TODO(marcel): add int _tuple_idx indexed by TupleId somewhere in runtime_state.h
    _tuple_idx = 0;
    _is_init = true;

    return Status::OK();
}

Status VSchemaScanNode::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    if (state == nullptr || block == nullptr || eos == nullptr) {
        return Status::InternalError("input is NULL pointer");
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    VLOG_CRITICAL << "VSchemaScanNode::GetNext";
    if (!_is_init) {
        return Status::InternalError("used before initialize.");
    }
    RETURN_IF_CANCELLED(state);
    bool schema_eos = false;

    const std::vector<SchemaScanner::ColumnDesc>& columns_desc(_schema_scanner->get_column_desc());

    do {
        block->clear();
        for (int i = 0; i < _slot_num; ++i) {
            auto dest_slot_desc = _dest_tuple_desc->slots()[i];
            block->insert(ColumnWithTypeAndName(dest_slot_desc->get_empty_mutable_column(),
                                                dest_slot_desc->get_data_type_ptr(),
                                                dest_slot_desc->col_name()));
        }

        // src block columns desc is filled by schema_scanner->get_column_desc.
        vectorized::Block src_block;
        for (int i = 0; i < columns_desc.size(); ++i) {
            TypeDescriptor descriptor(columns_desc[i].type);
            auto data_type =
                    vectorized::DataTypeFactory::instance().create_data_type(descriptor, true);
            src_block.insert(ColumnWithTypeAndName(data_type->create_column(), data_type,
                                                   columns_desc[i].name));
        }
        while (true) {
            RETURN_IF_CANCELLED(state);

            // get all slots from schema table.
            RETURN_IF_ERROR(_schema_scanner->get_next_block(&src_block, &schema_eos));

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
                auto dest_slot_desc = _dest_tuple_desc->slots()[i];
                vectorized::MutableColumnPtr column_ptr =
                        std::move(*block->get_by_position(i).column).mutate();
                column_ptr->insert_range_from(
                        *src_block.get_by_name(dest_slot_desc->col_name()).column, 0,
                        src_block.rows());
            }
            RETURN_IF_ERROR(VExprContext::filter_block(_conjuncts, block,
                                                       _dest_tuple_desc->slots().size()));
            VLOG_ROW << "VSchemaScanNode output rows: " << src_block.rows();
            src_block.clear();
        }
    } while (block->rows() == 0 && !(*eos));

    reached_limit(block, eos);
    return Status::OK();
}

Status VSchemaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    return ExecNode::close(state);
}

void VSchemaScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "SchemaScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status VSchemaScanNode::set_scan_ranges(RuntimeState* state,
                                        const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace doris::vectorized
