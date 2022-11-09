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

#include "schema_scan_node.h"

#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/string_util.h"

namespace doris {

SchemaScanNode::SchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.schema_scan_node.table_name),
          _tuple_id(tnode.schema_scan_node.tuple_id),
          _src_tuple_desc(nullptr),
          _dest_tuple_desc(nullptr),
          _tuple_idx(0),
          _slot_num(0),
          _tuple_pool(nullptr),
          _schema_scanner(nullptr),
          _src_tuple(nullptr),
          _dest_tuple(nullptr) {}

SchemaScanNode::~SchemaScanNode() {
    delete[] reinterpret_cast<char*>(_src_tuple);
    _src_tuple = nullptr;
}

Status SchemaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode, state));
    if (tnode.schema_scan_node.__isset.db) {
        _scanner_param.db = _pool->add(new std::string(tnode.schema_scan_node.db));
    }

    if (tnode.schema_scan_node.__isset.table) {
        _scanner_param.table = _pool->add(new std::string(tnode.schema_scan_node.table));
    }

    if (tnode.schema_scan_node.__isset.wild) {
        _scanner_param.wild = _pool->add(new std::string(tnode.schema_scan_node.wild));
    }

    if (tnode.schema_scan_node.__isset.current_user_ident) {
        _scanner_param.current_user_ident =
                _pool->add(new TUserIdentity(tnode.schema_scan_node.current_user_ident));
    } else {
        if (tnode.schema_scan_node.__isset.user) {
            _scanner_param.user = _pool->add(new std::string(tnode.schema_scan_node.user));
        }
        if (tnode.schema_scan_node.__isset.user_ip) {
            _scanner_param.user_ip = _pool->add(new std::string(tnode.schema_scan_node.user_ip));
        }
    }

    if (tnode.schema_scan_node.__isset.ip) {
        _scanner_param.ip = _pool->add(new std::string(tnode.schema_scan_node.ip));
    }
    if (tnode.schema_scan_node.__isset.port) {
        _scanner_param.port = tnode.schema_scan_node.port;
    }

    if (tnode.schema_scan_node.__isset.thread_id) {
        _scanner_param.thread_id = tnode.schema_scan_node.thread_id;
    }

    if (tnode.schema_scan_node.__isset.table_structure) {
        _scanner_param.table_structure = _pool->add(
                new std::vector<TSchemaTableStructure>(tnode.schema_scan_node.table_structure));
    }
    return Status::OK();
}

Status SchemaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    // new one mem pool
    _tuple_pool.reset(new (std::nothrow) MemPool());

    if (nullptr == _tuple_pool.get()) {
        return Status::InternalError("Allocate MemPool failed.");
    }

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

    // new one scanner
    _schema_scanner.reset(SchemaScanner::create(schema_table->schema_table_type()));

    if (nullptr == _schema_scanner.get()) {
        return Status::InternalError("schema scanner get nullptr pointer.");
    }

    RETURN_IF_ERROR(_schema_scanner->init(&_scanner_param, _pool));
    // get column info from scanner
    _src_tuple_desc = _schema_scanner->tuple_desc();

    if (nullptr == _src_tuple_desc) {
        return Status::InternalError("failed to get src schema tuple desc.");
    }

    _src_tuple = reinterpret_cast<Tuple*>(new (std::nothrow) char[_src_tuple_desc->byte_size()]);

    if (nullptr == _src_tuple) {
        return Status::InternalError("new src tuple failed.");
    }

    // if src tuple desc slots is zero, it's the dummy slots.
    if (0 == _src_tuple_desc->slots().size()) {
        _slot_num = 0;
    }

    // check if type is ok.
    if (_slot_num > 0) {
        _index_map.resize(_slot_num);
    }
    for (int i = 0; i < _slot_num; ++i) {
        // TODO(zhaochun): Is this slow?
        int j = 0;
        for (; j < _src_tuple_desc->slots().size(); ++j) {
            if (iequal(_dest_tuple_desc->slots()[i]->col_name(),
                       _src_tuple_desc->slots()[j]->col_name())) {
                break;
            }
        }

        if (j >= _src_tuple_desc->slots().size()) {
            LOG(WARNING) << "no match column for this column("
                         << _dest_tuple_desc->slots()[i]->col_name() << ")";
            return Status::InternalError("no match column for this column.");
        }

        if (_src_tuple_desc->slots()[j]->type().type != _dest_tuple_desc->slots()[i]->type().type) {
            LOG(WARNING) << "schema not match. input is " << _src_tuple_desc->slots()[j]->col_name()
                         << "(" << _src_tuple_desc->slots()[j]->type() << ") and output is "
                         << _dest_tuple_desc->slots()[i]->col_name() << "("
                         << _dest_tuple_desc->slots()[i]->type() << ")";
            return Status::InternalError("schema not match.");
        }
        _index_map[i] = j;
    }

    // TODO(marcel): add int _tuple_idx indexed by TupleId somewhere in runtime_state.h
    _tuple_idx = 0;
    _is_init = true;

    return Status::OK();
}

Status SchemaScanNode::open(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }

    if (nullptr == state) {
        return Status::InternalError("input pointer is nullptr.");
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    if (_scanner_param.user) {
        TSetSessionParams param;
        param.__set_user(*_scanner_param.user);
        //TStatus t_status;
        //RETURN_IF_ERROR(SchemaJniHelper::set_session(param, &t_status));
        //RETURN_IF_ERROR(Status(t_status));
    }

    return _schema_scanner->start(state);
}

void SchemaScanNode::copy_one_row() {
    memset(_dest_tuple, 0, _dest_tuple_desc->num_null_bytes());

    for (int i = 0; i < _slot_num; ++i) {
        if (!_dest_tuple_desc->slots()[i]->is_materialized()) {
            continue;
        }
        int j = _index_map[i];

        if (_src_tuple->is_null(_src_tuple_desc->slots()[j]->null_indicator_offset())) {
            _dest_tuple->set_null(_dest_tuple_desc->slots()[i]->null_indicator_offset());
        } else {
            void* dest_slot = _dest_tuple->get_slot(_dest_tuple_desc->slots()[i]->tuple_offset());
            void* src_slot = _src_tuple->get_slot(_src_tuple_desc->slots()[j]->tuple_offset());
            int slot_size = _src_tuple_desc->slots()[j]->type().get_slot_size();
            memcpy(dest_slot, src_slot, slot_size);
        }
    }
}

Status SchemaScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("GetNext before Init.");
    }

    if (nullptr == state || nullptr == row_batch || nullptr == eos) {
        return Status::InternalError("input pointer is nullptr.");
    }

    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    if (reached_limit()) {
        *eos = true;
        return Status::OK();
    }

    // create new tuple buffer for row_batch
    int tuple_buffer_size = row_batch->capacity() * _dest_tuple_desc->byte_size();
    void* tuple_buffer = _tuple_pool->allocate(tuple_buffer_size);

    if (nullptr == tuple_buffer) {
        return Status::InternalError("Allocate tuple buffer failed.");
    }

    // no use to clear, because CopyOneRow can clear
    _dest_tuple = reinterpret_cast<Tuple*>(tuple_buffer);
    // Indicates whether there are more rows to process. Set in _schema_scanner.get_next().
    bool scanner_eos = false;

    while (true) {
        RETURN_IF_CANCELLED(state);

        if (reached_limit() || row_batch->is_full()) {
            // hang on to last allocated chunk in pool, we'll keep writing into it in the
            // next get_next() call
            row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), !reached_limit());
            *eos = reached_limit();
            return Status::OK();
        }

        RETURN_IF_ERROR(_schema_scanner->get_next_row(_src_tuple, _tuple_pool.get(), &scanner_eos));

        if (scanner_eos) {
            row_batch->tuple_data_pool()->acquire_data(_tuple_pool.get(), false);
            *eos = true;
            return Status::OK();
        }

        int row_idx = row_batch->add_row();
        TupleRow* row = row_batch->get_row(row_idx);
        row->set_tuple(_tuple_idx, _dest_tuple);
        copy_one_row();

        // Error logging: Flush error stream and add name of HBase table and current row key.
        // check now
        if (eval_conjuncts(&_conjunct_ctxs[0], _conjunct_ctxs.size(), row)) {
            row_batch->commit_last_row();
            ++_num_rows_returned;
            COUNTER_SET(_rows_returned_counter, _num_rows_returned);
            char* new_tuple = reinterpret_cast<char*>(_dest_tuple);
            new_tuple += _dest_tuple_desc->byte_size();
            _dest_tuple = reinterpret_cast<Tuple*>(new_tuple);
        }
    }

    return Status::OK();
}

Status SchemaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    _tuple_pool.reset();
    return ExecNode::close(state);
}

void SchemaScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "SchemaScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status SchemaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace doris

/* vim: set ts=4 sw=4 sts=4 tw=100 : */
