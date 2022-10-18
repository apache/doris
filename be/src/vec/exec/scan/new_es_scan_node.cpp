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

#include "vec/exec/scan/new_es_scan_node.h"

#include "exec/es/es_query_builder.h"
#include "exec/es/es_scroll_query.h"
#include "vec/exec/scan/new_es_scanner.h"
#include "vec/utils/util.hpp"

static const std::string NEW_SCAN_NODE_TYPE = "NewEsScanNode";

// Prefer to the local host
static std::string get_host_port(const std::vector<doris::TNetworkAddress>& es_hosts) {
    std::string host_port;
    std::string localhost = doris::BackendOptions::get_localhost();

    doris::TNetworkAddress host = es_hosts[0];
    for (auto& es_host : es_hosts) {
        if (es_host.hostname == localhost) {
            host = es_host;
            break;
        }
    }

    host_port = host.hostname;
    host_port += ":";
    host_port += std::to_string(host.port);
    return host_port;
}

namespace doris::vectorized {

NewEsScanNode::NewEsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : VScanNode(pool, tnode, descs),
          _tuple_id(tnode.es_scan_node.tuple_id),
          _tuple_desc(nullptr),
          _es_profile(nullptr) {
    _output_tuple_id = tnode.es_scan_node.tuple_id;
}

std::string NewEsScanNode::get_name() {
    return fmt::format("VNewEsScanNode");
}

Status NewEsScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(VScanNode::init(tnode, state));

    // use TEsScanNode
    _properties = tnode.es_scan_node.properties;

    if (tnode.es_scan_node.__isset.docvalue_context) {
        _docvalue_context = tnode.es_scan_node.docvalue_context;
    }

    if (tnode.es_scan_node.__isset.fields_context) {
        _fields_context = tnode.es_scan_node.fields_context;
    }
    return Status::OK();
}

Status NewEsScanNode::prepare(RuntimeState* state) {
    VLOG_CRITICAL << NEW_SCAN_NODE_TYPE << "::prepare";
    RETURN_IF_ERROR(VScanNode::prepare(state));
    SCOPED_CONSUME_MEM_TRACKER(mem_tracker());

    _tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);
    if (_tuple_desc == nullptr) {
        return Status::InternalError("Failed to get tuple descriptor, _tuple_id=i{}", _tuple_id);
    }

    // set up column name vector for ESScrollQueryBuilder
    for (auto slot_desc : _tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        _column_names.push_back(slot_desc->col_name());
    }

    return Status::OK();
}

void NewEsScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& es_scan_range : scan_ranges) {
        DCHECK(es_scan_range.scan_range.__isset.es_scan_range);
        _scan_ranges.emplace_back(new TEsScanRange(es_scan_range.scan_range.es_scan_range));
    }
}

Status NewEsScanNode::_init_profile() {
    RETURN_IF_ERROR(VScanNode::_init_profile());
    _es_profile.reset(new RuntimeProfile("EsIterator"));
    _scanner_profile->add_child(_es_profile.get(), true, nullptr);

    _rows_read_counter = ADD_COUNTER(_es_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_es_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_es_profile, "MaterializeTupleTime(*)");
    return Status::OK();
}

Status NewEsScanNode::_process_conjuncts() {
    RETURN_IF_ERROR(VScanNode::_process_conjuncts());
    if (_eos) {
        return Status::OK();
    }

    // fe by enable_new_es_dsl to control whether to generate DSL for easy rollback. After the code is stable, can delete the be generation logic
    if (_properties.find(ESScanReader::KEY_QUERY_DSL) != _properties.end()) {
        return Status::OK();
    }

    // if conjunct is constant, compute direct and set eos = true
    for (int conj_idx = 0; conj_idx < _conjunct_ctxs.size(); ++conj_idx) {
        if (_conjunct_ctxs[conj_idx]->root()->is_constant()) {
            void* value = _conjunct_ctxs[conj_idx]->get_value(nullptr);
            if (value == nullptr || *reinterpret_cast<bool*>(value) == false) {
                _eos = true;
            }
        }
    }
    RETURN_IF_ERROR(build_conjuncts_list());
    // remove those predicates which ES cannot support
    std::vector<bool> list;
    BooleanQueryBuilder::validate(_predicates, &list);

    DCHECK(list.size() == _predicate_to_conjunct.size());
    for (int i = list.size() - 1; i >= 0; i--) {
        if (!list[i]) {
            _predicate_to_conjunct.erase(_predicate_to_conjunct.begin() + i);
            _predicates.erase(_predicates.begin() + i);
        }
    }

    // filter the conjuncts and ES will process them later
    for (int i = _predicate_to_conjunct.size() - 1; i >= 0; i--) {
        int conjunct_index = _predicate_to_conjunct[i];
        _conjunct_ctxs[conjunct_index]->close(_state);
        _conjunct_ctxs.erase(_conjunct_ctxs.begin() + conjunct_index);
    }

    auto checker = [&](int index) {
        return _conjunct_to_predicate[index] != -1 && list[_conjunct_to_predicate[index]];
    };

    // _peel_pushed_vconjunct
    if (_vconjunct_ctx_ptr == nullptr) {
        return Status::OK();
    }
    int leaf_index = 0;
    vectorized::VExpr* conjunct_expr_root = (*_vconjunct_ctx_ptr)->root();
    if (conjunct_expr_root != nullptr) {
        vectorized::VExpr* new_conjunct_expr_root = vectorized::VectorizedUtils::dfs_peel_conjunct(
                _state, *_vconjunct_ctx_ptr, conjunct_expr_root, leaf_index, checker);
        if (new_conjunct_expr_root == nullptr) {
            (*_vconjunct_ctx_ptr)->close(_state);
            _vconjunct_ctx_ptr.reset(nullptr);
        } else {
            (*_vconjunct_ctx_ptr)->set_root(new_conjunct_expr_root);
        }
    }
    return Status::OK();
}

Status NewEsScanNode::_init_scanners(std::list<VScanner*>* scanners) {
    if (_scan_ranges.empty()) {
        _eos = true;
        return Status::OK();
    }

    for (auto& es_scan_range : _scan_ranges) {
        // Collect the information from scan range to properties
        std::map<std::string, std::string> properties(_properties);
        properties[ESScanReader::KEY_INDEX] = es_scan_range->index;
        if (es_scan_range->__isset.type) {
            properties[ESScanReader::KEY_TYPE] = es_scan_range->type;
        }
        properties[ESScanReader::KEY_SHARD] = std::to_string(es_scan_range->shard_id);
        properties[ESScanReader::KEY_BATCH_SIZE] = std::to_string(_state->batch_size());
        properties[ESScanReader::KEY_HOST_PORT] = get_host_port(es_scan_range->es_hosts);
        // push down limit to Elasticsearch
        // if predicate in _conjunct_ctxs can not be processed by Elasticsearch, we can not push down limit operator to Elasticsearch
        if (limit() != -1 && limit() <= _state->batch_size() && _conjunct_ctxs.empty()) {
            properties[ESScanReader::KEY_TERMINATE_AFTER] = std::to_string(limit());
        }

        bool doc_value_mode = false;
        properties[ESScanReader::KEY_QUERY] = ESScrollQueryBuilder::build(
                properties, _column_names, _predicates, _docvalue_context, &doc_value_mode);

        NewEsScanner* scanner = new NewEsScanner(_state, this, _limit_per_scanner, _tuple_id,
                                                 properties, _docvalue_context, doc_value_mode);

        _scanner_pool.add(scanner);
        RETURN_IF_ERROR(scanner->prepare(_state));
        scanners->push_back(static_cast<VScanner*>(scanner));
    }
    return Status::OK();
}

// build predicate
Status NewEsScanNode::build_conjuncts_list() {
    Status status = Status::OK();
    _conjunct_to_predicate.resize(_conjunct_ctxs.size());

    for (int i = 0; i < _conjunct_ctxs.size(); ++i) {
        EsPredicate* predicate = _pool->add(new EsPredicate(_conjunct_ctxs[i], _tuple_desc, _pool));
        predicate->set_field_context(_fields_context);
        status = predicate->build_disjuncts_list();
        if (status.ok()) {
            _conjunct_to_predicate[i] = _predicate_to_conjunct.size();
            _predicate_to_conjunct.push_back(i);

            _predicates.push_back(predicate);
        } else {
            _conjunct_to_predicate[i] = -1;

            VLOG_CRITICAL << status.get_error_msg();
            status = predicate->get_es_query_status();
            if (!status.ok()) {
                LOG(WARNING) << status.get_error_msg();
                return status;
            }
        }
    }

    return Status::OK();
}
} // namespace doris::vectorized
