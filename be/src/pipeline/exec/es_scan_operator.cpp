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

#include "pipeline/exec/es_scan_operator.h"

#include "exec/es/es_scan_reader.h"
#include "exec/es/es_scroll_query.h"
#include "vec/exec/scan/new_es_scanner.h"

namespace doris::pipeline {

// Prefer to the local host
static std::string get_host_and_port(const std::vector<doris::TNetworkAddress>& es_hosts) {
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

Status EsScanLocalState::_init_profile() {
    RETURN_IF_ERROR(Base::_init_profile());
    _es_profile.reset(new RuntimeProfile("EsIterator"));
    Base::_scanner_profile->add_child(_es_profile.get(), true, nullptr);

    _rows_read_counter = ADD_COUNTER(_es_profile, "RowsRead", TUnit::UNIT);
    _read_timer = ADD_TIMER(_es_profile, "TotalRawReadTime(*)");
    _materialize_timer = ADD_TIMER(_es_profile, "MaterializeTupleTime(*)");
    return Status::OK();
}

Status EsScanLocalState::_process_conjuncts() {
    RETURN_IF_ERROR(Base::_process_conjuncts());
    if (Base::_eos_dependency->read_blocked_by() == nullptr) {
        return Status::OK();
    }

    CHECK(Base::_parent->cast<EsScanOperatorX>()._properties.find(ESScanReader::KEY_QUERY_DSL) !=
          Base::_parent->cast<EsScanOperatorX>()._properties.end());
    return Status::OK();
}

Status EsScanLocalState::_init_scanners(std::list<vectorized::VScannerSPtr>* scanners) {
    if (_scan_ranges.empty()) {
        Base::_eos_dependency->set_ready_for_read();
        return Status::OK();
    }

    auto& p = Base::_parent->cast<EsScanOperatorX>();
    for (auto& es_scan_range : _scan_ranges) {
        // Collect the information from scan range to properties
        std::map<std::string, std::string> properties(p._properties);
        properties[ESScanReader::KEY_INDEX] = es_scan_range->index;
        if (es_scan_range->__isset.type) {
            properties[ESScanReader::KEY_TYPE] = es_scan_range->type;
        }
        properties[ESScanReader::KEY_SHARD] = std::to_string(es_scan_range->shard_id);
        properties[ESScanReader::KEY_BATCH_SIZE] =
                std::to_string(vectorized::RuntimeFilterConsumer::_state->batch_size());
        properties[ESScanReader::KEY_HOST_PORT] = get_host_and_port(es_scan_range->es_hosts);
        // push down limit to Elasticsearch
        // if predicate in _conjunct_ctxs can not be processed by Elasticsearch, we can not push down limit operator to Elasticsearch
        if (p.limit() != -1 &&
            p.limit() <= vectorized::RuntimeFilterConsumer::_state->batch_size()) {
            properties[ESScanReader::KEY_TERMINATE_AFTER] = std::to_string(p.limit());
        }

        bool doc_value_mode = false;
        properties[ESScanReader::KEY_QUERY] = ESScrollQueryBuilder::build(
                properties, p._column_names, p._docvalue_context, &doc_value_mode);

        std::shared_ptr<vectorized::NewEsScanner> scanner = vectorized::NewEsScanner::create_shared(
                vectorized::RuntimeFilterConsumer::_state, this, p._limit_per_scanner, p._tuple_id,
                properties, p._docvalue_context, doc_value_mode,
                vectorized::RuntimeFilterConsumer::_state->runtime_profile());

        RETURN_IF_ERROR(
                scanner->prepare(vectorized::RuntimeFilterConsumer::_state, Base::_conjuncts));
        scanners->push_back(scanner);
    }

    return Status::OK();
}

void EsScanLocalState::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    for (auto& es_scan_range : scan_ranges) {
        DCHECK(es_scan_range.scan_range.__isset.es_scan_range);
        _scan_ranges.emplace_back(new TEsScanRange(es_scan_range.scan_range.es_scan_range));
    }
}

EsScanOperatorX::EsScanOperatorX(ObjectPool* pool, const TPlanNode& tnode,
                                 const DescriptorTbl& descs)
        : ScanOperatorX<EsScanLocalState>(pool, tnode, descs),
          _tuple_id(tnode.es_scan_node.tuple_id),
          _tuple_desc(nullptr) {
    ScanOperatorX<EsScanLocalState>::_output_tuple_id = tnode.es_scan_node.tuple_id;
}

Status EsScanOperatorX::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperatorX<EsScanLocalState>::init(tnode, state));

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

Status EsScanOperatorX::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ScanOperatorX<EsScanLocalState>::prepare(state));

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

} // namespace doris::pipeline
