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

#include "format/table/es/es_http_reader.h"

#include <gen_cpp/PlanNodes_types.h>

#include <algorithm>
#include <cctype>
#include <sstream>

#include "format/table/es/es_scan_reader.h"
#include "format/table/es/es_scroll_parser.h"
#include "format/table/es/es_scroll_query.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"

namespace doris {

const std::string EsHttpReader::KEY_ES_HOSTS = "es_hosts";

EsHttpReader::EsHttpReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                           RuntimeProfile* profile, const TFileRangeDesc& range,
                           const TFileScanRangeParams& params, const TupleDescriptor* tuple_desc)
        : _state(state),
          _profile(profile),
          _tuple_desc(tuple_desc),
          _range(range),
          _params(params),
          _file_slot_descs(file_slot_descs) {
    _init_profile();
}

EsHttpReader::~EsHttpReader() = default;

void EsHttpReader::_init_profile() {
    if (_profile == nullptr) {
        return;
    }
    static const char* es_profile = "EsHttpReader";
    ADD_TIMER_WITH_LEVEL(_profile, es_profile, 1);
    _es_profile.read_time = ADD_CHILD_TIMER_WITH_LEVEL(_profile, "EsReadTime", es_profile, 1);
    _es_profile.materialize_time =
            ADD_CHILD_TIMER_WITH_LEVEL(_profile, "EsMaterializeTime", es_profile, 1);
    _es_profile.batches_read =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "EsBatchesRead", TUnit::UNIT, es_profile, 1);
    _es_profile.rows_read =
            ADD_CHILD_COUNTER_WITH_LEVEL(_profile, "EsRowsRead", TUnit::UNIT, es_profile, 1);
}

std::string EsHttpReader::_extract_hostname(std::string host) {
    auto scheme_end = host.find("://");
    if (scheme_end != std::string::npos) {
        host = host.substr(scheme_end + 3);
    }

    if (!host.empty() && host.front() == '[') {
        auto bracket_end = host.find(']');
        if (bracket_end != std::string::npos) {
            return host.substr(1, bracket_end - 1);
        }
        return host;
    }

    auto colon = host.rfind(':');
    if (colon == std::string::npos) {
        return host;
    }
    std::string port = host.substr(colon + 1);
    if (!port.empty() && std::all_of(port.begin(), port.end(),
                                     [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
        return host.substr(0, colon);
    }
    return host;
}

Status EsHttpReader::init_reader() {
    // Build properties map from Thrift params, combining per-range (es_params)
    // and per-node (es_properties) parameters.
    std::map<std::string, std::string> properties;

    // Per-node shared properties (auth, query_dsl, doc_values_mode, etc.)
    if (_params.__isset.es_properties) {
        properties.insert(_params.es_properties.begin(), _params.es_properties.end());
    }

    // Per-range shard-specific properties (override per-node if same key)
    if (_range.__isset.table_format_params && _range.table_format_params.__isset.es_params) {
        for (const auto& [key, value] : _range.table_format_params.es_params) {
            properties[key] = value;
        }
    }

    // Set batch_size from runtime state
    properties[ESScanReader::KEY_BATCH_SIZE] = std::to_string(_state->batch_size());

    // Extract docvalue and fields context
    if (_params.__isset.es_docvalue_context) {
        _docvalue_context = _params.es_docvalue_context;
    }
    if (_params.__isset.es_fields_context) {
        _fields_context = _params.es_fields_context;
    }

    // Build column names for query DSL
    std::vector<std::string> column_names;
    for (const auto* slot_desc : _tuple_desc->slots()) {
        column_names.push_back(slot_desc->col_name());
    }

    // Build the final query body using ESScrollQueryBuilder
    properties[ESScanReader::KEY_QUERY] = ESScrollQueryBuilder::build(
            properties, column_names, _docvalue_context, &_doc_value_mode);

    // Select best host from es_hosts list, preferring localhost for locality
    std::string target_host = _select_host(properties);
    properties[ESScanReader::KEY_HOST_PORT] = target_host;
    _es_reader = std::make_unique<ESScanReader>(target_host, properties, _doc_value_mode);

    {
        SCOPED_RAW_TIMER(&_read_timer_ns);
        RETURN_IF_ERROR(_es_reader->open());
    }
    return Status::OK();
}

Status EsHttpReader::_do_get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_es_eof) {
        *eof = true;
        *read_rows = 0;
        return Status::OK();
    }

    auto column_size = _tuple_desc->slots().size();
    std::vector<MutableColumnPtr> columns(column_size);
    for (size_t i = 0; i < column_size; i++) {
        columns[i] = block->get_by_position(i).column->assume_mutable();
    }

    size_t rows_before = columns[0]->size();
    const int batch_size = _state->batch_size();

    while (columns[0]->size() - rows_before < batch_size && !_es_eof) {
        RETURN_IF_CANCELLED(_state);

        if (_line_eof && _batch_eof) {
            _es_eof = true;
            break;
        }

        while (!_batch_eof) {
            if (_line_eof || _es_scroll_parser == nullptr) {
                RETURN_IF_ERROR(_scroll_and_parse());
                if (_batch_eof) {
                    _es_eof = true;
                    break;
                }
            }

            {
                SCOPED_RAW_TIMER(&_materialize_timer_ns);
                RETURN_IF_ERROR(_es_scroll_parser->fill_columns(_tuple_desc, columns, &_line_eof,
                                                                _docvalue_context,
                                                                _state->timezone_obj()));
            }
            if (!_line_eof) {
                break;
            }
        }
    }

    *read_rows = columns[0]->size() - rows_before;
    _rows_read += *read_rows;
    *eof = _es_eof && *read_rows == 0;
    return Status::OK();
}

Status EsHttpReader::_scroll_and_parse() {
    {
        SCOPED_RAW_TIMER(&_read_timer_ns);
        RETURN_IF_ERROR(_es_reader->get_next(&_batch_eof, _es_scroll_parser));
    }
    if (!_batch_eof) {
        ++_batches_read;
    }
    _line_eof = false;
    return Status::OK();
}

void EsHttpReader::_collect_profile_before_close() {
    if (_profile == nullptr) {
        return;
    }
    COUNTER_UPDATE(_es_profile.read_time, _read_timer_ns);
    COUNTER_UPDATE(_es_profile.materialize_time, _materialize_timer_ns);
    COUNTER_UPDATE(_es_profile.batches_read, _batches_read);
    COUNTER_UPDATE(_es_profile.rows_read, _rows_read);
}

Status EsHttpReader::close() {
    if (_es_reader) {
        RETURN_IF_ERROR(_es_reader->close());
    }
    return Status::OK();
}

Status EsHttpReader::_get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) {
    for (const auto* slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

std::string EsHttpReader::_select_host(const std::map<std::string, std::string>& properties) const {
    // If es_hosts contains multiple hosts, prefer localhost for locality
    auto it = properties.find(KEY_ES_HOSTS);
    if (it != properties.end() && !it->second.empty()) {
        std::string localhost = BackendOptions::get_localhost();
        std::string best;
        std::istringstream stream(it->second);
        std::string host;
        while (std::getline(stream, host, ',')) {
            if (best.empty()) {
                best = host;
            }
            std::string hostname = _extract_hostname(host);
            if (hostname == localhost) {
                return host;
            }
        }
        if (!best.empty()) {
            return best;
        }
    }
    // Fallback to host_port
    auto hp = properties.find(ESScanReader::KEY_HOST_PORT);
    if (hp != properties.end()) {
        return hp->second;
    }
    return "";
}

} // namespace doris
