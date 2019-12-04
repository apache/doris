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

#include "exec/es/es_scan_reader.h"

#include <map>
#include <string>
#include <sstream>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/es/es_scroll_query.h"

namespace doris {
const std::string REUQEST_SCROLL_FILTER_PATH = "filter_path=_scroll_id,hits.hits._source,hits.total,_id,hits.hits._source.fields,hits.hits.fields";
const std::string REQUEST_SCROLL_PATH = "_scroll";
const std::string REQUEST_PREFERENCE_PREFIX = "&preference=_shards:";
const std::string REQUEST_SEARCH_SCROLL_PATH = "/_search/scroll";
const std::string REQUEST_SEPARATOR = "/";

ESScanReader::ESScanReader(const std::string& target, const std::map<std::string, std::string>& props) : _scroll_keep_alive(config::es_scroll_keepalive), _http_timeout_ms(config::es_http_timeout_ms) {
    _target = target;
    _index = props.at(KEY_INDEX);
    _type = props.at(KEY_TYPE);
    if (props.find(KEY_USER_NAME) != props.end()) {
        _user_name = props.at(KEY_USER_NAME);
    }
    if (props.find(KEY_PASS_WORD) != props.end()){
        _passwd = props.at(KEY_PASS_WORD);
    }
    if (props.find(KEY_SHARD) != props.end()) {
        _shards = props.at(KEY_SHARD);
    }
    if (props.find(KEY_QUERY) != props.end()) {
        _query = props.at(KEY_QUERY);
    }
    std::string batch_size_str = props.at(KEY_BATCH_SIZE);
    _batch_size = atoi(batch_size_str.c_str());
    _init_scroll_url = _target + REQUEST_SEPARATOR + _index + REQUEST_SEPARATOR + _type + "/_search?scroll=" + _scroll_keep_alive + REQUEST_PREFERENCE_PREFIX + _shards + "&" + REUQEST_SCROLL_FILTER_PATH;
    _next_scroll_url = _target + REQUEST_SEARCH_SCROLL_PATH + "?" + REUQEST_SCROLL_FILTER_PATH;
    _eos = false;
}

ESScanReader::~ESScanReader() {
}

Status ESScanReader::open() {
    _is_first = true;
    RETURN_IF_ERROR(_network_client.init(_init_scroll_url));
    LOG(INFO) << "First scroll request URL: " << _init_scroll_url;
    _network_client.set_basic_auth(_user_name, _passwd);
    _network_client.set_content_type("application/json");
    // phase open, we cached the first response for `get_next` phase
    Status status = _network_client.execute_post_request(_query, &_cached_response);
    if (!status.ok() || _network_client.get_http_status() != 200) {
        std::stringstream ss;
        ss << "Failed to connect to ES server, errmsg is: " << status.get_error_msg();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    VLOG(1) << "open _cached response: " << _cached_response;
    return Status::OK();
}

Status ESScanReader::get_next(bool* scan_eos, std::unique_ptr<ScrollParser>& scroll_parser) {
    std::string response;
    // if is first scroll request, should return the cached response
    *scan_eos = true;
    if (_eos) {
        return Status::OK();
    }

    if (_is_first) {
        response = _cached_response;
        _is_first = false;
    } else {
        RETURN_IF_ERROR(_network_client.init(_next_scroll_url));
        _network_client.set_basic_auth(_user_name, _passwd);
        _network_client.set_content_type("application/json");
        _network_client.set_timeout_ms(_http_timeout_ms);
        RETURN_IF_ERROR(_network_client.execute_post_request(
                        ESScrollQueryBuilder::build_next_scroll_body(_scroll_id, _scroll_keep_alive), &response));
        long status = _network_client.get_http_status();
        if (status == 404) {
            LOG(WARNING) << "request scroll search failure 404[" 
                         << ", response: " << (response.empty() ? "empty response" : response);
            return Status::InternalError("No search context found for " + _scroll_id);
        }
        if (status != 200) {
            LOG(WARNING) << "request scroll search failure[" 
                         << "http status" << status
                         << ", response: " << (response.empty() ? "empty response" : response);
            return Status::InternalError("request scroll search failure: " + (response.empty() ? "empty response" : response));        
        }
    }

    scroll_parser.reset(new ScrollParser());
    Status status = scroll_parser->parse(response);
    if (!status.ok()){
        _eos = true;
        LOG(WARNING) << status.get_error_msg();
        return status;
    }

    _scroll_id = scroll_parser->get_scroll_id();
    if (scroll_parser->get_total() == 0) {
        _eos = true;
        return Status::OK();
    }

    _eos = scroll_parser->get_size() < _batch_size;

    *scan_eos = false;
    return Status::OK();
}

Status ESScanReader::close() {
    if (_scroll_id.empty()) {
        return Status::OK();
    }

    std::string scratch_target = _target + REQUEST_SEARCH_SCROLL_PATH;
    RETURN_IF_ERROR(_network_client.init(scratch_target));
    _network_client.set_basic_auth(_user_name, _passwd);
    _network_client.set_method(DELETE);
    _network_client.set_content_type("application/json");
    _network_client.set_timeout_ms(5 * 1000);
    std::string response;
    RETURN_IF_ERROR(_network_client.execute_delete_request(ESScrollQueryBuilder::build_clear_scroll_body(_scroll_id), &response));
    if (_network_client.get_http_status() == 200) {
        return Status::OK();
    } else {
        return Status::InternalError("es_scan_reader delete scroll context failure");
    }
}
}
