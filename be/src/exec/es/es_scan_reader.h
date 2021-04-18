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

#include <string>

#include "exec/es/es_scroll_parser.h"
#include "http/http_client.h"

using std::string;

namespace doris {

class Status;

class ESScanReader {
public:
    static constexpr const char* KEY_USER_NAME = "user";
    static constexpr const char* KEY_PASS_WORD = "password";
    static constexpr const char* KEY_HOST_PORT = "host_port";
    static constexpr const char* KEY_INDEX = "index";
    static constexpr const char* KEY_TYPE = "type";
    static constexpr const char* KEY_SHARD = "shard_id";
    static constexpr const char* KEY_QUERY = "query";
    static constexpr const char* KEY_BATCH_SIZE = "batch_size";
    static constexpr const char* KEY_TERMINATE_AFTER = "limit";
    static constexpr const char* KEY_DOC_VALUES_MODE = "doc_values_mode";
    static constexpr const char* KEY_HTTP_SSL_ENABLED = "http_ssl_enabled";
    ESScanReader(const std::string& target, const std::map<std::string, std::string>& props,
                 bool doc_value_mode);
    ~ESScanReader();

    // launch the first scroll request, this method will cache the first scroll response, and return the this cached response when invoke get_next
    Status open();
    // invoke get_next to get next batch documents from elasticsearch
    Status get_next(bool* eos, std::unique_ptr<ScrollParser>& parser);
    // clear scroll context from elasticsearch
    Status close();

private:
    std::string _target;
    std::string _user_name;
    std::string _passwd;
    std::string _scroll_id;
    HttpClient _network_client;
    std::string _index;
    std::string _type;
    // push down filter
    std::string _query;
    // Elasticsearch shards to fetch document
    std::string _shards;
    // whether use ssl client
    bool _use_ssl_client = false;
    // distinguish the first scroll phase and the following scroll
    bool _is_first;

    // `_init_scroll_url` and `_next_scroll_url` used for scrolling result from Elasticsearch
    //In order to use scrolling, the initial search request should specify the scroll parameter in the query string,
    // which tells Elasticsearch how long it should keep the “search context” alive:
    // {index}/{type}/_search?scroll=5m
    std::string _init_scroll_url;
    // The result from the above request includes a _scroll_id, which should be passed to the scroll API in order to retrieve the next batch of results
    // _next_scroll_url for the subsequent scroll request, like /_search/scroll
    // POST /_search/scroll
    // {
    //    "scroll" : "1m",
    //    "scroll_id" : "DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAAAD4WYm9laVYtZndUQlNsdDcwakFMNjU1QQ=="
    // }
    // Each call to the scroll API returns the next batch of results until there are no more results left to return
    std::string _next_scroll_url;

    // _search_url used to execute just only one search request to Elasticsearch
    // _search_url would go into effect when `limit` specified:
    // select * from es_table limit 10 -> /es_table/doc/_search?terminate_after=10
    std::string _search_url;
    bool _eos;
    int _batch_size;

    std::string _cached_response;
    // keep-alive for es scroll
    std::string _scroll_keep_alive;
    // timeout for es http connection
    int _http_timeout_ms;

    bool _exactly_once;

    bool _doc_value_mode;
};
} // namespace doris
