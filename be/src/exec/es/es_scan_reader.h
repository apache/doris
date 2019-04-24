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
    ESScanReader(const std::string& target, const std::map<std::string, std::string>& props);
    ~ESScanReader();

    // launch the first scroll request, this method will cache the first scroll response, and return the this cached response when invoke get_next
    Status open();
    // invoke get_next to get next batch documents from elasticsearch
    Status get_next(bool *eos, std::unique_ptr<ScrollParser>& parser);
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
    // elaticsearch shards to fetch document
    std::string _shards;
    // distinguish the first scroll phase and the following scroll
    bool _is_first;
    std::string _init_scroll_url;
    std::string _next_scroll_url;
    bool _eos;
    int _batch_size;

    std::string _cached_response;
};
}

