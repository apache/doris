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

#include <stdint.h>

#include <string>
#include <map>

#include "common/status.h"

namespace doris {

class EsReader {
public:
    constexpr static const char* HOST = "host";
    constexpr static const char* INDEX = "index";
    constexpr static const char* TYPE = "type";
    constexpr static const char* SHARD_ID = "shard_id";
    constexpr static const char* BATCH_SIZE = "batch_size";

    EsReader(const std::string& target,
                const std::map<std::string, std::string>& properties,
                const std::vector<std::string>& columns) : 
        _target(target),
        _properties(properties),
        _columns(columns),
        _eof(false) {
        }

    ~EsReader() {};

    Status open() { return Status::OK; }

    Status read(uint8_t* buf, size_t* buf_len, bool* eof) {
        const char* json = "{\"_scroll_id\": \"DXF1ZXJ5QW5kRmV0Y2gBAAAAAAAA1ewWbEhKNHRWX1NTNG04bERuV05RUlA5Zw==\",\"hits\": {\"total\": 10,\"hits\": [{\"_source\": {\"id\": 1}},{\"_source\": {\"id\": 2}}]}}";
        memcpy(buf, json, strlen(json));
        *buf_len = strlen(json);
        *eof = true;
        return Status::OK;
    }

    void close() {};

private:

    const std::string& _target;
    const std::map<std::string, std::string>& _properties;
    const std::vector<std::string>& _columns;
    bool _eof;
};

}

