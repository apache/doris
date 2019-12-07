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

#include "rapidjson/document.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"

namespace doris {

class Status;

class ScrollParser {

public:
    ScrollParser();
    ~ScrollParser();

    Status parse(const std::string& scroll_result, bool exactly_once = false);
    Status fill_tuple(const TupleDescriptor* _tuple_desc, Tuple* tuple, 
                MemPool* mem_pool, bool* line_eof, const std::map<std::string, std::string>& docvalue_context);

    const std::string& get_scroll_id();
    int get_total();
    int get_size();

private:

    std::string _scroll_id;
    int _total;
    int _size;
    rapidjson::SizeType _line_index;

    rapidjson::Document _document_node;
    rapidjson::Value _inner_hits_node;
};
}
