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

#include <rapidjson/rapidjson.h>

#include <map>
#include <string>
#include <vector>

#include "rapidjson/document.h"
#include "vec/data_types/data_type.h"

namespace doris {

class Status;
class TupleDescriptor;

class ScrollParser {
public:
    ScrollParser(bool doc_value_mode);
    ~ScrollParser();

    Status parse(const std::string& scroll_result, bool exactly_once = false);
    // Add time_zone info to convert time field of ES to local time zone of Doris
    Status fill_columns(const TupleDescriptor* _tuple_desc,
                        std::vector<vectorized::MutableColumnPtr>& columns, bool* line_eof,
                        const std::map<std::string, std::string>& docvalue_context,
                        const cctz::time_zone& time_zone);

    const std::string& get_scroll_id();
    int get_size() const;

private:
    std::string _scroll_id;
    int _size;
    rapidjson::SizeType _line_index;

    rapidjson::Document _document_node;
    rapidjson::Value _inner_hits_node;

    // todo(milimin): ScrollParser should be divided into two classes: SourceParser and DocValueParser,
    // including remove some variables in the current implementation, e.g. pure_doc_value.
    // All above will be done in the DOE refactoring projects.
    // Current bug fixes minimize the scope of changes to avoid introducing other new bugs.
    // bool _doc_value_mode;
};
} // namespace doris
