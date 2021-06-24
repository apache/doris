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

#include <map>

#include "olap/rowset/rowset_meta.h"
#include "olap/tuple.h"

namespace doris {
// KEY: only split the ranges by key
// SEGMENT: split the ranges by SEGMENT, but the key range is work too, 
// because of key range contains some filter condition
enum ScanRangeType { KEY = 0, SEGMENT = 1 };
struct ScanRange {
    size_t size() const {
        if (range_type == KEY) {
            return key_range.size();
        } else if (range_type == SEGMENT) {
            return segments.size();
        }
        return 0;
    }
    std::string to_string() {
        std::stringstream ss;
        ss << "range_type: " << range_type << "\n"
           << "segments:[";
        for (auto sv : segments) {
            ss << "[";
            for (auto p : sv) {
                ss << "(" << p.first.to_string() << "," << p.second << ") ";
            }
            ss << "] ";
        }
        ss << "]\n";
        return ss.str();
    }

    ScanRangeType range_type;
    std::vector<OlapTuple> key_range;
    std::vector<std::vector<std::pair<RowsetId, uint64_t>>> segments;
};

} // namespace doris
