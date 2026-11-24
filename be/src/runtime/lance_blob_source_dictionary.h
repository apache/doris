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

#include <gen_cpp/PlanNodes_types.h>

#include <cstdint>
#include <map>
#include <string>

#include "common/status.h"

namespace doris {

struct LanceBlobSource {
    // Dataset URI, version and access options live in file_scan_range_params_map.
    int32_t scan_node_id;
    std::string column_name;
};

// Immutable after QueryContext initialization. FE assigns every nonzero source ID before
// execution; all participating BEs install the same dictionary, including consumer-only BEs.
class LanceBlobSourceDictionary {
public:
    Status init(const std::map<int, TFileScanRangeParams>& scan_params);
    const LanceBlobSource* find_source(uint32_t source_id) const;

private:
    std::map<uint32_t, LanceBlobSource> _sources_by_id;
};

} // namespace doris
