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
#include <vector>

#include "exec/es/es_predicate.h"

namespace doris {

class ESScrollQueryBuilder {
public:
    ESScrollQueryBuilder();
    ~ESScrollQueryBuilder();
    // build the query DSL for elasticsearch
    static std::string build_next_scroll_body(const std::string& scroll_id,
                                              const std::string& scroll);
    static std::string build_clear_scroll_body(const std::string& scroll_id);
    // @note: predicates should processed before pass it to this method,
    // tie breaker for predicate whether can push down es can reference the push-down filters
    static std::string build(const std::map<std::string, std::string>& properties,
                             const std::vector<std::string>& fields,
                             std::vector<EsPredicate*>& predicates,
                             const std::map<std::string, std::string>& docvalue_context,
                             bool* doc_value_mode);
};
} // namespace doris
