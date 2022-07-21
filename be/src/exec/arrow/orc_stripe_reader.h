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

#include <arrow/type_fwd.h>
#include <exprs/expr.h>
#include <exprs/expr_context.h>

#include <unordered_set>

#include "common/status.h"
#include "exec/arrow/orc_reader.h"

namespace doris {

class ORCReaderWrap;

class StripeReader {
public:
    StripeReader(const std::vector<ExprContext*>& conjunct_ctxs, ORCReaderWrap* parent);
    ~StripeReader();

    Status init_filter_groups(const TupleDescriptor* tuple_desc,
                              const std::map<std::string, int>& map_column,
                              const std::vector<int>& include_column_ids);

    std::unordered_set<int> filter_groups() { return _filter_group; };

private:
    void _init_conjuncts(const TupleDescriptor* tuple_desc,
                         const std::map<std::string, int>& _map_column,
                         const std::unordered_set<int>& include_column_ids);

private:
    std::map<int, std::vector<ExprContext*>> _slot_conjuncts;
    std::unordered_set<int> _filter_group;

    std::vector<ExprContext*> _conjunct_ctxs;
    ORCReaderWrap* _parent;
};
} // namespace doris
