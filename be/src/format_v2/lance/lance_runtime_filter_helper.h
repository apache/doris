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

#include <memory>
#include <string>
#include <vector>

#include "exprs/vexpr_fwd.h"

namespace doris {

class ShardedKVCache;
class RuntimeProfile;

namespace format::lance {

struct LanceRuntimeFilterSql {
    std::string expression;
    std::vector<int> pushable_filter_ids;
    std::vector<int> skipped_filter_ids;
};

// Build one immutable SQL snapshot for all supported Doris runtime filters. When cache is non-null,
// equivalent RF snapshots from parallel Lance readers in the same FileScanLocalState share the
// conversion result. The returned snapshot also identifies RFs that cannot be represented exactly
// by Lance SQL. A null result means that conjuncts contain no Doris runtime filter.
std::shared_ptr<const LanceRuntimeFilterSql> get_or_create_lance_runtime_filter_sql(
        const VExprContextSPtrs& conjuncts, ShardedKVCache* cache);

void record_lance_runtime_filter_pushdown(RuntimeProfile* profile,
                                          const LanceRuntimeFilterSql& runtime_filter_sql);

} // namespace format::lance
} // namespace doris
