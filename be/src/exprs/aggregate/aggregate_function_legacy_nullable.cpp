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

#include "agent/be_exec_version_manager.h"
#include "exprs/aggregate/aggregate_function.h"

namespace doris {

// AGG_STATE's persisted nullable representation predates native intermediate columns.
// Keep that byte codec at the type boundary: old and new rowsets, compaction and Exchange
// all see String, while every add/merge/reset operates on the same V2 execution state.
// Other native AGG_STATE types (count, bitmap, array, map) retain their existing codecs.

AggregateFunctionPtr make_agg_state_compatible_function(AggregateFunctionPtr function) {
    if (function->has_legacy_nullable_state_format()) {
        const auto* concrete_function = function.get();
        return concrete_function->create_legacy_nullable_codec(std::move(function));
    }
    return function;
}

AggregateFunctionPtr make_nullable_wire_compatible_function(AggregateFunctionPtr function,
                                                            int be_exec_version,
                                                            bool legacy_unary_v2) {
    // Before version 20, only unary functions have a native nullable codec. The old
    // protocol flag tells us which unary codec a coordinator requested; computation is V2
    // in both cases. Persisted AGG_STATE uses its separate, session-independent boundary.
    if (be_exec_version < USE_UNIFIED_NULLABLE_AGGREGATION_VERSION &&
        (function->get_argument_types().size() > 1 || !legacy_unary_v2)) {
        return make_agg_state_compatible_function(std::move(function));
    }
    return function;
}

} // namespace doris
