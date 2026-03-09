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

#include <gen_cpp/Types_types.h>
#include <stddef.h>

#include <memory>
#include <string>

#include "common/object_pool.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "core/data_type_serde/data_type_serde.h"
#include "exprs/aggregate/aggregate_function.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"

namespace doris {

class TFoldConstantParams;
class TQueryGlobals;
class PConstantExprResult;
class RuntimeProfile;
class TQueryOptions;

// This class used to fold constant expr from fe
class FoldConstantExecutor {
public:
    // fold constant vexpr
    Status fold_constant_vexpr(const TFoldConstantParams& params, PConstantExprResult* response);
    std::string query_id_string() { return print_id(_query_id); }

private:
    // init runtime_state and mem_tracker
    Status _init(const TQueryGlobals& query_globals, const TQueryOptions& query_options);
    // prepare expr
    template <typename Context>
    Status _prepare_and_open(Context* ctx);

    Status _get_result(void* src, size_t size, const DataTypePtr& type, ColumnPtr column_ptr,
                       DataTypePtr column_type, std::string& result,
                       const DataTypeSerDe::FormatOptions& options);

    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<MemTrackerLimiter> _mem_tracker;
    RuntimeProfile* _runtime_profile = nullptr;
    ObjectPool _pool;
    TUniqueId _query_id;
};
} // namespace doris