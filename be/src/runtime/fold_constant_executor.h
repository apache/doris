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

#include "common/object_pool.h"
#include "common/status.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "gen_cpp/internal_service.pb.h"
#include "runtime/define_primitive_type.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "util/runtime_profile.h"
#include "vec/data_types/data_type.h"

namespace doris {

class TFoldConstantParams;
class TExpr;
class TQueryGlobals;

// This class used to fold constant expr from fe
class FoldConstantExecutor {
public:
    // fold constant vexpr
    Status fold_constant_vexpr(const TFoldConstantParams& params, PConstantExprResult* response);

private:
    // init runtime_state and mem_tracker
    Status _init(const TQueryGlobals& query_globals, const TQueryOptions& query_options);
    // prepare expr
    template <typename Context>
    Status _prepare_and_open(Context* ctx);

    std::string _get_result(void* src, size_t size, const TypeDescriptor& type,
                            const vectorized::ColumnPtr column_ptr,
                            const vectorized::DataTypePtr column_type);

    std::unique_ptr<RuntimeState> _runtime_state;
    std::unique_ptr<MemTracker> _mem_tracker;
    RuntimeProfile* _runtime_profile = nullptr;
    ObjectPool _pool;
    TUniqueId _query_id;
};
} // namespace doris
