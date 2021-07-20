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

#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "exprs/expr_context.h"
#include "exprs/expr.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "runtime/exec_env.h"
#include "gen_cpp/internal_service.pb.h"
#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

class TFoldConstantParams;
class TExpr;
class TQueryGlobals;

// This class used to fold constant expr from fe
class FoldConstantMgr {
public:
    FoldConstantMgr(ExecEnv* exec_env);
    // fold constant expr 
    Status fold_constant_expr(const TFoldConstantParams& params, PConstantExprResult* response);
    // init runtime_state and mem_tracker
    Status init(TQueryGlobals query_globals);
    // prepare expr
    Status prepare_and_open(ExprContext* ctx);

    std::string get_result(void* src, PrimitiveType slot_type);

private:
    std::unique_ptr<RuntimeState> _runtime_state;
    std::shared_ptr<MemTracker> _mem_tracker;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<MemPool> _mem_pool;
    ExecEnv* _exec_env;
    ObjectPool _pool;
    static TUniqueId _dummy_id;
};
}

