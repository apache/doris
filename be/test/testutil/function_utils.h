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

#include <memory>
#include <vector>

#include "runtime/types.h"
#include "testutil/mock/mock_runtime_state.h"
#include "vec/data_types/data_type.h"

namespace doris {

class FunctionContext;

class FunctionUtils {
public:
    FunctionUtils(const vectorized::DataTypePtr& return_type,
                  const std::vector<vectorized::DataTypePtr>& arg_types, bool enable_strict_cast);

    doris::FunctionContext* get_fn_ctx() { return _fn_ctx.get(); }

private:
    std::unique_ptr<MockRuntimeState> _state;
    std::unique_ptr<doris::FunctionContext> _fn_ctx;
};

} // namespace doris
