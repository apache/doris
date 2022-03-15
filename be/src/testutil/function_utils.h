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

#include "udf/udf.h"

namespace doris {

class MemPool;
class RuntimeState;

class FunctionUtils {
public:
    FunctionUtils();
    FunctionUtils(RuntimeState* state);
    FunctionUtils(const doris_udf::FunctionContext::TypeDesc& return_type,
                  const std::vector<doris_udf::FunctionContext::TypeDesc>& arg_types,
                  int varargs_buffer_size);
    ~FunctionUtils();

    doris_udf::FunctionContext* get_fn_ctx() { return _fn_ctx; }

private:
    RuntimeState* _state = nullptr;
    MemPool* _memory_pool = nullptr;
    doris_udf::FunctionContext* _fn_ctx = nullptr;
};

} // namespace doris
