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

namespace doris_udf {
class FunctionContext;
}

namespace doris {

class MemPool;
class MemTracker;
class RuntimeState;

class FunctionUtils {
public:
    FunctionUtils();
    ~FunctionUtils();

    doris_udf::FunctionContext* get_fn_ctx() {
        return _fn_ctx;
    }
private:
    RuntimeState* _state = nullptr;
    MemTracker* _mem_tracker = nullptr;
    MemPool* _memory_pool = nullptr;
    doris_udf::FunctionContext* _fn_ctx = nullptr;
};

}
