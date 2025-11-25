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

#include "common/status.h"
#include "udf/python/python_udf_client.h"
#include "udf/python/python_udf_meta.h"
#include "udf/python/python_udf_runtime.h"

namespace doris {

class PythonUDAFClient;
using PythonUDAFClientPtr = std::shared_ptr<PythonUDAFClient>;

class PythonUDFServerManager {
public:
    PythonUDFServerManager() = default;

    ~PythonUDFServerManager() = default;

    static PythonUDFServerManager& instance() {
        static PythonUDFServerManager instance;
        return instance;
    }

    Status init(const std::vector<PythonVersion>& versions);

    template <typename T>
    Status get_client(const PythonUDFMeta& func_meta, const PythonVersion& version,
                      std::shared_ptr<T>* client);

    Status fork(PythonUDFProcessPool* pool, ProcessPtr* process);

    void shutdown();

private:
    std::unordered_map<PythonVersion, PythonUDFProcessPoolPtr> _pools;
    // protect _pools
    std::mutex _pools_mutex;
};

} // namespace doris