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

#include <functional>
#include <memory>
#include <string>
#include <unordered_map>

#include "common/status.h"

namespace doris {
class ObjectPool;

namespace vectorized {
class TableFunction;

class TableFunctionFactory {
public:
    TableFunctionFactory() = delete;
    static Status get_fn(const TFunction& t_fn, ObjectPool* pool, TableFunction** fn);

    const static std::unordered_map<std::string, std::function<std::unique_ptr<TableFunction>()>>
            _function_map;
};
} // namespace vectorized
} // namespace doris
