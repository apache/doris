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

#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/Types_types.h>

#include <vector>

#include "common/status.h"
#include "exec/schema_scanner.h"

namespace doris {
class RuntimeState;

namespace vectorized {
class Block;
} // namespace vectorized

class SchemaVariablesScanner : public SchemaScanner {
    ENABLE_FACTORY_CREATOR(SchemaVariablesScanner);

public:
    SchemaVariablesScanner(TVarType::type type);
    ~SchemaVariablesScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next_block_internal(vectorized::Block* block, bool* eos) override;

private:
    struct VariableStruct {
        const char* name = nullptr;
        const char* value = nullptr;
    };

    Status _fill_block_impl(vectorized::Block* block);

    static std::vector<SchemaScanner::ColumnDesc> _s_vars_columns;

    TShowVariableResult _var_result;
    TVarType::type _type;
};

} // namespace doris
