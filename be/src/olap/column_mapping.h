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

#include <gen_cpp/Exprs_types.h>

#include <memory>

#include "olap/tablet_schema.h"
namespace doris {

class WrapperField;

struct ColumnMapping {
    ColumnMapping() = default;
    virtual ~ColumnMapping() = default;

    bool has_reference() const { return expr != nullptr || ref_column >= 0; }

    // <0: use default value
    // >=0: use origin column
    int32_t ref_column = -1;
    // normally for default value. stores values for filters
    WrapperField* default_value = nullptr;
    std::shared_ptr<TExpr> expr;
    const TabletColumn* new_column = nullptr;
};

using SchemaMapping = std::vector<ColumnMapping>;

} // namespace doris
