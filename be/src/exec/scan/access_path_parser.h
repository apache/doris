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

#include <vector>

#include "common/status.h"
#include "format_v2/column_data.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

class SlotDescriptor;

class AccessPathParser {
public:
    static Status build_nested_children(format::ColumnDefinition* column,
                                        const SlotDescriptor* slot_desc,
                                        const format::ColumnDefinition* schema_column);

    static Status build_nested_children(format::ColumnDefinition* column,
                                        const std::vector<TColumnAccessPath>& access_paths,
                                        const format::ColumnDefinition* schema_column);
};

} // namespace doris
