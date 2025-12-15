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
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "vec/exec/format/table/table_format_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

struct FieldSchema;

class HiveParquetNestedColumnUtils {
public:
    static void extract_nested_column_ids(const FieldSchema& field_schema,
                                          const std::vector<std::vector<std::string>>& paths,
                                          std::set<uint64_t>& column_ids);
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
