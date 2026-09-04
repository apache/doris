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

#include <cstddef>

#include "core/column/column_string.h"
#include "core/string_ref.h"

namespace doris {

struct JsonbValue;

// Returns the first sorted sparse path in [start, end) that is not less than path.
size_t find_variant_sparse_path_lower_bound(StringRef path, const ColumnString& sparse_paths,
                                            size_t start, size_t end);

// True when a JSONB value recursively contains no visible scalar payload. Variant V1 and V2
// readers share this rule when deciding whether a materialized nested value contributes a path.
bool is_variant_jsonb_value_semantically_empty(const JsonbValue* value);

} // namespace doris
