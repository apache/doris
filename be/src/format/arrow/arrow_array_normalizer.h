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

#include <arrow/type_fwd.h>

#include <memory>

#include "common/status.h"

namespace doris {

/// DataTypeSerDe::read_column_from_arrow was written for the Arrow that Doris itself emits, so it
/// accepts only a subset of the Arrow type variants. Third-party ADBC drivers emit others: DuckDB
/// emits string_view, Go-based drivers may emit large_* and dictionary. This normalizes them into
/// a shape the serdes accept.
///
/// Only top-level types are normalized. A nested type whose child is an unaccepted variant (say
/// list<large_utf8>) passes through and fails inside the serde, loudly rather than silently.

/// Whether the serdes take this Arrow type as-is.
bool is_serde_acceptable_arrow_type(const arrow::DataType& type);

/// Normalizes `arr` into a serde-acceptable shape using `pool` for every conversion. Returns it
/// unchanged (no copy) when it already is one, and fails with the offending type named when no
/// accepted shape exists.
Status normalize_arrow_array(const std::shared_ptr<arrow::Array>& arr, arrow::MemoryPool* pool,
                             std::shared_ptr<arrow::Array>* out);

} // namespace doris
