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

#include "core/column/column.h"
#include "core/data_type/data_type.h"

namespace doris {

/// Whether `type` is FLOAT/DOUBLE or a nested type (array/map/struct) holding one.
/// Nullable wrappers are looked through.
bool contains_float_or_double(const DataTypePtr& type);

/// Hash tables, fixed/serialized keys and the shuffle partitioners compare floating point
/// keys by their raw bits, while Doris equality treats -0.0 == +0.0 and all NaN payloads as
/// one value. Collapse such values (see NormalizeFloat) so equal keys always hash and compare
/// equal. Columns without a floating point leaf are left untouched and never copied; a float
/// column is normalized in place when `column` is its only owner and replaced by a normalized
/// copy otherwise. Operators pass a second reference to the block's column, so the block keeps
/// the stored row values and only the copy they hash is canonical.
void normalize_float_hash_key(ColumnPtr& column, const DataTypePtr& type);

} // namespace doris
