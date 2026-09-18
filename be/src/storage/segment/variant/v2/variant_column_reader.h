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

#include "common/status.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"

namespace doris {
class ColumnVariantV2;
}

namespace doris::segment_v2::variant_v2 {

ColumnVariantV2* try_get_variant_v2_destination(IColumn& column);

// Publishes an assembled Nullable<ColumnVariantV2> batch into the scan destination. An empty
// nullable destination adopts the complete wrapper, while an empty non-nullable destination adopts
// its nested ColumnVariantV2 without cloning. Later batches append with normal COW detachment.
Status append_assembled_variant(MutableColumnPtr& dst, ColumnNullable::MutablePtr&& assembled);

} // namespace doris::segment_v2::variant_v2
