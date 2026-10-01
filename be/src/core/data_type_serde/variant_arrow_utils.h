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

#include "common/status.h"
#include "core/value/variant/variant_batch_builder.h"

namespace doris {

// Import only values obtained from validated ColumnVariantV2 storage. Depth includes any
// enclosing legacy containers so all native Flight paths enforce the same nesting limit.
Status append_flight_variant_value(VariantRef value, VariantBatchBuilder::Row& output,
                                   size_t depth = 0);

} // namespace doris
