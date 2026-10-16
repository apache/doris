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

#include "core/field.h"

namespace doris {

// Owning non-NaN bounds used by zonemap pruning. Empty, null-only, and NaN-only sets leave both
// Fields null; contains_nan distinguishes the last case.
struct HybridSetMinMax {
    bool contains_nan = false;
    Field min_value;
    Field max_value;
};

} // namespace doris
