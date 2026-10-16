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

#include <cstdint>
#include <hll.hpp>
#include <string_view>

#include "core/custom_allocator.h"
#include "core/string_ref.h"

namespace doris::datasketches_hll {

// Shared by the DataSketches HLL aggregate and scalar functions so that both track memory through
// Doris allocators and report broken input the same way.
using Alloc = CustomStdAllocator<uint8_t>;
using Sketch = datasketches::hll_sketch_alloc<Alloc>;
using Union = datasketches::hll_union_alloc<Alloc>;

// Deserializes a serialized DataSketches HLL sketch. `action` names the caller in the error
// message, e.g. "add" or "read".
//
// Empty bytes are rejected before the library reads them. A doris::Exception keeps its code, so
// MEM_ALLOC_FAILED raised by the allocator is not reported as corrupted data; any other failure of
// the library means the bytes are not a valid sketch and becomes CORRUPTION.
Sketch deserialize(const StringRef& value, std::string_view action);

} // namespace doris::datasketches_hll
