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

#include "common/status.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris::index_query {

// The first `count` ids off `nulls`, all below count + |nulls|: a count-only answer the scan's
// null subtraction leaves whole. Fails when the ids would leave the docid domain or the window
// holds fewer than `count` of them, which only a corrupt index causes.
Status fabricate_null_disjoint_count_bitmap(uint64_t count, const roaring::Roaring& nulls,
                                            roaring::Roaring* out);

} // namespace doris::index_query
