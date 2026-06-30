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
#include <vector>

namespace doris::snii::query::internal {

std::vector<uint32_t> intersect_sorted(const std::vector<uint32_t>& a,
                                       const std::vector<uint32_t>& b);

void union_sorted_into(std::vector<uint32_t>* acc, const std::vector<uint32_t>& next);

std::vector<uint32_t> union_sorted_many(const std::vector<std::vector<uint32_t>>& lists);

} // namespace doris::snii::query::internal
