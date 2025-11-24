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

#include "vec/runtime/vector_search_user_params.h"

#include <fmt/format.h>

namespace doris {
#include "common/compile_check_begin.h"
bool VectorSearchUserParams::operator==(const VectorSearchUserParams& other) const {
    return hnsw_ef_search == other.hnsw_ef_search &&
           hnsw_check_relative_distance == other.hnsw_check_relative_distance &&
           hnsw_bounded_queue == other.hnsw_bounded_queue;
}

std::string VectorSearchUserParams::to_string() const {
    return fmt::format(
            "hnsw_ef_search: {}, hnsw_check_relative_distance: {}, "
            "hnsw_bounded_queue: {}",
            hnsw_ef_search, hnsw_check_relative_distance, hnsw_bounded_queue);
}
} // namespace doris