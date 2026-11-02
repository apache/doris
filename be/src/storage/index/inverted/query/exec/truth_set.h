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
#include <roaring/roaring.hh>
#include <utility>

namespace doris::index_query {

// TRUE and UNKNOWN rows are complete and disjoint within one document space.
struct TruthSet {
    roaring::Roaring true_rows;
    roaring::Roaring null_rows;

    void intersect_with(const TruthSet& other) {
        // Without UNKNOWN rows on either side, AND keeps the rows both hold TRUE.
        if (null_rows.isEmpty() && other.null_rows.isEmpty()) {
            true_rows &= other.true_rows;
            return;
        }
        auto possible = true_rows | null_rows;
        possible &= other.true_rows | other.null_rows;
        true_rows &= other.true_rows;
        null_rows = std::move(possible);
        null_rows -= true_rows;
    }

    void union_with(const TruthSet& other) {
        true_rows |= other.true_rows;
        null_rows |= other.null_rows;
        null_rows -= true_rows;
    }

    void negate(uint32_t row_count) {
        roaring::Roaring complement;
        complement.addRange(0, row_count);
        complement -= true_rows;
        complement -= null_rows;
        true_rows = std::move(complement);
    }
};

} // namespace doris::index_query
