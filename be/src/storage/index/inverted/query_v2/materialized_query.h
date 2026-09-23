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

#include <roaring/roaring.hh>

namespace doris::segment_v2::inverted_index::query_v2 {

// A query whose rows are known before it runs: the rows it matches and, when its field has
// NULLs, the rows it leaves UNKNOWN.
class MaterializedQuery {
public:
    virtual ~MaterializedQuery() = default;

    virtual const roaring::Roaring& rows() const = 0;
    virtual const roaring::Roaring* null_rows() const = 0;
};

} // namespace doris::segment_v2::inverted_index::query_v2
