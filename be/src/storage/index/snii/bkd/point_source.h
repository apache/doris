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
#include "storage/index/snii/common/slice.h"

// Supplies ordered points to the shared leaf writer from memory or merged spill runs.
namespace doris::snii::bkd {

// Forward-only source of records ordered by (value, doc_id). Disk refill failures are returned through Status.
class PointSource {
public:
    virtual ~PointSource() = default;

    PointSource(const PointSource&) = delete;
    PointSource& operator=(const PointSource&) = delete;

    // Hands back the next run of at most `max_points` CONSECUTIVE records as one
    // contiguous view, which is exactly the shape encode_leaf_block consumes -- no
    // per-leaf PointRef array is ever materialized.
    //
    // The view is owned by the source and stays valid only until the next call.
    // Fewer than `max_points` records come back only when the stream runs out; an
    // EMPTY slice means exhausted, and every later call returns empty again.
    virtual Status next_block(uint32_t max_points, Slice* records) = 0;

protected:
    PointSource() = default;
};

} // namespace doris::snii::bkd
