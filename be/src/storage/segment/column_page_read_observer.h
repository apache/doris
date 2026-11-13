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

#include "io/fs/file_range_coalescer.h"

namespace doris::io {
class ReadAheadRangeBuffer;
}

namespace doris::segment_v2 {

struct ColumnPageReadContext {
    io::ReadAheadRangeBuffer* buffer {nullptr};
    bool retry_on_error {false};
};

/// Synchronous interception of a physical data-page load, including loads inside next_batch().
/// The enclosing iterator owns the observer and detaches it before destroying the inner iterator.
/// No worker retains or invokes this observer.
class ColumnPageReadObserver {
public:
    virtual ~ColumnPageReadObserver() = default;
    virtual ColumnPageReadContext before_page_read(int32_t page_index, io::FileRange range) {
        return {};
    }
    virtual void after_page_read(int32_t page_index, io::FileRange range) {}
};

} // namespace doris::segment_v2
