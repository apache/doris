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

#include "storage/index/query/spi/io_read_batch.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/file_reader.h"

namespace doris::snii::io {

// Exposes the shared read batch through SNII's existing byte-view interface.
class BatchRangeFetcher : public index_query::IoReadBatch {
public:
    using IoReadBatch::IoReadBatch;

    Slice get(size_t handle) const {
        const auto bytes = IoReadBatch::get(handle);
        return {bytes.data(), bytes.size()};
    }
};

} // namespace doris::snii::io
