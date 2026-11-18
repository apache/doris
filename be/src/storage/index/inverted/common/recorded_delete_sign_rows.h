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

#include <functional>
#include <memory>
#include <mutex>

#include "common/status.h"

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris::segment_v2 {

// The rows written with the delete sign that a merge-on-write text index records
// (IndexColumnWriter::set_delete_sign_rows), kept with the index's reader in the searcher cache
// as the norm length sums are, so merge-on-write BM25 statistics read them once per index rather
// than read the delete-sign column for every query. A few bytes for most segments: a roaring
// bitmap of the rows a CDC stream deleted.
class RecordedDeleteSignRows {
public:
    // Sets `out` to the rows, or to null when the index records none: it was written by an older
    // BE, or merged by an older BE's index compaction. `read` reads them the first time.
    using Read = std::function<Status(std::shared_ptr<const roaring::Roaring>* out)>;
    Status get(const Read& read, std::shared_ptr<const roaring::Roaring>* out) {
        // Queries of the index that need the rows meanwhile wait for this read, not read them too.
        std::lock_guard lock(_mutex);
        if (!_read) {
            RETURN_IF_ERROR(read(&_rows));
            _read = true;
        }
        *out = _rows;
        return Status::OK();
    }

private:
    std::mutex _mutex;
    bool _read = false;
    std::shared_ptr<const roaring::Roaring> _rows;
};

} // namespace doris::segment_v2
