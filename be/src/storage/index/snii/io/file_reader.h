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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "common/status.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/io/io_metrics.h"

namespace doris::snii::io {

// One logical read request (offset, length).
struct Range {
    uint64_t offset = 0;
    size_t len = 0;
};

// The single physical-read primitive (a BE-internal read_at). All higher layers
// route reads through this so I/O can be accounted and backed by local files or
// object storage interchangeably.
class FileReader {
public:
    virtual ~FileReader() = default;

    // Reads exactly len bytes starting at offset into *out (which is resized to
    // len). Reading past EOF is an error (Corruption/IoError).
    virtual Status read_at(uint64_t offset, size_t len, std::vector<uint8_t>* out) = 0;

    // Reads a batch of ranges that may be served concurrently. The default is a
    // sequential loop; backends that model concurrency (MeteredFileReader) or
    // perform real parallel fetches (object storage) override this.
    virtual Status read_batch(const std::vector<Range>& ranges,
                              std::vector<std::vector<uint8_t>>* outs) {
        outs->resize(ranges.size());
        for (size_t i = 0; i < ranges.size(); ++i) {
            RETURN_IF_ERROR(read_at(ranges[i].offset, ranges[i].len, &(*outs)[i]));
        }
        return Status::OK();
    }

    // Total size of the underlying object in bytes.
    virtual uint64_t size() const = 0;

    // Optional live metrics. Readers that do not account I/O return nullptr.
    virtual const IoMetrics* io_metrics() const { return nullptr; }
};

} // namespace doris::snii::io
