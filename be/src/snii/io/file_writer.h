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

#include "snii/common/slice.h"
#include "common/status.h"

namespace snii::io {

// Append-only writer (no seek-back), so the format can be produced in a single
// streaming pass compatible with S3FileWriter / StreamSinkFileWriter / packed
// writer. All container bytes are written front-to-back; back-references are
// resolved by writing metadata last.
class FileWriter {
public:
    virtual ~FileWriter() = default;

    virtual doris::Status append(Slice data) = 0;
    virtual doris::Status finalize() = 0;
    virtual uint64_t bytes_written() const = 0;
};

} // namespace snii::io
