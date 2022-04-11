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

#include "common/status.h"

namespace doris {

// The implementations may contains buffers or local caches
class WriteStream {
public:
    WriteStream() = default;
    virtual ~WriteStream() = default;

    // Appends multiple slices of data referenced by 'data' to the stream.
    //
    // Does not guarantee durability of 'data'; close() must be called for all
    // outstanding data to reach the disk.
    virtual Status write(const char* from, size_t n) = 0;

    virtual Status sync() = 0;

    virtual Status close() = 0;
};

} // namespace doris
