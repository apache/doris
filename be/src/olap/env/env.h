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
#include "util/slice.h"

namespace doris {

class RandomAccessFile {
public:
    RandomAccessFile() { }
    virtual ~RandomAccessFile() { }

    // Read "result.size" bytes from the file starting at "offset".
    // Copies the resulting data into "result.data".
    // 
    // If an error was encountered, returns a non-OK status.
    //
    // This method will internally retry on EINTR and "short reads" in order to
    // fully read the requested number of bytes. In the event that it is not
    // possible to read exactly 'length' bytes, an IOError is returned.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status read_at(uint64_t offset, const Slice& result) const = 0;

    // Reads up to the "results" aggregate size, based on each Slice's "size",
    // from the file starting at 'offset'. The Slices must point to already-allocated
    // buffers for the data to be written to.
    // 
    // If an error was encountered, returns a non-OK status.
    //
    // This method will internally retry on EINTR and "short reads" in order to
    // fully read the requested number of bytes. In the event that it is not
    // possible to read exactly 'length' bytes, an IOError is returned.
    //
    // Safe for concurrent use by multiple threads.
    virtual Status read_at(uint64_t offset, const std::vector<Slice>& result) const = 0;

    // Return the size of this file
    virtual Status size(uint64_t* size) const = 0;

    // Return name of this file
    virtual std::string file_name() const = 0;
};

}
