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

#include "storage/index/inverted/spimi/lucene_output.h"

namespace lucene::store {
class IndexOutput;
}

namespace doris::segment_v2::inverted_index::spimi {

// Bridges SPIMI's `LuceneOutput` interface to a CLucene
// `lucene::store::IndexOutput`. The SPIMI writer stack stays portable
// (no link-time dependency on CLucene's writer-side classes) by going
// through this adapter when the segment-emit phase needs to land bytes
// in a `DorisFSDirectory` (which already wraps `io::FileWriter` inside
// CLucene's IndexOutput abstraction).
//
// The adapter is non-owning: the caller closes / deletes the underlying
// IndexOutput. Errors thrown by the CLucene write path propagate as
// CLuceneError exceptions, matching how the existing writer reports
// I/O failures.
class IndexOutputLuceneOutput final : public LuceneOutput {
public:
    explicit IndexOutputLuceneOutput(lucene::store::IndexOutput* out);

    void WriteByte(uint8_t b) override;
    void WriteBytes(const uint8_t* b, size_t len) override;
    int64_t FilePointer() const override;

private:
    lucene::store::IndexOutput* _out;
};

} // namespace doris::segment_v2::inverted_index::spimi
