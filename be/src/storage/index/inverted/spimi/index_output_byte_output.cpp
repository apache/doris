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

#include "storage/index/inverted/spimi/index_output_byte_output.h"

#include <CLucene.h> // IWYU pragma: keep
#include <CLucene/store/IndexOutput.h>

namespace doris::segment_v2::inverted_index::spimi {

IndexOutputByteOutput::IndexOutputByteOutput(lucene::store::IndexOutput* out) : _out(out) {}

void IndexOutputByteOutput::WriteByte(uint8_t b) {
    _out->writeByte(b);
}

void IndexOutputByteOutput::WriteBytes(const uint8_t* b, size_t len) {
    _out->writeBytes(b, static_cast<int32_t>(len));
}

int64_t IndexOutputByteOutput::FilePointer() const {
    return _out->getFilePointer();
}

} // namespace doris::segment_v2::inverted_index::spimi
