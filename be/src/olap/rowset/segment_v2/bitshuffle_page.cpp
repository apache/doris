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

#include "olap/rowset/segment_v2/bitshuffle_page.h"

#include "olap/rowset/segment_v2/common.h"

namespace doris {

namespace segment_v2 {

void warn_with_bitshuffle_error(int64_t val) {
    switch (val) {
    case -1:
        LOG(WARNING) << "Failed to allocate memory";
        break;
    case -11:
        LOG(WARNING) << "Missing SSE";
        break;
    case -12:
        LOG(WARNING) << "Missing AVX";
        break;
    case -80:
        LOG(WARNING) << "Input size not a multiple of 8";
        break;
    case -81:
        LOG(WARNING) << "block_size not multiple of 8";
        break;
    case -91:
        LOG(WARNING) << "Decompression error, wrong number of bytes processed";
        break;
    default:
        LOG(WARNING) << "Error internal to compression routine";
    }
}

} // namespace segment_v2

} // namespace doris
