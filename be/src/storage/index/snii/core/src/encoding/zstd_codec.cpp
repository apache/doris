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

#include "snii/encoding/zstd_codec.h"

#include <zstd.h>

#include <string>

namespace snii {

doris::Status zstd_compress(Slice input, int level, std::vector<uint8_t>* out) {
    size_t bound = ZSTD_compressBound(input.size());
    out->resize(bound);
    size_t n = ZSTD_compress(out->data(), bound, input.data(), input.size(), level);
    if (ZSTD_isError(n)) {
        return doris::Status::Error<doris::ErrorCode::INTERNAL_ERROR, false>(std::string("zstd compress: ") + ZSTD_getErrorName(n));
    }
    out->resize(n);
    return doris::Status::OK();
}

doris::Status zstd_decompress(Slice input, size_t expected_uncomp_len, std::vector<uint8_t>* out) {
    out->resize(expected_uncomp_len);
    size_t n = ZSTD_decompress(out->data(), expected_uncomp_len, input.data(), input.size());
    if (ZSTD_isError(n)) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>(std::string("zstd decompress: ") + ZSTD_getErrorName(n));
    }
    if (n != expected_uncomp_len) {
        return doris::Status::Error<doris::ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("zstd decompressed length mismatch");
    }
    return doris::Status::OK();
}

} // namespace snii
