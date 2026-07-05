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

#include "common/check.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {

struct ScanByteBuckets {
    int64_t local_bytes = 0;
    int64_t remote_bytes = 0;
};

inline TFileType::type resolve_file_type_for_scan_bytes(const TFileRangeDesc& range,
                                                        const TFileScanRangeParams& params) {
    if (range.__isset.file_type) {
        return range.file_type;
    }
    DORIS_CHECK(params.__isset.file_type);
    return params.file_type;
}

inline ScanByteBuckets compute_scan_byte_buckets(int64_t read_bytes_delta,
                                                 int64_t cache_local_delta,
                                                 int64_t cache_remote_delta,
                                                 const TFileRangeDesc& range,
                                                 const TFileScanRangeParams& params) {
    DORIS_CHECK_GE(read_bytes_delta, 0);
    DORIS_CHECK_GE(cache_local_delta, 0);
    DORIS_CHECK_GE(cache_remote_delta, 0);

    // Cache statistics describe physical bytes read from each storage source. They can increase
    // without read_bytes_delta, for example when a deletion vector uses DelegateReader directly.
    // Do not use these values to derive FileReadBytes or total ScanBytes: those counters keep the
    // existing FileReaderStats::read_bytes semantics and are not required to equal these buckets.
    if (cache_local_delta > 0 || cache_remote_delta > 0) {
        return ScanByteBuckets {
                .local_bytes = cache_local_delta,
                .remote_bytes = cache_remote_delta,
        };
    }
    if (read_bytes_delta == 0) {
        return {};
    }
    return resolve_file_type_for_scan_bytes(range, params) == TFileType::FILE_LOCAL
                   ? ScanByteBuckets {.local_bytes = read_bytes_delta}
                   : ScanByteBuckets {.remote_bytes = read_bytes_delta};
}

} // namespace doris
