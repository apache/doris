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
#include <memory>
#include <vector>

namespace doris {

struct OlapScanRange;

// Groups exact, single-VARCHAR scan keys by the table's HASH distribution bucket.
// The caller must establish that this is the table's sole non-null storage and
// distribution key. Ranges remain owned by the scan local state.
class ScanKeyBucketPruner {
public:
    // Returns false for non-point ranges, including ranges coalesced by max_scan_key_num.
    bool init(const std::vector<std::unique_ptr<OlapScanRange>>& ranges, int32_t bucket_num);

    const std::vector<OlapScanRange*>& ranges_for_bucket(int32_t bucket_seq) const;

private:
    std::vector<std::vector<OlapScanRange*>> _bucket_ranges;
};

} // namespace doris
