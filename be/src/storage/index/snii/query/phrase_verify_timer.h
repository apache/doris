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

#include <chrono>
#include <cstdint>

#include "storage/index/snii/format/prx_decode_stats.h"

namespace doris::snii::query::internal {

uint64_t exclusive_phrase_verify_ns(uint64_t elapsed_ns, uint64_t decode_ns_before,
                                    uint64_t decode_ns_after);

class PhraseVerifyTimer {
public:
    explicit PhraseVerifyTimer(format::PrxDecodeContext* decode_context);

    void commit_success();

private:
    format::PrxDecodeStats* stats_ = nullptr;
    std::chrono::steady_clock::time_point start_;
    uint64_t decode_ns_before_ = 0;
};

#ifdef BE_TEST
namespace testing {

uint64_t phrase_verify_clock_read_count();
void reset_phrase_verify_clock_read_count();
void note_phrase_verify_clock_read();

} // namespace testing
#endif
} // namespace doris::snii::query::internal
