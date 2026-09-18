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

#include "common/status.h"
#include "storage/index/snii/encoding/byte_sink.h"
#include "storage/index/snii/encoding/byte_source.h"

namespace doris::snii {

// PFOR integer block encoder/decoder (unsigned uint32 array).
// Encoded layout: [u8 bit_width][varint n_exceptions][bit-packed low
// bits][exception table]. Selects the bit_width that minimizes total byte size;
// values exceeding it go into the exception table (index_delta, full_value).
// delta/zigzag is handled by the upper layer (.frq window); PFOR only processes
// unsigned integer arrays.
void pfor_encode(const uint32_t* values, size_t n, ByteSink* out);
Status pfor_decode(ByteSource* src, size_t n, uint32_t* out);
Status pfor_skip(ByteSource* src, size_t n);

} // namespace doris::snii

// Test-only instrumentation seam (mirrors the dict-block decode-counter pattern).
// pfor_width_evals() returns a process-global count of per-value bit-width
// evaluations performed by pfor_encode since the last reset -- one per
// value_width() call, the single evaluation point on the encode path. Deterministic
// perf tests assert it equals the number of encoded values per run, proving the
// histogram path scans each value exactly once (vs the former O(maxw*n) re-scan).
// Compiled to a no-op in non-test builds; reset between tests.
namespace doris::snii::testing {

uint64_t pfor_width_evals();
void reset_pfor_width_evals();

} // namespace doris::snii::testing
