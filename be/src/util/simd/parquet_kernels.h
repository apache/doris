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

namespace doris::simd {

enum class RawComparisonOp : uint8_t { EQ, NE, LT, LE, GT, GE };

void byte_stream_split_decode(const uint8_t* src, size_t width, size_t offset, size_t num_values,
                              size_t stride, uint8_t* dest);

void delta_decode(int32_t* values, size_t count, int32_t min_delta, int32_t* last_value);
void delta_decode(int64_t* values, size_t count, int64_t min_delta, int64_t* last_value);

void dictionary_gather(const uint8_t* dictionary, const uint32_t* indices, size_t count,
                       size_t value_width, uint8_t* dest);

void expand_nullable_values(uint8_t* values, size_t compact_count, const uint8_t* nulls,
                            size_t output_count, size_t value_width);

void raw_compare(const uint8_t* values, size_t count, int32_t literal, RawComparisonOp op,
                 uint8_t* matches);
void raw_compare(const uint8_t* values, size_t count, int64_t literal, RawComparisonOp op,
                 uint8_t* matches);
void raw_compare(const uint8_t* values, size_t count, float literal, RawComparisonOp op,
                 uint8_t* matches);
void raw_compare(const uint8_t* values, size_t count, double literal, RawComparisonOp op,
                 uint8_t* matches);

} // namespace doris::simd
