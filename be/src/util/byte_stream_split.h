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

namespace doris {

/**
 * @brief Decode a byte stream into a byte stream split format.
 * 
 * @param src The encoded data by byte stream split.
 * @param width The width of type.
 * @param offset The offset of encoded data.
 * @param num_values The num of values to decode.
 * @param stride The length of each stream.
 * @param dest The buffer to store the decoded data.
 */
void byte_stream_split_decode(const uint8_t* src, int width, int64_t offset, int64_t num_values,
                              int64_t stride, uint8_t* dest);

} // namespace doris
