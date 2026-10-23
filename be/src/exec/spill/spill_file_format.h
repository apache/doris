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

namespace doris {

// Each completed part ends in [block offsets][max sub-block size][block count]
// [little-endian format version][magic]. The first three fields retain their
// native size_t encoding. Unmarked older parts are intentionally not readable.
inline constexpr char kSpillPartMagic[] = {'D', 'S', 'P', 'L'};
inline constexpr uint32_t kSpillPartFormatVersion = 1;
inline constexpr size_t kSpillPartFormatBytes = sizeof(uint32_t) + sizeof(kSpillPartMagic);

} // namespace doris
