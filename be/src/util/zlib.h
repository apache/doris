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

#include <iosfwd>

#include "common/status.h"
#include "util/slice.h"

namespace doris {
namespace zlib {

// Zlib-compress the data in 'input', appending the result to 'out'.
//
// In case of an error, some data may still be appended to 'out'.
Status Compress(Slice input, std::ostream* out);

// The same as the above, but with a custom level (1-9, where 1 is fastest
// and 9 is best compression).
Status CompressLevel(Slice input, int level, std::ostream* out);

// Uncompress the zlib-compressed data in 'compressed', appending the result
// to 'out'.
//
// In case of an error, some data may still be appended to 'out'.
Status Uncompress(Slice compressed, std::ostream* out);

} // namespace zlib
} // namespace doris
