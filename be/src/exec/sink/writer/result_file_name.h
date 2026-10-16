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

#include <fmt/format.h>

#include <string>

namespace doris {

// Builds an output file name of the form "{prefix}{id}_{idx}.{suffix}", shared by
// the SELECT INTO OUTFILE writer (VFileResultWriter) and the INSERT INTO FUNCTION
// TVF writer (VTVFTableWriter).
//
// When pad > 0 the index is zero-padded to `pad` digits so that the files produced
// by a single writer sort lexicographically (e.g. _00011 sorts after _00002 rather
// than before _2). pad <= 0 leaves the index unpadded, which is byte-for-byte the
// historical name format.
inline std::string build_result_file_name(const std::string& prefix, const std::string& id, int idx,
                                          int pad, const std::string& suffix) {
    return fmt::format("{}{}_{:0{}}.{}", prefix, id, idx, pad < 0 ? 0 : pad, suffix);
}

} // namespace doris
