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

#include "format_v2/delimited_text/hive_csv_line_reader.h"

namespace doris::format::csv {

const uint8_t* HiveCsvLineReaderCtx::read_line(const uint8_t* start, size_t len) {
    for (size_t i = 0; i < len; ++i) {
        if (start[i] == '\n') {
            _delimiter_length = 1;
            return start + i;
        }
        if (start[i] == '\r') {
            // Wait for lookahead when CR straddles input buffers. At EOF the parser removes it.
            if (i + 1 == len) {
                return nullptr;
            }
            _delimiter_length = start[i + 1] == '\n' ? 2 : 1;
            return start + i;
        }
    }
    return nullptr;
}

} // namespace doris::format::csv
