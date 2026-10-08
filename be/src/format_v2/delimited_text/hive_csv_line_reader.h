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

#include "format/file_reader/new_plain_text_line_reader.h"

namespace doris::format::csv {

class HiveCsvLineReaderCtx final : public TextLineReaderContextIf {
public:
    const uint8_t* read_line(const uint8_t* start, size_t len) override;
    size_t line_delimiter_length() const override { return _delimiter_length; }
    void refresh() override {}

private:
    size_t _delimiter_length = 1;
};

} // namespace doris::format::csv
