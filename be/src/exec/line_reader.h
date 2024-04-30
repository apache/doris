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

#include "common/factory_creator.h"
#include "common/status.h"
#include "util/profile_collector.h"

namespace doris {
namespace io {
struct IOContext;
}
// This class is used to read content line by line
class LineReader : public ProfileCollector {
public:
    virtual ~LineReader() = default;
    virtual Status read_line(const uint8_t** ptr, size_t* size, bool* eof,
                             const io::IOContext* io_ctx) = 0;

    virtual void close() = 0;
};

} // namespace doris
