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

#include "common/status.h"

namespace doris::io {

class Compressor {
public:
    virtual ~Compressor() = default;
    virtual Status init() = 0;
    virtual Status set_input(const char* data, size_t length) = 0;
    virtual bool need_input() = 0;
    virtual Status compress(char* buffer, size_t length, size_t& compressed_length) = 0;
    virtual size_t get_bytes_read() = 0;
    virtual size_t get_bytes_written() = 0;
    virtual void finish() = 0;
    virtual bool finished() = 0;
    virtual Status reset() = 0;
};
} // namespace doris::io