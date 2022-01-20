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

#include "vec/common/string_utils/string_utils.h"

namespace doris::vectorized {

class ReadBuffer {
public:
    ReadBuffer(char* d, size_t n) :
         _start(d), _end(d + n) {}

    ReadBuffer(const unsigned char* d, size_t n) :
         _start((char*)(d)), _end((char*)(d) + n) {}

    bool eof() { return _start == _end; }

    char*& position() {
        return _start;
    }

    char* end() { return _end; }

    size_t count() { return _end - _start; }
private:
    char* _start;
    char* _end;
};

}
