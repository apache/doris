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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/be/src/util/mem-range.h
// and modified by Doris

#pragma once

#include <cstdint>

#include "common/logging.h"

namespace doris {

/// Represents a range of memory. This is a convenient alternative to passing around
/// a separate pointer and length.
class MemRange {
public:
    MemRange(uint8_t* data, int64_t len) : data_(data), len_(len) {
        DCHECK_GE(len, 0);
        DCHECK(len == 0 || data != nullptr);
    }

    uint8_t* data() const { return data_; }
    int64_t len() const { return len_; }

    static MemRange null() { return MemRange(nullptr, 0); }

private:
    uint8_t* data_ = nullptr;
    int64_t len_;
};
} // namespace doris
