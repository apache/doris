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
#include <vector>

#include "common/status.h"
#include "core/string_ref.h"

namespace doris {

class IColumn;

// 已解码 column batch 的物理值来源类型。
// 该枚举只描述通用内存布局，不包含 Parquet/ORC/Arrow 等格式专有类型。
enum class DecodedValueKind {
    BOOL,
    INT32,
    INT64,
    FLOAT,
    DOUBLE,
    BINARY,
    FIXED_BINARY,
};

enum class DecodedTimeUnit {
    UNKNOWN,
    MILLIS,
    MICROS,
    NANOS,
};

struct DecodedColumnView {
    DecodedValueKind value_kind = DecodedValueKind::INT32;
    DecodedTimeUnit time_unit = DecodedTimeUnit::UNKNOWN;
    int64_t row_count = 0;
    int decimal_precision = -1;
    int decimal_scale = -1;
    int fixed_length = -1;
    const uint8_t* values = nullptr;
    const uint8_t* null_map = nullptr;
    const std::vector<StringRef>* binary_values = nullptr;
};

} // namespace doris
