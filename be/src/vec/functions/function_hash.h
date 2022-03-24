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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/FunctionHash.h
// and modified by Doris

#pragma once

#include <utility>

#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/bit_cast.h"
#include "vec/common/hash_table/hash.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_number.h"
#include "vec/functions/function.h"
#include "vec/functions/function_helpers.h"

namespace doris::vectorized {

struct IntHash64Impl {
    using ReturnType = UInt64;

    static UInt64 apply(UInt64 x) { return int_hash64(x ^ 0x4CF2D2BAAE6DA887ULL); }
};
} // namespace doris::vectorized
