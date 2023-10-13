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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/DataStreams/materializeBlock.cpp
// and modified by Doris

#include "vec/core/materialize_block.h"

#include <stddef.h>

#include "vec/columns/column.h"
#include "vec/core/column_with_type_and_name.h"

namespace doris::vectorized {

void materialize_block_inplace(Block& block) {
    for (size_t i = 0; i < block.columns(); ++i) {
        block.replace_by_position_if_const(i);
    }
}

} // namespace doris::vectorized
