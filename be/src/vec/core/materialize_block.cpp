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

#include "vec/core/materialize_block.h"
namespace doris::vectorized {

Block materialize_block(const Block& block) {
    if (!block) return block;

    Block res = block;
    size_t columns = res.columns();
    for (size_t i = 0; i < columns; ++i) {
        auto& element = res.get_by_position(i);
        element.column = element.column->convert_to_full_column_if_const();
    }

    return res;
}

void materialize_block_inplace(Block& block) {
    for (size_t i = 0; i < block.columns(); ++i) {
        block.replace_by_position_if_const(i);
    }
}

} // namespace doris::vectorized
