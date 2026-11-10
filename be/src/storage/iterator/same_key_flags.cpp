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

#include "storage/iterator/same_key_flags.h"

#include <glog/logging.h>

#include "core/block/block.h"

namespace doris {

void SameKeyFlags::compute(const Block& block, size_t first_row, std::vector<uint8_t>* flags) {
    const size_t rows = block.rows();
    DCHECK_LT(first_row, rows);
    flags->assign(rows - first_row, 1);
    (*flags)[0] = !_last_key.empty();
    for (size_t i = 0; i < _key_ordinals.size(); ++i) {
        const IColumn& column = *block.get_by_position(_key_ordinals[i]).column;
        if ((*flags)[0] && column.compare_at(first_row, 0, *_last_key[i], -1) != 0) {
            (*flags)[0] = 0;
        }
        for (size_t row = first_row + 1; row < rows; ++row) {
            uint8_t& flag = (*flags)[row - first_row];
            if (flag && column.compare_at(row, row - 1, column, -1) != 0) {
                flag = 0;
            }
        }
    }

    if (_last_key.empty()) {
        for (uint32_t ordinal : _key_ordinals) {
            _last_key.push_back(block.get_by_position(ordinal).column->clone_empty());
        }
    }
    for (size_t i = 0; i < _key_ordinals.size(); ++i) {
        _last_key[i]->clear();
        _last_key[i]->insert_from(*block.get_by_position(_key_ordinals[i]).column, rows - 1);
    }
}

} // namespace doris
