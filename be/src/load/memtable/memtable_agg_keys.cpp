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

#include "load/memtable/memtable_agg_keys.h"

#include "common/config.h"

namespace doris {

bool AggKeysMemTable::need_agg() const {
    auto max_size = _last_agg_pos + config::write_buffer_size_for_agg;
    return memory_usage() >= max_size;
}

void AggKeysMemTable::_aggregate_flexible_partial_update(
        bool, MutableBlock&, DorisVector<std::shared_ptr<RowInBlock>>&) {
    // Flexible partial update is a UNIQUE_KEYS feature: only UniqueKeysMemTable sets
    // _skip_bitmap_col_idx, which is the only way to reach this hook.
    DORIS_CHECK(false);
}

} // namespace doris
