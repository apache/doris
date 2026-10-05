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

namespace doris {
class Block;
class TabletSchema;
class IOlapColumnDataAccessor;
class OlapBlockDataConvertor;

namespace segment_v2 {

// Helpers shared by more than one block transform stage.

// Converts the key columns [0, num_key_columns) of `block` to OLAP data accessors.
Status convert_key_columns(OlapBlockDataConvertor& convertor, const TabletSchema& schema,
                           const Block& block, size_t num_rows,
                           std::vector<IOlapColumnDataAccessor*>& key_columns);

// Converts the sequence column at input position `src_pos` to an OLAP data accessor.
Status convert_seq_column(OlapBlockDataConvertor& convertor, const TabletSchema& schema,
                          const Block& block, size_t src_pos, size_t num_rows,
                          IOlapColumnDataAccessor*& seq_column);

// Widens a narrow fixed partial-update block to the full `schema`; missing columns stay empty.
Block widen_partial_update_block(const TabletSchema& schema,
                                 const std::vector<uint32_t>& update_cids, const Block& narrow);

} // namespace segment_v2
} // namespace doris
