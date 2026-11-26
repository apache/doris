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

#include <cstdint>
#include <memory>

#include "common/status.h"
#include "storage/iterator/block_batch_limit.h"
#include "storage/iterator/block_iterator.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_reader.h"

namespace doris {
class BaseTablet;
struct OlapReaderStatistics;
struct RowsetReaderContext;
class TabletSchema;

enum class BlockIterMode : uint8_t {
    NONE,
    TOPN,
    CHANGELOG_MIN_DELTA,
    CHANGELOG_DETAIL,
    DIRECT,
    KEY_ORDERED,
    UNIQUE_NEWEST,
    SEQUENCE_MAP,
    AGG_FOLD,
};

struct BlockIteratorBuildContext {
    const BaseTablet& tablet;
    ReadSchemaSPtr read_schema;
    RowsetReaderContext& reader_context;
    OlapReaderStatistics& stats;
    uint64_t* merged_rows;
    BlockBatchLimit batch_limit;
};

// Returns false when non-empty rowsets are monotonically ascending and disjoint on keys.
bool rowsets_not_mono_asc_disjoint(const TabletReader::ReaderParams& read_params);

BlockIterMode select_block_iter_mode(const TabletReader::ReaderParams& read_params,
                                     const BaseTablet& tablet, const TabletSchema& tablet_schema,
                                     const ReadSchema& read_schema, bool direct_mode,
                                     bool merge_by_sequence_mapping);

Status build_block_iterator(const TabletReader::ReaderParams& read_params, BlockIterMode mode,
                            BlockIteratorBuildContext ctx,
                            std::unique_ptr<BlockIterator>* out_iter);

} // namespace doris
