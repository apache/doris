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
#include "storage/iterators.h"
#include "storage/schema.h"
#include "storage/tablet/tablet_reader.h"

namespace doris {
class HeapMergeOrder;

class BlockIteratorBuilder {
public:
    enum class Mode : uint8_t {
        TOPN,
        CHANGELOG_MIN_DELTA,
        CHANGELOG_DETAIL,
        DIRECT,
        KEY_ORDERED,
        UNIQUE_NEWEST,
        SEQUENCE_MAP,
        AGG_FOLD,
    };

    // Returns false when non-empty rowsets are monotonically ascending and disjoint on keys.
    static bool rowsets_not_mono_asc_disjoint(const TabletReader::ReaderParams& read_params);

    static Mode select_mode(const TabletReader::ReaderParams& read_params, const BaseTablet& tablet,
                            const TabletSchema& tablet_schema, const ReadSchema& read_schema,
                            bool direct_mode, bool merge_by_sequence_mapping);

    BlockIteratorBuilder(const TabletReader::ReaderParams& read_params, const BaseTablet& tablet,
                         ReadSchemaSPtr read_schema, RowsetReaderContext* reader_context,
                         OlapReaderStatistics* stats, uint64_t* merged_rows,
                         BlockBatchLimit batch_limit);

    BlockIteratorBuilder(const BlockIteratorBuilder&) = delete;
    BlockIteratorBuilder& operator=(const BlockIteratorBuilder&) = delete;
    BlockIteratorBuilder(BlockIteratorBuilder&&) = delete;
    BlockIteratorBuilder& operator=(BlockIteratorBuilder&&) = delete;

    Status build(Mode mode, std::unique_ptr<BlockIterator>* out_iter);

private:
    Status _validate_merge_schema(const HeapMergeOrder& order) const;
    Status _build_heap_merge_iter(HeapMergeOrder order, RowwiseIteratorUPtr* merge_iter);
    Status _init_direct_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_ordered_block_iter(HeapMergeOrder order, std::unique_ptr<BlockIterator>* out_iter);
    Status _init_changelog_input_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_key_ordered_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_unique_key_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_sequence_map_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_agg_key_iter(std::unique_ptr<BlockIterator>* out_iter);
    Status _init_topn_iter(std::unique_ptr<BlockIterator>* out_iter);

    const TabletReader::ReaderParams& _read_params;
    const BaseTablet& _tablet;
    ReadSchemaSPtr _read_schema;
    RowsetReaderContext* const _reader_context;
    OlapReaderStatistics* const _stats;
    uint64_t* const _merged_rows;
    const BlockBatchLimit _batch_limit;
};

} // namespace doris
