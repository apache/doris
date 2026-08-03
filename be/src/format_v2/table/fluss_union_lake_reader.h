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
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::fluss {

/**
 * A lake split of a tiered primary-key fluss table, minus the rows its log tail has superseded.
 *
 * <p>A primary-key table whose rows are being tiered into a lake is read as two halves: the lake, as
 * the paimon sibling's own splits, and the change log written after the lake snapshot. The halves
 * overlap by key rather than by row - a row the lake still holds may since have been updated or
 * deleted in the log - so returning both copies would duplicate rows in a way no row count reveals.
 *
 * <p>The split of labour: this reader only SUPPRESSES. It reads the keys of one bucket's log tail,
 * once per bucket per BE, and drops the lake rows those keys name. What the tail ended up saying
 * about each of those keys is contributed separately, by a `PK_TAIL` fluss range replayed on the
 * Java side, so every row is produced exactly once and a key the tail ended by deleting is produced
 * not at all.
 *
 * <p><b>The lake half is read entirely by the sibling's reader stack.</b> This class owns a
 * PaimonHybridReader and forwards every split to it untouched: native ORC/Parquet, serialized JNI
 * splits, deletion vectors, schema evolution and split scheduling are all its answer. The wrapping
 * range (`fluss_union`) carries the sibling's own payload beside the tail descriptor for exactly
 * that reason.
 *
 * <p><b>Where the filter sits.</b> On the block the child returns, in table-schema terms, not inside
 * the child's file scan request. FE keeps the primary-key columns in the scan's tuple whenever it
 * plans a union read, so the keys are ordinary projected columns of that block and the column mapper
 * has already resolved them - by name or by field id, whichever the lake table needs. Pushing the
 * predicate into the file request would buy the non-key columns of the suppressed rows not being
 * decoded, which is bounded by the size of the tail, and would cost a second place that resolves key
 * columns against a file schema plus a separate implementation for the JNI child.
 */
class FlussUnionLakeReader final : public format::TableReader {
public:
    /** One bucket's log tail: the half-open offset range the lake snapshot does not cover yet. */
    struct Tail {
        /** Empty on an unpartitioned table, which fluss subscribes to by bucket alone. */
        std::string partition_id;
        int32_t bucket_id = 0;
        int64_t start_offset = 0;
        int64_t stop_offset = 0;
        /** Exactly what FE wrote. Also the identity of this tail in the scan node's split cache. */
        std::string spec;
    };

    /**
     * The keys of one bucket's log tail, one row per change-log record and one column per key
     * column. Held by the scan node's split cache so every split of that bucket shares one read.
     */
    struct SuppressionKeys {
        Block keys;
    };

    ~FlussUnionLakeReader() override = default;

    Status init(format::TableReadOptions&& options) override;
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    bool current_split_pruned() const override;
    bool current_split_uses_metadata_count() const override;
    Status abort_split() override;
    Status close() override;
    void set_batch_size(size_t batch_size) override;
    int64_t condition_cache_hit_count() const override;

    /**
     * `partitionId:bucket:start:stop`, as FE encoded it. The partition id is empty rather than a
     * sentinel on an unpartitioned table, so that its buckets cannot collide with a partitioned
     * table's in the cache.
     */
    static Status parse_tail(const std::string& spec, Tail* tail);

    /** The fluss range that reads that tail: one bucket's log over `[start, stop)`, keys only. */
    static TFileRangeDesc tail_scan_range(const Tail& tail);

private:
    /** Fills one batch of tail keys and says whether the tail has been read to its end. */
    using NextBatch = std::function<Status(Block*, bool*)>;

    Status _resolve_union_properties();
    Status _prepare_suppression(const format::SplitReadOptions& options);
    Status _load_suppression_keys(const format::SplitReadOptions& options, const Tail& tail);
    Status _accumulate_tail_keys(const Tail& tail, const NextBatch& next_batch, Block* keys);
    Status _read_tail_keys(const Tail& tail, Block* keys);
    Status _build_suppression_predicate(const Block& keys);
    Status _suppress(Block* block);
    Block _empty_key_block() const;
    void _init_union_profile();

    std::unique_ptr<format::TableReader> _lake_reader;

    /** Positions of the key columns in the projected block, in the order FE listed them. */
    std::vector<size_t> _key_column_indexes;
    /** The projected column descriptions of those same columns, reused rather than rebuilt. */
    std::vector<format::ColumnDefinition> _key_columns;
    int64_t _max_tail_rows = 0;

    /** The tail the current predicate was built from; a split of the same bucket reuses it. */
    std::string _suppression_tail_spec;
    VExprContextSPtr _suppression;

    RuntimeProfile::Counter* _suppressed_rows_counter = nullptr;
    RuntimeProfile::Counter* _tail_keys_read_counter = nullptr;
    RuntimeProfile::Counter* _tail_cache_hit_counter = nullptr;

#ifdef BE_TEST
    // Reading the tail needs a fluss cluster. Tests stand in for that one step and drive everything
    // built on top of it - the cache, the limit, and the suppression itself.
    std::function<Status(const Tail&, Block*)> _test_tail_reader;
#endif
};

} // namespace doris::format::fluss
