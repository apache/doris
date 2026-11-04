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

#include <parallel_hashmap/phmap.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "core/data_type/define_primitive_type.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris {
class EqualityDeleteHashIndex;
}

namespace doris::format::fluss {

/**
 * The lake half of a fluss scan: the sibling's lake splits, minus the rows a log tail has
 * superseded.
 *
 * <p>A fluss table whose rows are being tiered into a lake is read as two halves: the lake, as the
 * paimon sibling's own splits, and the log written after the lake snapshot. A log table's halves
 * meet at an offset and its lake splits arrive plain (`fluss.range_type=LAKE`), read here purely by
 * delegation. A primary-key table's halves overlap by KEY rather than by row - a row the lake still
 * holds may since have been updated or deleted in the log - so returning both copies would
 * duplicate rows in a way no row count reveals; its lake splits arrive with the tail that
 * supersedes part of them (`fluss.range_type=LAKE_SUPPRESS`).
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
 * range carries the sibling's own payload beside the fluss descriptor for exactly that reason.
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
     * Keys of any shape the integer set does not cover, each packed into one byte string
     * (see the .cpp), so a lake row is one hash lookup rather than the generic predicate's binary
     * search over every key the tail holds. The set points into this object's own storage, so it is
     * built in place and never copied or moved.
     */
    struct PackedKeys {
        PackedKeys() = default;
        PackedKeys(const PackedKeys&) = delete;
        PackedKeys& operator=(const PackedKeys&) = delete;

        /** The type of each key column as packed; a lake block of other types takes the generic path. */
        std::vector<PrimitiveType> types;
        /** Every packed key back to back; `keys` holds views into it. */
        std::string bytes;
        phmap::flat_hash_set<std::string_view> keys;
    };

    /**
     * The DISTINCT keys of one bucket's log tail, one column per key column. Versioned tails share
     * the bounded BE cache; older FE requests use the scan node's split cache.
     *
     * <p>Distinct rather than one row per change-log record, which is what the tail is read as: a tail
     * names each key it touched once for every record it wrote about it, an update alone writes two,
     * and suppression asks only whether a key is in the set. What is retained for the life of the scan
     * therefore has no reason to carry the repeats -- and the scan retains one of these per bucket per
     * partition it reads, all of them at once.
     */
    struct SuppressionKeys {
        Block keys;
        /** Immutable lookup index built once with the cached key block and shared by every split. */
        std::shared_ptr<const EqualityDeleteHashIndex> hash_index;
        /** Change-log records those keys came from, for the profile: kept so the ratio is visible. */
        int64_t records = 0;
        /** The same keys as a hash set, when the key is a single BIGINT or INT column. */
        std::shared_ptr<const phmap::flat_hash_set<int64_t>> int_keys;
        /** Those integer keys ascending, walked alongside lake blocks whose keys ascend too. */
        std::shared_ptr<const std::vector<int64_t>> sorted_int_keys;
        /** The same keys packed into bytes, when they are not a single integer column. */
        std::shared_ptr<const PackedKeys> packed_keys;
    };

    /** The keys of a single BIGINT or INT key column as a hash set; null for any other key. */
    static std::shared_ptr<const phmap::flat_hash_set<int64_t>> build_int_keys(const Block& keys);

    /** The keys packed into bytes; null when a key column has a type packing does not cover. */
    static std::shared_ptr<const PackedKeys> build_packed_keys(const Block& keys);

    ~FlussUnionLakeReader() override {
        // Release the predicate's column references before the cache lease can free its tracked buffers.
        _suppression.reset();
        _current_keys.reset();
    }

    Status init(format::TableReadOptions&& options) override;
    Status prepare_split(const format::SplitReadOptions& options) override;
    Status refresh_conjuncts(VExprContextSPtrs conjuncts) override;
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
    Status _prepare_suppression_keys(const Tail& tail, SuppressionKeys* loaded);
    Status _load_suppression_keys(const format::SplitReadOptions& options, const Tail& tail);
    Status _accumulate_tail_keys(const Tail& tail, const NextBatch& next_batch,
                                 SuppressionKeys* keys);
    Status _read_tail_keys(const Tail& tail, SuppressionKeys* keys);
    Status _build_suppression_predicate(const SuppressionKeys& keys);
    /** What identifies a tail across every scan on this BE. */
    std::string _tail_cache_key(const Tail& tail) const;
    /** The single integer key probed against _fast_keys; *handled is false when the column is not one. */
    Status _suppress_fast(Block* block, bool* handled);
    /** Any other key packed and probed against _packed_keys; *handled as above. */
    Status _suppress_packed(Block* block, bool* handled);
    Status _clone_conjuncts(VExprContextSPtrs* conjuncts) const;
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
    // The tail the predicate was built from, held while splits use it, and its integer keys.
    std::shared_ptr<const SuppressionKeys> _current_keys;
    const phmap::flat_hash_set<int64_t>* _fast_keys = nullptr;
    const std::vector<int64_t>* _sorted_keys = nullptr;
    const PackedKeys* _packed_keys = nullptr;
    // Whether the split being read is a LAKE_SUPPRESS one. A plain LAKE split passes through with
    // no suppression at all, and must not be filtered by a predicate a neighbouring bucket's split
    // left behind - the predicate itself is kept, so a later split of that bucket can reuse it.
    bool _current_split_suppressed = false;

    RuntimeProfile::Counter* _suppressed_rows_counter = nullptr;
    RuntimeProfile::Counter* _tail_keys_read_counter = nullptr;
    RuntimeProfile::Counter* _tail_keys_retained_counter = nullptr;
    RuntimeProfile::Counter* _tail_cache_hit_counter = nullptr;
    // What this reader adds to reading the lake, and the two are answered separately because the
    // remedies differ: a slow tail read is a round trip to fluss made once per bucket, while a slow
    // suppression grows with the lake half's rows.
    RuntimeProfile::Counter* _tail_read_timer = nullptr;
    RuntimeProfile::Counter* _suppress_timer = nullptr;

#ifdef BE_TEST
    // Reading the tail needs a fluss cluster. Tests stand in for that one step and drive everything
    // built on top of it - the cache, the limit, and the suppression itself.
    std::function<Status(const Tail&, Block*)> _test_tail_reader;
#endif
};

} // namespace doris::format::fluss
