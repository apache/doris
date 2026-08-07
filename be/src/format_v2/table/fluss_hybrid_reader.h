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

#include "common/status.h"
#include "exprs/vexpr_fwd.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::fluss {

/**
 * One reader for every range of a fluss scan, whatever the range holds.
 *
 * <p>A fluss table is read as up to three kinds of ranges, all under the one table format "fluss":
 * ranges of fluss's own log - a log slice, a whole primary-key bucket, a primary-key log tail -
 * read through the JNI scanner; and lake splits of a tiered table, planned by the paimon sibling
 * connector and read by the sibling's reader stack, with the rows a log tail has superseded
 * suppressed on the way out. FE names each range's kind in `fluss.range_type`, and this reader's
 * only job is to hand every range to the child that reads that kind. FileScannerV2 owns one table
 * reader for the scanner lifetime, so the dispatch has to live inside the reader - the same
 * arrangement as PaimonHybridReader, one level up.
 *
 * <p>An unknown or missing range kind fails loud rather than defaulting to either child: a range
 * routed to the wrong side does not fail as a routing mistake, it fails as whatever that child
 * makes of a foreign payload.
 */
class FlussHybridReader final : public format::TableReader {
public:
    ~FlussHybridReader() override = default;

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
     * Whether {@code range} is a lake split (plain or suppressed) rather than a range of fluss's
     * own log. Public because FileScannerV2 must answer "can V2 read this range" by the same key
     * this reader dispatches on: a lake split is readable exactly when the paimon payload it
     * carries is, while a log range needs the JNI path.
     */
    static bool is_lake_range(const TFileRangeDesc& range);

private:
    Status _ensure_current_split_reader(const format::SplitReadOptions& options);
    Status _init_child_reader(format::TableReader* reader, format::FileFormat file_format);
    Status _clone_conjuncts(VExprContextSPtrs* conjuncts) const;

    std::unique_ptr<format::TableReader> _log_reader;  // LOG / PK_FULL / PK_TAIL, via JNI
    std::unique_ptr<format::TableReader> _lake_reader; // LAKE / LAKE_SUPPRESS, via the paimon stack
    format::TableReader* _current_split_reader = nullptr;
#ifdef BE_TEST
    // Tests reach in directly (the UT build disables access control) to observe routing and to
    // stand in children that need no cluster.
    std::function<std::unique_ptr<format::TableReader>()> _test_log_reader_factory;
    std::function<std::unique_ptr<format::TableReader>()> _test_lake_reader_factory;
#endif
};

} // namespace doris::format::fluss
