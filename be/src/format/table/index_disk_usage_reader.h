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

#include <gen_cpp/PlanNodes_types.h>

#include <cstdint>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "format/generic_reader.h"
#include "io/io_common.h"
#include "runtime/descriptors.h"
#include "storage/index/index_disk_usage.h"

namespace doris {
class Block;
class RuntimeProfile;
class RuntimeState;

// Produces the rows of the index_disk_usage table function for the tablets of one scan range.
// Each block holds the rows of one tablet, so memory stays bounded by the largest tablet.
class IndexDiskUsageReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(IndexDiskUsageReader);

public:
    // Output columns, in the order the FE schema declares them.
    enum class Column : uint8_t {
        kPartitionName,
        kMaterializedIndexName,
        kTabletId,
        kBackendId,
        kRowsetId,
        kSegmentId,
        kIndexId,
        kIndexName,
        kIndexType,
        kColumnName,
        kIndexSuffix,
        kStructure,
        kStorageFormat,
        kSegmentCount,
        kRowCount,
        kTotalBytes,
        kDictBytes,
        kPostingBytes,
        kPositionBytes,
        kStatsBytes,
        kOtherBytes,
        kStatsSource,
    };

    IndexDiskUsageReader(std::vector<SlotDescriptor*> slots, RuntimeState* state,
                         RuntimeProfile* profile, TMetaScanRange scan_range);
    ~IndexDiskUsageReader() override = default;

    Status init_reader();
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status close() override { return Status::OK(); }
    const segment_v2::IndexDiskUsageOptions& options() const { return _options; }

    // The schema that labels the rows of a tablet: the newest schema among its rowsets, because a
    // cached cloud tablet may not know an index that later rowsets already carry.
    static TabletSchemaSPtr label_schema(const TabletSchemaSPtr& tablet_schema,
                                         const std::vector<RowsetSharedPtr>& rowsets);
    // The IO context of one tablet: the query context plus the tablet TTL that classifies its
    // reads in the file cache.
    static io::IOContext tablet_io_context(const io::IOContext& query_io_ctx, int64_t ttl_seconds);

protected:
    Status _do_init_reader(ReaderInitContext* /*ctx*/) override { return init_reader(); }

private:
    Status _collect_tablet(const TIndexDiskUsageTablet& target,
                           std::vector<segment_v2::IndexDiskUsageRow>* rows,
                           TabletSchemaSPtr* current_schema) const;
    Status _fill_block(Block* block, const TIndexDiskUsageTablet& target,
                       const TabletSchema& schema,
                       const std::vector<segment_v2::IndexDiskUsageRow>& rows) const;
    void _append_rows(std::vector<MutableColumnPtr>& columns, const TIndexDiskUsageTablet& target,
                      const TabletSchema& schema,
                      const std::vector<segment_v2::IndexDiskUsageRow>& rows) const;

    RuntimeState* _state = nullptr;
    std::vector<SlotDescriptor*> _slots;
    TMetaScanRange _scan_range;
    std::vector<Column> _slot_columns;
    segment_v2::IndexDiskUsageLevel _level = segment_v2::IndexDiskUsageLevel::kTablet;
    // Referenced by _options, so the reader must not move after init_reader().
    io::IOContext _io_ctx;
    segment_v2::IndexDiskUsageOptions _options;
    size_t _next_tablet = 0;
};

} // namespace doris
