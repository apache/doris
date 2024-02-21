#pragma once

#include <memory>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/compaction.h"

namespace doris {

class CloudCumulativeCompaction : public CloudCompactionMixin {
public:
    CloudCumulativeCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);

    ~CloudCumulativeCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    void do_lease();

private:
    Status pick_rowsets_to_compact();

    std::string_view compaction_name() const override { return "CloudCumulativeCompaction"; }

    Status modify_rowsets() override;

    void garbage_collection() override;

    void update_cumulative_point();

    ReaderType compaction_type() const override { return ReaderType::READER_CUMULATIVE_COMPACTION; }

    std::string _uuid;
    int64_t _input_segments = 0;
    int64_t _max_conflict_version = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
    Version _last_delete_version {-1, -1};
};

} // namespace doris
