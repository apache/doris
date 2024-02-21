#pragma once

#include <memory>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/compaction.h"

namespace doris {

class CloudFullCompaction : public CloudCompactionMixin {
public:
    CloudFullCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);

    ~CloudFullCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    void do_lease();

protected:
    Status pick_rowsets_to_compact();

    std::string_view compaction_name() const override { return "CloudFullCompaction"; }

    Status modify_rowsets() override;
    void garbage_collection() override;

private:
    /*
    Status _cloud_full_compaction_update_delete_bitmap(int64_t initiator);
    Status _cloud_full_compaction_calc_delete_bitmap(const RowsetSharedPtr& rowset,
                                                     const int64_t& cur_version,
                                                     const DeleteBitmapPtr& delete_bitmap);
    */

    ReaderType compaction_type() const override { return ReaderType::READER_FULL_COMPACTION; }

    std::string _uuid;
    int64_t _input_segments = 0;
    // Snapshot values when pick input rowsets
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
};

} // namespace doris
