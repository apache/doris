#pragma once

#include <memory>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/compaction.h"

namespace doris {

class CloudBaseCompaction : public CloudCompactionMixin {
public:
    CloudBaseCompaction(CloudStorageEngine& engine, CloudTabletSPtr tablet);
    ~CloudBaseCompaction() override;

    Status prepare_compact() override;
    Status execute_compact() override;

    void do_lease();

private:
    Status pick_rowsets_to_compact();

    std::string_view compaction_name() const override { return "CloudBaseCompaction"; }

    Status modify_rowsets() override;

    void garbage_collection() override;

    void _filter_input_rowset();

    void build_basic_info();

    ReaderType compaction_type() const override { return ReaderType::READER_CLOUD_BASE_COMPACTION; }

    std::string _uuid;
    int64_t _input_segments = 0;
    int64_t _base_compaction_cnt = 0;
    int64_t _cumulative_compaction_cnt = 0;
};

} // namespace doris
