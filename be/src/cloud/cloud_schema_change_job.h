#pragma once

#include <memory>
#include "cloud/cloud_storage_engine.h"
#include "olap/rowset/rowset.h"
#include "olap/schema_change.h"
#include "cloud/cloud_tablet.h"
#include "olap/tablet_fwd.h"

namespace doris {

class CloudSchemaChangeJob {
public:
    CloudSchemaChangeJob(CloudStorageEngine& cloud_storage_engine, std::string job_id, int64_t expiration);
    ~CloudSchemaChangeJob();

    // This method is idempotent for a same request.
    Status process_alter_tablet(const TAlterTabletReqV2& request);

private:
    Status _convert_historical_rowsets(const SchemaChangeParams& sc_params);

private:
    CloudStorageEngine& _cloud_storage_engine;
    std::shared_ptr<CloudTablet> _base_tablet;
    std::shared_ptr<CloudTablet> _new_tablet;
    TabletSchemaSPtr _base_tablet_schema;
    TabletSchemaSPtr _new_tablet_schema;
    std::string _job_id;
    std::vector<RowsetSharedPtr> _output_rowsets;
    int64_t _output_cumulative_point = 0;
    int64_t _expiration;
};

} // namespace doris
