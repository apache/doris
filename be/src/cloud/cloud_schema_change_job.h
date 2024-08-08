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

#include <memory>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "olap/rowset/rowset.h"
#include "olap/schema_change.h"
#include "olap/tablet_fwd.h"

namespace doris {

class CloudSchemaChangeJob {
public:
    CloudSchemaChangeJob(CloudStorageEngine& cloud_storage_engine, std::string job_id,
                         int64_t expiration);
    ~CloudSchemaChangeJob();

    // This method is idempotent for a same request.
    Status process_alter_tablet(const TAlterTabletReqV2& request);

private:
    Status _convert_historical_rowsets(const SchemaChangeParams& sc_params);

    Status _process_delete_bitmap(int64_t alter_version, int64_t start_calc_delete_bitmap_version,
                                  int64_t initiator);

private:
    CloudStorageEngine& _cloud_storage_engine;
    std::shared_ptr<CloudTablet> _base_tablet;
    std::shared_ptr<CloudTablet> _new_tablet;
    TabletSchemaSPtr _base_tablet_schema;
    TabletSchemaSPtr _new_tablet_schema;
    std::string _job_id;
    std::vector<RowsetSharedPtr> _output_rowsets;
    int64_t _output_cumulative_point = 0;
    // absolute expiration time in second
    int64_t _expiration;
};

} // namespace doris
