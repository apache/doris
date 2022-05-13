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

#ifndef DORIS_BE_SRC_OLAP_STORAGE_MIGRATION_V2_H
#define DORIS_BE_SRC_OLAP_STORAGE_MIGRATION_V2_H

#include <deque>
#include <functional>
#include <queue>
#include <utility>
#include <vector>

#include "gen_cpp/AgentService_types.h"
#include "olap/column_mapping.h"
#include "olap/delete_handler.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/tablet.h"

namespace doris {

class StorageMigrationV2Handler {
public:
    static StorageMigrationV2Handler* instance() {
        static StorageMigrationV2Handler instance;
        return &instance;
    }

    // schema change v2, it will not set alter task in base tablet
    Status process_storage_migration_v2(const TStorageMigrationReqV2& request);

private:
    Status _get_versions_to_be_changed(TabletSharedPtr base_tablet,
                                       std::vector<Version>* versions_to_be_changed);

    struct StorageMigrationParams {
        TabletSharedPtr base_tablet;
        TabletSharedPtr new_tablet;
        std::vector<RowsetReaderSharedPtr> ref_rowset_readers;
        DeleteHandler* delete_handler = nullptr;
    };

    Status _do_process_storage_migration_v2(const TStorageMigrationReqV2& request);

    Status _validate_migration_result(TabletSharedPtr new_tablet,
                                      const TStorageMigrationReqV2& request);

    Status _convert_historical_rowsets(const StorageMigrationParams& sm_params);

    Status _generate_rowset_writer(const FilePathDesc& src_desc, const FilePathDesc& dst_desc,
                                   RowsetReaderSharedPtr rowset_reader,
                                   RowsetWriter* new_rowset_writer, TabletSharedPtr new_tablet);

private:
    StorageMigrationV2Handler();
    virtual ~StorageMigrationV2Handler();
    StorageMigrationV2Handler(const StorageMigrationV2Handler&) = delete;
    StorageMigrationV2Handler& operator=(const StorageMigrationV2Handler&) = delete;

    std::shared_ptr<MemTracker> _mem_tracker;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_STORAGE_MIGRATION_V2_H
