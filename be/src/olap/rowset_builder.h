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

#include <gen_cpp/Types_types.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "olap/delta_writer_context.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class CalcDeleteBitmapToken;
class FlushToken;
class MemTable;
class MemTracker;
class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;
class RowsetWriter;

namespace vectorized {
class Block;
} // namespace vectorized

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class RowsetBuilder {
public:
    RowsetBuilder(const WriteRequest& req, StorageEngine* storage_engine, RuntimeProfile* profile);

    ~RowsetBuilder();

    Status init();

    Status build_rowset();

    Status submit_calc_delete_bitmap_task();

    Status wait_calc_delete_bitmap();

    Status commit_txn();

    Status cancel();

    std::shared_ptr<RowsetWriter> rowset_writer() const { return _rowset_writer; }

    TabletSharedPtr tablet() const { return _tablet; }

    RowsetSharedPtr rowset() const { return _rowset; }

    TabletSchemaSPtr tablet_schema() const { return _tablet_schema; }

    // For UT
    DeleteBitmapPtr get_delete_bitmap() { return _delete_bitmap; }

    std::shared_ptr<PartialUpdateInfo> get_partial_update_info() const {
        return _partial_update_info;
    }

private:
    void _garbage_collection();

    void _build_current_tablet_schema(int64_t index_id,
                                      const OlapTableSchemaParam* table_schema_param,
                                      const TabletSchema& ori_tablet_schema);

    void _init_profile(RuntimeProfile* profile);

    bool _is_init = false;
    bool _is_cancelled = false;
    bool _is_committed = false;
    WriteRequest _req;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _rowset;
    std::shared_ptr<RowsetWriter> _rowset_writer;
    TabletSchemaSPtr _tablet_schema;

    StorageEngine* _storage_engine = nullptr;

    std::mutex _lock;

    DeleteBitmapPtr _delete_bitmap;
    std::unique_ptr<CalcDeleteBitmapToken> _calc_delete_bitmap_token;
    // current rowset_ids, used to do diff in publish_version
    RowsetIdUnorderedSet _rowset_ids;

    std::shared_ptr<PartialUpdateInfo> _partial_update_info;

    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _build_rowset_timer = nullptr;
    RuntimeProfile::Counter* _submit_delete_bitmap_timer = nullptr;
    RuntimeProfile::Counter* _wait_delete_bitmap_timer = nullptr;
    RuntimeProfile::Counter* _commit_txn_timer = nullptr;
};

} // namespace doris
