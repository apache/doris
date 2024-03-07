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
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_fwd.h"
#include "util/runtime_profile.h"

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
class BaseRowsetBuilder {
public:
    BaseRowsetBuilder(const WriteRequest& req, RuntimeProfile* profile);

    virtual ~BaseRowsetBuilder();

    virtual Status init() = 0;

    Status build_rowset();

    Status submit_calc_delete_bitmap_task();

    Status wait_calc_delete_bitmap();

    Status cancel();

    const std::shared_ptr<RowsetWriter>& rowset_writer() const { return _rowset_writer; }

    const BaseTabletSPtr& tablet() const { return _tablet; }

    const RowsetSharedPtr& rowset() const { return _rowset; }

    const TabletSchemaSPtr& tablet_schema() const { return _tablet_schema; }

    // For UT
    const DeleteBitmapPtr& get_delete_bitmap() { return _delete_bitmap; }

    const std::shared_ptr<PartialUpdateInfo>& get_partial_update_info() const {
        return _partial_update_info;
    }

    Status init_mow_context(std::shared_ptr<MowContext>& mow_context);

protected:
    void _build_current_tablet_schema(int64_t index_id,
                                      const OlapTableSchemaParam* table_schema_param,
                                      const TabletSchema& ori_tablet_schema);

    virtual void _init_profile(RuntimeProfile* profile);

    bool _is_init = false;
    bool _is_cancelled = false;
    WriteRequest _req;
    BaseTabletSPtr _tablet;
    RowsetSharedPtr _rowset;
    std::shared_ptr<RowsetWriter> _rowset_writer;
    PendingRowsetGuard _pending_rs_guard;
    TabletSchemaSPtr _tablet_schema;

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
};

// `StorageEngine` mixin for `BaseRowsetBuilder`
class RowsetBuilder final : public BaseRowsetBuilder {
public:
    RowsetBuilder(StorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile);

    ~RowsetBuilder() override;

    Status init() override;

    Status commit_txn();

private:
    void _init_profile(RuntimeProfile* profile) override;

    Status check_tablet_version_count();

    Status prepare_txn();

    void _garbage_collection();

    // Cast `BaseTablet` to `Tablet`
    Tablet* tablet();
    TabletSharedPtr tablet_sptr();

    StorageEngine& _engine;
    RuntimeProfile::Counter* _commit_txn_timer = nullptr;
    bool _is_committed = false;
};

} // namespace doris
