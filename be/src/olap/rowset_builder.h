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

    virtual Status build_rowset();

    virtual Status submit_calc_delete_bitmap_task();

    virtual Status wait_calc_delete_bitmap();

    virtual Status commit_txn() {
        return Status::NotSupported("BaseRowsetBuilder::commit_txn not implemented");
    }

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

    bool is_data_builder() const { return _req.write_req_type == WriteRequestType::DATA; }

    // Attach an extra rowset (e.g. binlog rowset) to the same txn.
    Status attach_rowset_to_txn(const RowsetSharedPtr& rowset);

    // Attach an extra pending rowset id so that PendingLocalRowsets can be
    // cleaned up together with the primary rowset.
    Status attach_pending_rs_guard_to_txn(const RowsetId& rowset_id);

    RowsetId rowset_id() const { return _rowset_id; }

    Status init_mow_context(std::shared_ptr<MowContext>& mow_context);

protected:
    Status _build_current_tablet_schema(int64_t index_id,
                                        const OlapTableSchemaParam* table_schema_param,
                                        const TabletSchema& ori_tablet_schema);

    virtual void _init_profile(RuntimeProfile* profile);

    Status _init_context_common_fields(RowsetWriterContext& context);

    bool _is_init = false;
    bool _is_cancelled = false;
    bool _is_committed = false;
    WriteRequest _req;
    BaseTabletSPtr _tablet;
    RowsetSharedPtr _rowset;
    // Extra rowsets attached to the same txn (e.g. binlog rowsets).
    std::vector<RowsetSharedPtr> _attach_rowsets;
    std::shared_ptr<RowsetWriter> _rowset_writer;
    PendingRowsetGuard _pending_rs_guard;
    // Extra rowset ids that share the same PendingRowsetGuard.
    std::vector<RowsetId> _attach_rowset_ids;
    TabletSchemaSPtr _tablet_schema;

    std::mutex _lock;

    DeleteBitmapPtr _delete_bitmap;
    std::unique_ptr<CalcDeleteBitmapToken> _calc_delete_bitmap_token;
    // current rowset_ids, used to do diff in publish_version
    std::shared_ptr<RowsetIdUnorderedSet> _rowset_ids {std::make_shared<RowsetIdUnorderedSet>()};
    int64_t _max_version_in_flush_phase {-1};

    std::shared_ptr<PartialUpdateInfo> _partial_update_info;

    RuntimeProfile* _profile = nullptr;
    RuntimeProfile::Counter* _build_rowset_timer = nullptr;
    RuntimeProfile::Counter* _submit_delete_bitmap_timer = nullptr;
    RuntimeProfile::Counter* _wait_delete_bitmap_timer = nullptr;

    RowsetId _rowset_id;
};

// `StorageEngine` mixin for `BaseRowsetBuilder`
class RowsetBuilder : public BaseRowsetBuilder {
public:
    RowsetBuilder(StorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile);

    ~RowsetBuilder() override;

    Status init() override;

    Status commit_txn() override;

    // Cast `BaseTablet` to `Tablet`
    Tablet* tablet();

private:
    void _init_profile(RuntimeProfile* profile) override;

    Status check_tablet_version_count();

    Status prepare_txn();

    void _garbage_collection(bool cancel_txn);

    TabletSharedPtr tablet_sptr();

    StorageEngine& _engine;
    RuntimeProfile::Counter* _commit_txn_timer = nullptr;
};

// Rowset builder dedicated for row_binlog rowset, it shares the same tablet
// but uses an independent row_binlog tablet schema.
class RowBinlogRowsetBuilder : public RowsetBuilder {
public:
    RowBinlogRowsetBuilder(StorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile);

    // just attach rowset to txn_rs_builder in GroupRowsetBuilder, then rely on
    // txn_rs_builder's clean logic.
    ~RowBinlogRowsetBuilder() override = default;

    Status init() override;

    // before commit, binlog rowset builder is responsible for cleaning rowset.
    // after commit, rowset will be attached to data(txn) rowset builder, and
    // the owner of rowset will be changed, so cleaning rowset is handed to the
    // data(txn) rowset builder.
    Status commit_txn() override {
        _is_committed = true;
        return Status::OK();
    }
};

// manage one transaction with multiple rowset_builder
// eg. normal data rowset + row_binlog rowset
// Now only support one tablet
class GroupRowsetBuilder : public BaseRowsetBuilder {
public:
    GroupRowsetBuilder(StorageEngine& engine, const WriteRequest& req,
                       const WriteRequest& row_binlog_req, RuntimeProfile* profile);

    Status init() override;

    Status build_rowset() override;

    Status submit_calc_delete_bitmap_task() override;

    Status wait_calc_delete_bitmap() override;

    Status commit_txn() override;

    RowsetBuilder* txn_rowset_builder() { return _txn_rs_builder.get(); }
    RowsetBuilder* row_binlog_builder() { return _row_binlog_rowset_builder.get(); }

private:
    // txn rowset builder will manage txn; other builders will add their
    // rowsets into here.
    std::shared_ptr<RowsetBuilder> _txn_rs_builder;
    std::shared_ptr<RowsetBuilder> _row_binlog_rowset_builder;
};

} // namespace doris
