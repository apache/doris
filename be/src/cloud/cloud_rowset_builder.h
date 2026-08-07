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

#include "storage/rowset_builder.h"

namespace doris {

class CloudTablet;
class CloudStorageEngine;

class CloudRowsetBuilder : public BaseRowsetBuilder {
public:
    CloudRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& req,
                       RuntimeProfile* profile);

    ~CloudRowsetBuilder() override;

    Status init() override;

    virtual void update_tablet_stats();

    const RowsetMetaSharedPtr& rowset_meta();

    virtual Status commit_rowset(const std::string& job_id, int64_t table_id);

    virtual Status set_txn_related_info();

    virtual void set_skip_writing_rowset_metadata(bool skip) {
        _skip_writing_rowset_metadata = skip;
    }

protected:
    // Convert `_tablet` from `BaseTablet` to `CloudTablet`
    CloudTablet* cloud_tablet();

    Status check_tablet_version_count();

    CloudStorageEngine& _engine;

    // whether to skip writing rowset metadata to meta service.
    // This is used for empty rowset when config::skip_writing_empty_rowset_metadata is true.
    bool _skip_writing_rowset_metadata = false;
};

class CloudGroupRowsetBuilder final : public CloudRowsetBuilder {
public:
    CloudGroupRowsetBuilder(CloudStorageEngine& engine, const WriteRequest& group_build_req,
                            const WriteRequest& sub_data_req,
                            const WriteRequest& sub_row_binlog_req, RuntimeProfile* profile);

    Status init() override;

    Status build_rowset() override;

    Status submit_calc_delete_bitmap_task() override;

    Status wait_calc_delete_bitmap() override;

    void update_tablet_stats() override;

    Status commit_rowset(const std::string& job_id, int64_t table_id) override;

    Status set_txn_related_info() override;

    void set_skip_writing_rowset_metadata(bool skip) override;

    const BaseTabletSPtr& tablet_sptr() const override { return _data_builder->tablet_sptr(); }

    const RowsetSharedPtr& rowset() const override { return _data_builder->rowset(); }

    const TabletSchemaSPtr& tablet_schema() const override {
        return _data_builder->tablet_schema();
    }

    const std::shared_ptr<PartialUpdateInfo>& get_partial_update_info() const override {
        return _data_builder->get_partial_update_info();
    }

    CloudRowsetBuilder* data_builder() { return _data_builder.get(); }

    CloudRowsetBuilder* row_binlog_builder() { return _row_binlog_builder.get(); }

private:
    std::shared_ptr<CloudRowsetBuilder> _data_builder;
    std::shared_ptr<CloudRowsetBuilder> _row_binlog_builder;
};

} // namespace doris
