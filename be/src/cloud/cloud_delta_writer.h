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

#include <bthread/mutex.h>

#include "olap/delta_writer.h"

namespace doris {

class CloudStorageEngine;
class CloudRowsetBuilder;

class CloudDeltaWriter final : public BaseDeltaWriter {
public:
    CloudDeltaWriter(CloudStorageEngine& engine, const WriteRequest& req, RuntimeProfile* profile,
                     const UniqueId& load_id);
    ~CloudDeltaWriter() override;

    Status write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs) override;

    Status close() override;

    Status cancel_with_status(const Status& st) override;

    Status build_rowset() override;

    void update_tablet_stats();

    const RowsetMetaSharedPtr& rowset_meta();

    bool is_init() const { return _is_init; }

    static Status batch_init(std::vector<CloudDeltaWriter*> writers);

    Status commit_rowset();

    Status set_txn_related_delete_bitmap();

    QueryThreadContext query_thread_context() { return _query_thread_context; }

private:
    // Convert `_rowset_builder` from `BaseRowsetBuilder` to `CloudRowsetBuilder`
    CloudRowsetBuilder* rowset_builder();

    bthread::Mutex _mtx;
    CloudStorageEngine& _engine;
    QueryThreadContext _query_thread_context;
};

} // namespace doris
