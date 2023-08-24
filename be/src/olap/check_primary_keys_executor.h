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
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/segment_v2/segment_writer.h"
#include "olap/tablet_meta.h"
#include "util/threadpool.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris {

class Tablet;

class CheckPrimaryKeysToken {
public:
    explicit CheckPrimaryKeysToken(std::unique_ptr<ThreadPoolToken> thread_token)
            : _thread_token(std::move(thread_token)), _status(Status::OK()) {}

    Status submit(Tablet* tablet, const PartialUpdateReadPlan* read_plan,
                  const std::map<RowsetId, RowsetSharedPtr>* rsid_to_rowset,
                  std::unordered_map<uint32_t, std::string>* pk_entries, bool with_seq_col);
    Status submit(Tablet* tablet, const PartialUpdateReadPlan* read_plan,
                  const std::map<RowsetId, RowsetSharedPtr>* rsid_to_rowset,
                  segment_v2::SegmentWriter* segment_writer,
                  std::vector<vectorized::IOlapColumnDataAccessor*>* key_columns, uint32_t row_pos);

    Status wait();

    void cancel() { _thread_token->shutdown(); }

    Status get_delete_bitmap(DeleteBitmapPtr res_bitmap);

private:
    std::unique_ptr<ThreadPoolToken> _thread_token;
    std::shared_mutex _mutex;
    Status _status;
};

class CheckPrimaryKeysExecutor {
public:
    CheckPrimaryKeysExecutor() = default;
    ~CheckPrimaryKeysExecutor() { _thread_pool->shutdown(); }

    void init();

    std::unique_ptr<CheckPrimaryKeysToken> create_token();

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace doris
