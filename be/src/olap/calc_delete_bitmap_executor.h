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

#include <stdint.h>

#include <atomic>
#include <iosfwd>
#include <memory>
#include <utility>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "util/threadpool.h"

namespace doris {

class DataDir;
class Tablet;
enum RowsetTypePB : int;
using TabletSharedPtr = std::shared_ptr<Tablet>;

// A thin wrapper of ThreadPoolToken to submit calc delete bitmap task.
// Usage:
// 1. create a token
// 2. submit delete bitmap calculate tasks
// 3. wait all tasks complete
// 4. call `get_delete_bitmap()` to get the result of all tasks
class CalcDeleteBitmapToken {
public:
    explicit CalcDeleteBitmapToken(std::unique_ptr<ThreadPoolToken> thread_token)
            : _thread_token(std::move(thread_token)), _status(Status::OK()) {}

    Status submit(TabletSharedPtr tablet, RowsetSharedPtr cur_rowset,
                  const segment_v2::SegmentSharedPtr& cur_segment,
                  const std::vector<RowsetSharedPtr>& target_rowsets, int64_t end_version,
                  DeleteBitmapPtr delete_bitmap,
                  std::shared_ptr<std::map<uint32_t, std::vector<uint32_t>>> indicator_maps,
                  RowsetWriter* rowset_writer);

    // wait all tasks in token to be completed.
    Status wait();

    void cancel() { _thread_token->shutdown(); }

private:
    std::unique_ptr<ThreadPoolToken> _thread_token;

    std::shared_mutex _lock;
    // Records the current status of the calc delete bitmap job.
    // Note: Once its value is set to Failed, it cannot return to SUCCESS.
    Status _status;
};

// CalcDeleteBitmapExecutor is responsible for calc delete bitmap concurrently.
// It encapsulate a ThreadPool to handle all tasks.
class CalcDeleteBitmapExecutor {
public:
    CalcDeleteBitmapExecutor() {}
    ~CalcDeleteBitmapExecutor() { _thread_pool->shutdown(); }

    // init should be called after storage engine is opened,
    void init();

    std::unique_ptr<CalcDeleteBitmapToken> create_token();

private:
    std::unique_ptr<ThreadPool> _thread_pool;
};

} // namespace doris
