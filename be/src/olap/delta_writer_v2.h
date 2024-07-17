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

#include <brpc/stream.h>
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
#include "olap/memtable_writer.h"
#include "olap/olap_common.h"
#include "olap/partial_update_info.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class FlushToken;
class MemTable;
class MemTracker;
class Schema;
class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;
class BetaRowsetWriterV2;
class LoadStreamStub;

namespace vectorized {
class Block;
} // namespace vectorized

// Writer for a particular (load, index, tablet).
// This class is NOT thread-safe, external synchronization is required.
class DeltaWriterV2 {
    ENABLE_FACTORY_CREATOR(DeltaWriterV2);

public:
    DeltaWriterV2(WriteRequest* req, const std::vector<std::shared_ptr<LoadStreamStub>>& streams,
                  RuntimeState* state);

    ~DeltaWriterV2();

    Status init();

    Status write(const vectorized::Block* block, const std::vector<uint32_t>& row_idxs);

    // flush the last memtable to flush queue, must call it before close_wait()
    Status close();
    // wait for all memtables to be flushed.
    // mem_consumption() should be 0 after this function returns.
    Status close_wait(int32_t& num_segments, RuntimeProfile* profile = nullptr);

    // abandon current memtable and wait for all pending-flushing memtables to be destructed.
    // mem_consumption() should be 0 after this function returns.
    Status cancel();
    Status cancel_with_status(const Status& st);

private:
    void _build_current_tablet_schema(int64_t index_id,
                                      const OlapTableSchemaParam* table_schema_param,
                                      const TabletSchema& ori_tablet_schema);

    void _update_profile(RuntimeProfile* profile);

    RuntimeState* _state = nullptr;

    bool _is_init = false;
    bool _is_cancelled = false;
    WriteRequest _req;
    std::shared_ptr<BetaRowsetWriterV2> _rowset_writer;
    TabletSchemaSPtr _tablet_schema;
    bool _delta_written_success = false;

    std::mutex _lock;

    int64_t _write_memtable_time = 0;
    int64_t _wait_flush_limit_time = 0;
    int64_t _close_wait_time = 0;

    std::shared_ptr<MemTableWriter> _memtable_writer;
    MonotonicStopWatch _lock_watch;

    std::vector<std::shared_ptr<LoadStreamStub>> _streams;

    std::shared_ptr<PartialUpdateInfo> _partial_update_info;
};

} // namespace doris
