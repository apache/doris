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

#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "common/status.h"
#include "olap/delta_writer_context.h"
#include "olap/memtable.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset_builder.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;
class RowsetWriter;
class RuntimeProfile;

namespace vectorized {
class Block;
} // namespace vectorized

// Builder from segments (load, index, tablet).
class LoadStreamWriter {
public:
    LoadStreamWriter(WriteRequest* context, RuntimeProfile* profile);

    ~LoadStreamWriter();

    Status init();

    Status append_data(uint32_t segid, butil::IOBuf buf);

    Status close_segment(uint32_t segid);

    Status add_segment(uint32_t segid, const SegmentStatistics& stat);

    // wait for all memtables to be flushed.
    Status close();

    int64_t tablet_id() { return _req.tablet_id; }

private:
    bool _is_init = false;
    bool _is_canceled = false;
    WriteRequest _req;
    RowsetBuilder _rowset_builder;
    std::shared_ptr<RowsetWriter> _rowset_writer = nullptr;
    std::mutex _lock;

    std::unordered_map<uint32_t /*segid*/, SegmentStatisticsSharedPtr> _segment_stat_map;
    std::mutex _segment_stat_map_lock;
    std::vector<io::FileWriterPtr> _segment_file_writers;
};

using LoadStreamWriterSharedPtr = std::shared_ptr<LoadStreamWriter>;

} // namespace doris
