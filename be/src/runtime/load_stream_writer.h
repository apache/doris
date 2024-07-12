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

#include <gen_cpp/internal_service.pb.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>
#include <vector>

#include "brpc/stream.h"
#include "butil/iobuf.h"
#include "common/status.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "olap/delta_writer_context.h"
#include "olap/memtable.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"
#include "util/spinlock.h"
#include "util/uid_util.h"

namespace doris {

class StorageEngine;
class TupleDescriptor;
class SlotDescriptor;
class OlapTableSchemaParam;
class RowsetWriter;
class RuntimeProfile;
struct SegmentStatistics;
using SegmentStatisticsSharedPtr = std::shared_ptr<SegmentStatistics>;
class BaseRowsetBuilder;

namespace vectorized {
class Block;
} // namespace vectorized

// Builder from segments (load, index, tablet).
class LoadStreamWriter {
public:
    LoadStreamWriter(WriteRequest* context, RuntimeProfile* profile);

    ~LoadStreamWriter();

    Status init();

    Status append_data(uint32_t segid, uint64_t offset, butil::IOBuf buf,
                       FileType file_type = FileType::SEGMENT_FILE);

    Status close_writer(uint32_t segid, FileType file_type);

    Status add_segment(uint32_t segid, const SegmentStatistics& stat, TabletSchemaSPtr flush_chema);

    Status _calc_file_size(uint32_t segid, FileType file_type, size_t* file_size);

    // wait for all memtables to be flushed.
    Status close();

private:
    bool _is_init = false;
    bool _is_canceled = false;
    WriteRequest _req;
    std::unique_ptr<BaseRowsetBuilder> _rowset_builder;
    std::shared_ptr<RowsetWriter> _rowset_writer;
    std::mutex _lock;

    std::unordered_map<uint32_t /*segid*/, SegmentStatisticsSharedPtr> _segment_stat_map;
    std::mutex _segment_stat_map_lock;
    std::vector<io::FileWriterPtr> _segment_file_writers;
    std::vector<io::FileWriterPtr> _inverted_file_writers;
    QueryThreadContext _query_thread_context;
};

using LoadStreamWriterSharedPtr = std::shared_ptr<LoadStreamWriter>;

} // namespace doris
