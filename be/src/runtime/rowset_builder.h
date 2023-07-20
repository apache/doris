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
#include "olap/memtable.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer.h"
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

namespace vectorized {
class Block;
} // namespace vectorized

struct BuildContext {
    int64_t tablet_id;
    int32_t schema_hash;
    int64_t txn_id;
    int64_t partition_id;
    PUniqueId load_id;
    bool is_high_priority = false;
    OlapTableSchemaParam* table_schema_param;
    int64_t index_id = 0;
};

// Builder from segments (load, index, tablet).
class RowsetBuilder {
public:
    RowsetBuilder(BuildContext* context, const UniqueId& load_id = TUniqueId());

    ~RowsetBuilder();

    Status init();

    Status append_data(uint32_t segid, butil::IOBuf buf);
    Status close_segment(uint32_t segid);
    Status add_segment(uint32_t segid, SegmentStatistics& stat);

    // wait for all memtables to be flushed.
    Status close();

    int64_t tablet_id() { return _tablet->tablet_id(); }

private:
    void _garbage_collection();

    Status _build_current_tablet_schema(int64_t index_id,
                                        const OlapTableSchemaParam* table_schema_param,
                                        const TabletSchema& ori_tablet_schema);

    RowsetSharedPtr _build_rowset();

    bool _is_init = false;
    bool _is_closed = false;
    bool _is_canceled = false;
    BuildContext _context;
    TabletSharedPtr _tablet;
    RowsetSharedPtr _cur_rowset;
    std::unique_ptr<RowsetWriter> _rowset_writer;
    //const TabletSchema* _tablet_schema;
    // tablet schema owned by delta writer, all write will use this tablet schema
    // it's build from tablet_schema（stored when create tablet） and OlapTableSchema
    // every request will have it's own tablet schema so simple schema change can work
    TabletSchemaSPtr _tablet_schema;

    StorageEngine* _storage_engine;
    UniqueId _load_id;
    std::mutex _lock;

    DeleteBitmapPtr _delete_bitmap = nullptr;
    // current rowset_ids, used to do diff in publish_version
    RowsetIdUnorderedSet _rowset_ids;
    // current max version, used to calculate delete bitmap
    int64_t _cur_max_version;

    bool _success;
    std::shared_ptr<RowsetMeta> _rowset_meta;
    std::unordered_map<uint32_t /*segid*/, SegmentStatisticsSharedPtr> _segment_stat_map;
    std::mutex _segment_stat_map_lock;
    std::vector<io::FileWriterPtr> _segment_file_writers;
};

using RowsetBuilderSharedPtr = std::shared_ptr<RowsetBuilder>;

} // namespace doris
