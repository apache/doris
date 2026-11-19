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

#include <butil/macros.h>
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Exprs_types.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "core/block/block.h"
#include "runtime/runtime_state.h"
#include "storage/olap_common.h"
#include "storage/olap_scan_common.h"
#include "storage/rowset/pending_rowset_helper.h"
#include "storage/rowset/rowset_fwd.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {

namespace format {
class LoadTableReader;
}

class DescriptorTbl;
class RuntimeProfile;
class TBrokerScanRange;
class TDescriptorTable;
class TTabletInfo;
class StorageEngine;

class VExprContext;

class PushHandler {
public:
    PushHandler(StorageEngine& engine) : _engine(engine) {}
    ~PushHandler() = default;

    // Load local data file into specified tablet.
    Status process_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                       PushType push_type,
                                       std::vector<TTabletInfo>* tablet_info_vec);

    int64_t write_bytes() const { return _write_bytes; }
    int64_t write_rows() const { return _write_rows; }

private:
    Status _convert_v2(TabletSharedPtr cur_tablet, RowsetSharedPtr* cur_rowset,
                       TabletSchemaSPtr tablet_schema, PushType push_type);

    Status _do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                   PushType push_type, std::vector<TTabletInfo>* tablet_info_vec);

    StorageEngine& _engine;

    // mainly tablet_id, version and delta file path
    TPushReq _request;

    ObjectPool _pool;
    DescriptorTbl* _desc_tbl = nullptr;

    int64_t _write_bytes = 0;
    int64_t _write_rows = 0;
    PendingRowsetGuard _pending_rs_guard;
};

class PushBrokerReader {
    ENABLE_FACTORY_CREATOR(PushBrokerReader);

public:
    PushBrokerReader(const TBrokerScanRange& t_scan_range, const TDescriptorTable& t_desc_tbl);
    ~PushBrokerReader();
    Status init();
    Status next(Block* block);
    void print_profile();

    Status close();
    bool eof() const { return _eof; }

private:
    Status _get_next_reader();

    bool _ready;
    bool _eof;
    size_t _next_range;
    const TDescriptorTable& _t_desc_tbl;
    const TupleDescriptor* _dest_tuple_desc = nullptr;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile = nullptr;
    std::unique_ptr<format::LoadTableReader> _cur_reader;
    bool _cur_reader_eof;
    const TBrokerScanRangeParams& _params;
    const std::vector<TBrokerRangeDesc>& _ranges;
    TFileScanRangeParams _file_params;
    std::vector<TFileRangeDesc> _file_ranges;
    std::unique_ptr<io::FileCacheStatistics> _file_cache_statistics;
    std::unique_ptr<io::FileReaderStats> _file_reader_stats;
    std::shared_ptr<io::IOContext> _io_ctx;
};

} // namespace doris
