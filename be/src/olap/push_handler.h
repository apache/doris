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

#include <map>
#include <string>
#include <vector>

#include "exec/base_scanner.h"
#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "olap/file_helper.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_schema.h"

namespace doris {

struct ColumnMapping;
class RowCursor;

class PushHandler {
public:
    PushHandler() = default;
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

    // Only for debug
    std::string _debug_version_list(const Versions& versions) const;

    Status _do_streaming_ingestion(TabletSharedPtr tablet, const TPushReq& request,
                                   PushType push_type, std::vector<TTabletInfo>* tablet_info_vec);

    // mainly tablet_id, version and delta file path
    TPushReq _request;

    ObjectPool _pool;
    DescriptorTbl* _desc_tbl = nullptr;

    int64_t _write_bytes = 0;
    int64_t _write_rows = 0;
    DISALLOW_COPY_AND_ASSIGN(PushHandler);
};

class PushBrokerReader {
public:
    PushBrokerReader() : _ready(false), _eof(false) {}
    ~PushBrokerReader() = default;

    Status init(const Schema* schema, const TBrokerScanRange& t_scan_range,
                const TDescriptorTable& t_desc_tbl);
    Status next(vectorized::Block* block);
    void print_profile();

    Status close() {
        _ready = false;
        return Status::OK();
    }
    bool eof() const { return _eof; }

private:
    bool _ready;
    bool _eof;
    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile;
    std::unique_ptr<ScannerCounter> _counter;
    std::unique_ptr<BaseScanner> _scanner;
    // Not used, just for placeholding
    std::vector<TExpr> _pre_filter_texprs;
};

} // namespace doris
