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
#include <stdint.h>

#include <memory>
#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "common/status.h"
#include "exec/olap_common.h"
#include "olap/olap_common.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/tablet_fwd.h"
#include "runtime/runtime_state.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {

class DescriptorTbl;
class RuntimeProfile;
class Schema;
class TBrokerScanRange;
class TDescriptorTable;
class TTabletInfo;
class StorageEngine;

namespace vectorized {
class Block;
class GenericReader;
class VExprContext;
} // namespace vectorized

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
    PushBrokerReader(const Schema* schema, const TBrokerScanRange& t_scan_range,
                     const TDescriptorTable& t_desc_tbl);
    ~PushBrokerReader() = default;
    Status init();
    Status next(vectorized::Block* block);
    void print_profile();

    Status close();
    bool eof() const { return _eof; }

protected:
    Status _get_next_reader();
    Status _init_src_block();
    Status _cast_to_input_block();
    Status _convert_to_output_block(vectorized::Block* block);
    Status _init_expr_ctxes();

private:
    bool _ready;
    bool _eof;
    int _next_range;
    vectorized::Block* _src_block_ptr = nullptr;
    vectorized::Block _src_block;
    const TDescriptorTable& _t_desc_tbl;
    std::unordered_map<std::string, TypeDescriptor> _name_to_col_type;
    std::unordered_set<std::string> _missing_cols;
    std::unordered_map<std::string, size_t> _src_block_name_to_idx;
    vectorized::VExprContextSPtrs _dest_expr_ctxs;
    vectorized::VExprContextSPtr _pre_filter_ctx_ptr;
    std::vector<SlotDescriptor*> _src_slot_descs_order_by_dest;
    std::unordered_map<int, int> _dest_slot_to_src_slot_index;

    std::vector<SlotDescriptor*> _src_slot_descs;
    std::unique_ptr<RowDescriptor> _row_desc;
    const TupleDescriptor* _dest_tuple_desc = nullptr;

    std::unique_ptr<RuntimeState> _runtime_state;
    RuntimeProfile* _runtime_profile = nullptr;
    std::unique_ptr<vectorized::GenericReader> _cur_reader;
    bool _cur_reader_eof;
    const TBrokerScanRangeParams& _params;
    const std::vector<TBrokerRangeDesc>& _ranges;
    TFileScanRangeParams _file_params;
    std::vector<TFileRangeDesc> _file_ranges;

    std::unique_ptr<io::FileCacheStatistics> _file_cache_statistics;
    std::unique_ptr<io::IOContext> _io_ctx;

    // col names from _slot_descs
    std::vector<std::string> _all_col_names;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    vectorized::VExprContextSPtrs _push_down_exprs;
    const std::unordered_map<std::string, int>* _col_name_to_slot_id;
    // single slot filter conjuncts
    std::unordered_map<int, vectorized::VExprContextSPtrs> _slot_id_to_filter_conjuncts;
    // not single(zero or multi) slot filter conjuncts
    vectorized::VExprContextSPtrs _not_single_slot_filter_conjuncts;
    // File source slot descriptors
    std::vector<SlotDescriptor*> _file_slot_descs;
    // row desc for default exprs
    std::unique_ptr<RowDescriptor> _default_val_row_desc;
    const TupleDescriptor* _real_tuple_desc = nullptr;

    // Not used, just for placeholding
    std::vector<TExpr> _pre_filter_texprs;
};

} // namespace doris
