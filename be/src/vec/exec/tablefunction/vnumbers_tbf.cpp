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

#include "vec/exec/tablefunction/vnumbers_tbf.h"

#include <memory>
#include <sstream>

#include "exec/exec_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"

namespace doris {
namespace vectorized {
VNumbersTBF::VNumbersTBF(TupleId tuple_id, const TupleDescriptor* tuple_desc)
        : VTableValuedFunctionInf(tuple_id, tuple_desc) {}

void transverSegments() {
    std::vector<TabletSharedPtr> tablets =
            StorageEngine::instance()->tablet_manager()->get_all_tablet();
    for (const auto& tablet : tablets) {
        TabletMetaSharedPtr tabletMetas = tablet->tablet_meta();
        // max version rowset
        std::shared_lock rowset_ldlock(tablet->get_header_lock());

        // all rowset Meta
        // std::vector<RowsetMetaSharedPtr> RowsetMetas = tabletMetas->all_rs_metas();
        RowsetSharedPtr rowset = tablet->get_rowset_by_version(tabletMetas->max_version());
        SegmentCacheHandle cache_handle;
        Status st = SegmentLoader::instance()->load_segments(
                std::dynamic_pointer_cast<BetaRowset>(rowset), &cache_handle);
        if (!st.ok()) {
            return;
        }
        LOG(INFO) << "--ftw:";
        LOG(INFO) << "tablet datadir path = " << tablet->data_dir()->path()
                  << " tablet id = " << tablet->tablet_id()
                  << " rowset tablet path = " << rowset->tablet_path() << " rowsetMeat = {"
                  << rowset->rowset_meta()->rowset_id() << ", "
                  << rowset->rowset_meta()->partition_id() << ", "
                  << rowset->rowset_meta()->tablet_id();

        for (auto& seg_ptr : cache_handle.get_segments()) {
            // ... visit segment...
            // 必须处理完segments
            std::shared_ptr<SegmentFooterPB> segment_footer =
                    std::make_shared<SegmentFooterPB>(seg_ptr->footer());
            LOG(INFO) << "segment num rows = " << segment_footer->num_rows();
        }
    }
}

Status VNumbersTBF::get_next(RuntimeState* state, vectorized::Block* block, bool* eos) {
    transverSegments();
    bool mem_reuse = block->mem_reuse();
    DCHECK(block->rows() == 0);
    std::vector<vectorized::MutableColumnPtr> columns(_slot_num);

    do {
        for (int i = 0; i < _slot_num; ++i) {
            if (mem_reuse) {
                columns[i] = std::move(*(block->get_by_position(i).column)).mutate();
            } else {
                columns[i] = _tuple_desc->slots()[i]->get_empty_mutable_column();
            }
        }
        while (true) {
            RETURN_IF_CANCELLED(state);
            int batch_size = state->batch_size();
            if (columns[0]->size() == batch_size) {
                // what if batch_size < _total_numbers, should we set *eos？
                break;
            }
            // if _total_numbers == 0, so we can break loop at now.
            if (_cur_offset >= _total_numbers) {
                *eos = true;
                break;
            }
            columns[0]->insert_data(reinterpret_cast<const char*>(&_cur_offset),
                                    sizeof(_cur_offset));
            ++_cur_offset;
        }
        auto n_columns = 0;
        if (!mem_reuse) {
            for (const auto slot_desc : _tuple_desc->slots()) {
                block->insert(ColumnWithTypeAndName(std::move(columns[n_columns++]),
                                                    slot_desc->get_data_type_ptr(),
                                                    slot_desc->col_name()));
            }
        } else {
            columns.clear();
        }
    } while (block->rows() == 0 && !(*eos));
    return Status::OK();
}

Status VNumbersTBF::set_scan_ranges(const std::vector<TScanRangeParams>& scan_range_params) {
    _total_numbers = scan_range_params[0].scan_range.tvf_scan_range.numbers_params.totalNumbers;
    return Status::OK();
}
} // namespace vectorized

} // namespace doris
