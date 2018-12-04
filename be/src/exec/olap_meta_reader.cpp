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

#include "exec/olap_meta_reader.h"

#include <cstring>
#include <string>
#include <errno.h>
#include <algorithm>

#include "gen_cpp/PaloInternalService_types.h"
#include "olap_scanner.h"
#include "olap_scan_node.h"
#include "olap_utils.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_pool.h"

namespace doris {

Status EngineMetaReader::get_hints(
        boost::shared_ptr<DorisScanRange> scan_range,
        int block_row_count,
        bool is_begin_include,
        bool is_end_include,
        std::vector<OlapScanRange>& scan_key_range,
        std::vector<OlapScanRange>* sub_scan_range, 
        RuntimeProfile* profile) {
    auto tablet_id = scan_range->scan_range().tablet_id;
    int32_t schema_hash = strtoul(scan_range->scan_range().schema_hash.c_str(), NULL, 10);
    std::string err;
    TabletSharedPtr table = StorageEngine::instance()->tablet_manager()->get_tablet(
        tablet_id, schema_hash, true, &err);
    if (table == nullptr) {
        std::stringstream ss;
        ss << "failed to get tablet: " << tablet_id << "with schema hash: "
            << schema_hash << ", reason: " << err;
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }

    RuntimeProfile::Counter* show_hints_timer = profile->get_counter("ShowHintsTime");
    std::vector<std::vector<OlapTuple>> ranges;
    bool have_valid_range = false;
    for (auto& key_range : scan_key_range) {
        if (key_range.begin_scan_range.size() == 1 
                && key_range.begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }
        SCOPED_TIMER(show_hints_timer);
    
        OLAPStatus res = OLAP_SUCCESS;
        std::vector<OlapTuple> range;
        res = table->split_range(key_range.begin_scan_range,
                                 key_range.end_scan_range,
                                 block_row_count, &range);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to show hints by split range. [res=%d]", res);
            return Status::InternalError("fail to show hints");
        }
        ranges.emplace_back(std::move(range));
        have_valid_range = true;
    }

    if (!have_valid_range) {
        std::vector<OlapTuple> range;
        auto res = table->split_range({}, {}, block_row_count, &range);
        if (res != OLAP_SUCCESS) {
            OLAP_LOG_WARNING("fail to show hints by split range. [res=%d]", res);
            return Status::InternalError("fail to show hints");
        }
        ranges.emplace_back(std::move(range));
    }

    for (int i = 0; i < ranges.size(); ++i) {
        for (int j = 0; j < ranges[i].size(); j += 2) {
            OlapScanRange range;
            range.begin_scan_range.reset();
            range.begin_scan_range = ranges[i][j];
            range.end_scan_range.reset();
            range.end_scan_range = ranges[i][j + 1];

            if (0 == j) {
                range.begin_include = is_begin_include;
            } else {
                range.begin_include = true;
            }

            if (j + 2 == ranges[i].size()) {
                range.end_include = is_end_include;
            } else {
                range.end_include = false;
            }

            sub_scan_range->push_back(range);
        }
    }

    return Status::OK();
}

} // namespace doris
