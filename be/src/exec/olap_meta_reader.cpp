// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include <lzo/lzo1x.h>

#include "gen_cpp/PaloInternalService_types.h"
#include "olap_scanner.h"
#include "olap_scan_node.h"
#include "olap_utils.h"
#include "olap/olap_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_pool.h"

namespace palo {

EngineMetaReader::EngineMetaReader(
    boost::shared_ptr<PaloScanRange> scan_range) :
    _scan_range(scan_range) {
}

EngineMetaReader::~EngineMetaReader() {
}

Status EngineMetaReader::close() {
    return Status::OK;
}

Status EngineMetaReader::open() {
    return Status::OK;
}

Status EngineMetaReader::get_hints(
    int block_row_count,
    bool is_begin_include,
    bool is_end_include,
    std::vector<OlapScanRange>& scan_key_range,
    std::vector<OlapScanRange>* sub_scan_range, 
    RuntimeProfile* profile) {
    TShowHintsRequest show_hints_request;
    show_hints_request.__set_tablet_id(_scan_range->scan_range().tablet_id);
    show_hints_request.__set_schema_hash(
        strtoul(_scan_range->scan_range().schema_hash.c_str(), NULL, 10));
    show_hints_request.__set_block_row_count(block_row_count);
    show_hints_request.__set_end_range("lt");

    for (auto key_range : scan_key_range) {
        if (key_range.begin_scan_range.size() == 1 
                && key_range.begin_scan_range[0] == NEGATIVE_INFINITY) {
            continue;
        }
        TFetchEndKey end_key;
        TFetchStartKey start_key;

        for (auto key : key_range.begin_scan_range) {
            start_key.key.push_back(key);
        }
        for (auto key : key_range.end_scan_range) {
            end_key.key.push_back(key);
        }

        show_hints_request.start_key.push_back(start_key);
        show_hints_request.end_key.push_back(end_key);
    }

    std::vector<std::vector<std::vector<std::string>>> ranges;

    if (!OLAPShowHints::show_hints(show_hints_request, &ranges, profile).ok()) {
        LOG(WARNING) << "Failed to show_hints.";
        return Status("Show hints execute fail.");
    }

    for (int i = 0; i < ranges.size(); ++i) {
        for (int j = 0; j < ranges[i].size(); j += 2) {
            OlapScanRange range;
            range.begin_scan_range.clear();
            range.begin_scan_range = ranges[i][j];
            range.end_scan_range.clear();
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

    return Status::OK;
}

} // namespace palo
