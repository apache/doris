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

#include <cstring>
#include <string>

#include "gen_cpp/PaloInternalService_types.h"
#include "olap_scanner.h"
#include "olap_scan_node.h"
#include "olap_utils.h"
#include "olap/olap_reader.h"
#include "service/backend_options.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "util/mem_util.hpp"
#include "util/network_util.h"

namespace palo {

static const std::string SCANNER_THREAD_TOTAL_WALLCLOCK_TIME =
    "ScannerThreadsTotalWallClockTime";
static const std::string MATERIALIZE_TUPLE_TIMER =
    "MaterializeTupleTime(*)";

OlapScanner::OlapScanner(
    RuntimeState* runtime_state,
    const boost::shared_ptr<PaloScanRange> scan_range,
    const std::vector<OlapScanRange>& key_ranges,
    const std::vector<TCondition>& olap_filter,
    const TupleDescriptor& tuple_desc,
    RuntimeProfile* profile,
    const std::vector<TCondition> is_null_vector) :
    _runtime_state(runtime_state),
    _tuple_desc(tuple_desc),
    _scan_range(scan_range),
    _key_ranges(key_ranges),
    _olap_filter(olap_filter),
    _profile(profile),
    _is_open(false),
    _is_null_vector(is_null_vector) {
    _reader.reset(OLAPReader::create(tuple_desc, runtime_state));
    DCHECK(_reader.get() != NULL);
}

OlapScanner::~OlapScanner() {
}

bool OlapScanner::is_open() {
    return _is_open;
}
void OlapScanner::set_opened() {
    _is_open = true;
}

Status OlapScanner::open() {
    TFetchRequest fetch_request;
    fetch_request.__set_use_compression(false);
    fetch_request.__set_num_rows(256);
    fetch_request.__set_schema_hash(
        strtoul(_scan_range->scan_range().schema_hash.c_str(), NULL, 10));
    fetch_request.__set_version(
        strtoul(_scan_range->scan_range().version.c_str(), NULL, 10));
    fetch_request.__set_version_hash(
        strtoul(_scan_range->scan_range().version_hash.c_str(), NULL, 10));
    fetch_request.__set_tablet_id(_scan_range->scan_range().tablet_id);

    // fields
    const std::vector<SlotDescriptor*>& slots = _tuple_desc.slots();
    if (slots.size() <= 0) {
        return Status("Failed to BuildOlapQuery, no query slot!");
    }

    for (int i = 0; i < slots.size(); ++i) {
        if (!slots[i]->is_materialized()) {
            continue;
        }

        fetch_request.field.push_back(slots[i]->col_name());
        VLOG(3) << "Slot: name=" << slots[i]->col_name() << " type=" << slots[i]->type();
    }

    if (fetch_request.field.size() <= 0) {
        return Status("Failed to BuildOlapQuery, no materialized slot!");
    }

    // begin, end key
    for (auto key_range : _key_ranges) {
        if (key_range.begin_scan_range.size() == 1 &&
                key_range.begin_scan_range[0] == NEGATIVE_INFINITY) {
            continue;
        }

        fetch_request.__set_range(key_range.begin_include ? "ge" : "gt");
        fetch_request.__set_end_range(key_range.end_include ? "le" : "lt");
        TFetchStartKey start_key;

        for (auto key : key_range.begin_scan_range) {
            start_key.key.push_back(key);
        }

        fetch_request.start_key.push_back(start_key);
        TFetchEndKey end_key;

        for (auto key : key_range.end_scan_range) {
            end_key.key.push_back(key);
        }

        fetch_request.end_key.push_back(end_key);
    }


    // where cause
    for (auto filter : _olap_filter) {
        fetch_request.where.push_back(filter);
    }
    for (auto is_null_str : _is_null_vector) {
        fetch_request.where.push_back(is_null_str);
    }

    // output
    fetch_request.__set_output("palo2");
    fetch_request.__set_aggregation(_aggregation);

    if (!_reader->init(fetch_request, &_vec_conjunct_ctxs, _profile).ok()) {
        std::string local_ip = BackendOptions::get_localhost();
        std::stringstream ss;
		if (MemTracker::limit_exceeded(*_runtime_state->mem_trackers())) {
			ss << "Memory limit exceeded. Tablet: " << fetch_request.tablet_id << ". host: " << local_ip;
		} else {
			ss << "Storage Reader init fail. Tablet: " << fetch_request.tablet_id << ". host: " << local_ip;
		}	
        return Status(ss.str());
    }

    return Status::OK;
}

Status OlapScanner::get_next(Tuple* tuple, int64_t* raw_rows_read, bool* eof) {
	if (!_reader->next_tuple(tuple, raw_rows_read, eof).ok()) {
		if (MemTracker::limit_exceeded(*_runtime_state->mem_trackers())) {
            LOG(ERROR) << "Memory limit exceeded.";
            return Status("Internal Error: Memory limit exceeded.");       
        }
		LOG(ERROR) << "read storage fail.";
        return Status("Internal Error: read storage fail.");
    }
    return Status::OK;
}

Status OlapScanner::close(RuntimeState* state) {
    _reader.reset();
    Expr::close(_row_conjunct_ctxs, state);
    Expr::close(_vec_conjunct_ctxs, state);
    return Status::OK;
}

} // namespace palo
