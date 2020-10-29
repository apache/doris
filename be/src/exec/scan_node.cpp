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

#include "exec/scan_node.h"

#include <boost/bind.hpp>

namespace doris {

const string ScanNode::_s_bytes_read_counter = "BytesRead";
const string ScanNode::_s_rows_read_counter = "RowsRead";
const string ScanNode::_s_materialize_tuple_timer = "MaterializeTupleTime(*)";
const string ScanNode::_s_num_disks_accessed_counter = "NumDiskAccess";
const string ScanNode::_s_scanner_thread_counters_prefix = "ScannerThreads";
const string ScanNode::_s_scanner_thread_total_wallclock_time =
    "ScannerThreadsTotalWallClockTime";

Status ScanNode::prepare(RuntimeState* state) {
    init_scan_profile();
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _bytes_read_counter =
        ADD_COUNTER(_segment_profile, _s_bytes_read_counter, TUnit::BYTES);
    //TODO: The _rows_read_counter == RowsReturned counter in exec node, there is no need to keep both of them
    _rows_read_counter =
        ADD_COUNTER(_scanner_profile, _s_rows_read_counter, TUnit::UNIT);
#ifndef BE_TEST
#endif
    _materialize_tuple_timer = ADD_CHILD_TIMER(runtime_profile(), _s_materialize_tuple_timer,
                               _s_scanner_thread_total_wallclock_time);
    _num_disks_accessed_counter =
        ADD_COUNTER(runtime_profile(), _s_num_disks_accessed_counter, TUnit::UNIT);

    return Status::OK();
}

void ScanNode::init_scan_profile() {
    _scanner_profile.reset(new RuntimeProfile("OlapScanner"));
    runtime_profile()->add_child(_scanner_profile.get(), true, NULL);

    _segment_profile.reset(new RuntimeProfile("SegmentIterator"));
    _scanner_profile->add_child(_segment_profile.get(), true, NULL);
}

}
