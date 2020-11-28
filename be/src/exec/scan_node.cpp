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

const std::string ScanNode::_s_bytes_read_counter = "BytesRead";
const std::string ScanNode::_s_rows_read_counter = "RowsRead";
const std::string ScanNode::_s_total_throughput_counter = "TotalReadThroughput";
const std::string ScanNode::_s_num_disks_accessed_counter = "NumDiskAccess";

Status ScanNode::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::prepare(state));

    _bytes_read_counter = ADD_COUNTER(runtime_profile(), _s_bytes_read_counter, TUnit::BYTES);
    //TODO: The _rows_read_counter == RowsReturned counter in exec node, there is no need to keep both of them
    _rows_read_counter = ADD_COUNTER(runtime_profile(), _s_rows_read_counter, TUnit::UNIT);
#ifndef BE_TEST
    _total_throughput_counter =
            runtime_profile()->add_rate_counter(_s_total_throughput_counter, _bytes_read_counter);
#endif
    _num_disks_accessed_counter =
            ADD_COUNTER(runtime_profile(), _s_num_disks_accessed_counter, TUnit::UNIT);

    return Status::OK();
}

} // namespace doris
