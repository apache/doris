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

#include "exec/olap_scanner.h"

#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>

#include <iostream>
#include <vector>

#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "util/cpu_info.h"
#include "util/runtime_profile.h"

namespace doris {

static const int RES_BUF_SIZE = 100 * 1024 * 1024;
static char res_buf[RES_BUF_SIZE];

std::shared_ptr<DorisScanRange> construct_scan_ranges() {
    TPaloScanRange doris_scan_range;
    TNetworkAddress host;
    host.__set_hostname("host");
    host.__set_port(9999);
    doris_scan_range.hosts.push_back(host);
    doris_scan_range.__set_schema_hash("462300563");
    doris_scan_range.__set_version("94");
    // Useless but it is required in TPaloScanRange
    doris_scan_range.__set_version_hash("0");
    doris_scan_range.engine_table_name.push_back("DorisTestStats");
    doris_scan_range.__set_db_name("olap");
    TKeyRange key_range;
    key_range.__set_column_type(to_thrift(TYPE_INT));
    key_range.__set_begin_key(-65535);
    key_range.__set_end_key(65535);
    key_range.__set_column_name("UserId");
    doris_scan_range.partition_column_ranges.push_back(key_range);
    std::shared_ptr<DorisScanRange> scan_range(new DorisScanRange(doris_scan_range));
    return scan_range;
}

void construct_one_tuple(TupleDescriptor& tuple_desc) {
    {
        TSlotDescriptor t_slot;
        t_slot.__set_id(1);
        t_slot.__set_parent(2);
        t_slot.__set_slotType(::doris::TPrimitiveType::INT);
        t_slot.__set_columnPos(0);
        t_slot.__set_byteOffset(0);
        t_slot.__set_nullIndicatorByte(0);
        t_slot.__set_nullIndicatorBit(0);
        t_slot.__set_slotIdx(0);
        t_slot.__set_isMaterialized(true);
        t_slot.__set_colName("UserId");

        SlotDescriptor* slot = new SlotDescriptor(t_slot);
        tuple_desc.add_slot(slot);
    }
}

TEST(OlapIdlUtilTest, normalcase) {}

} // namespace doris
