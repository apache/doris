// Modifications copyright (C) 2017, Baidu.com, Inc.
// Copyright 2017 The Apache Software Foundation

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

#include <gtest/gtest.h>
#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "exec/olap_scanner.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/descriptors.h"
#include "util/cpu_info.h"
#include "util/runtime_profile.h"
#include "runtime/runtime_state.h"

namespace palo {

static const int RES_BUF_SIZE = 100 * 1024 * 1024;
static char res_buf[RES_BUF_SIZE];

boost::shared_ptr<PaloScanRange> construct_scan_ranges() {
    TPaloScanRange palo_scan_range;
    TNetworkAddress host;
    host.__set_hostname("host");
    host.__set_port(9999);
    palo_scan_range.hosts.push_back(host);
    palo_scan_range.__set_schema_hash("462300563");
    palo_scan_range.__set_version("94");
    palo_scan_range.__set_version_hash("422202811388534102");
    palo_scan_range.engine_table_name.push_back("PaloTestStats");
    palo_scan_range.__set_db_name("olap");
    TKeyRange key_range;
    key_range.__set_column_type(to_thrift(TYPE_INT));
    key_range.__set_begin_key(-65535);
    key_range.__set_end_key(65535);
    key_range.__set_column_name("UserId");
    palo_scan_range.partition_column_ranges.push_back(key_range);
    boost::shared_ptr<PaloScanRange> scan_range(new PaloScanRange(palo_scan_range));
    return scan_range;
}

void construct_one_tuple(TupleDescriptor& tuple_desc) {
    {
        TSlotDescriptor t_slot;
        t_slot.__set_id(1);
        t_slot.__set_parent(2);
        t_slot.__set_slotType(::palo::TPrimitiveType::INT);
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

TEST(OlapIdlUtilTest, normalcase) {
}

}

int main(int argc, char** argv) {
    std::string conffile = std::string(getenv("PALO_HOME")) + "/conf/be.conf";
    if (!palo::config::init(conffile.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    palo::CpuInfo::Init();
    return RUN_ALL_TESTS();
}
