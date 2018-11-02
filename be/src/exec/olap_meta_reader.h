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

#ifndef DORIS_BE_SRC_QUERY_EXEC_OLAP_META_READER_H
#define DORIS_BE_SRC_QUERY_EXEC_OLAP_META_READER_H

#include <boost/shared_ptr.hpp>
#include <list>
#include <vector>
#include <string>
#include <utility>

#include "common/status.h"
#include "exec/olap_common.h"
#include "gen_cpp/PlanNodes_types.h"
#include "runtime/descriptors.h"
#include "runtime/tuple.h"

namespace doris {

class StorageShowHints;
class RuntimeProfile;

/*
 * @breif 读取olap engine meta的接口
 */
class EngineMetaReader {
public:
    static Status get_hints(
        boost::shared_ptr<DorisScanRange> scan_range,
        int block_row_count,
        bool is_begin_include,
        bool is_end_include,
        std::vector<OlapScanRange>& scan_key_range,
        std::vector<OlapScanRange>* sub_scan_range, 
        RuntimeProfile* profile);
};

} // namespace doris

#endif
