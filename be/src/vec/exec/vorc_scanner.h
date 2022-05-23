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

#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/array.h>
#include <vec/exec/varrow_scanner.h>
#include "common/status.h"
#include "exec/base_scanner.h"
#include "gen_cpp/Types_types.h"
#include "runtime/mem_pool.h"
#include "util/runtime_profile.h"

namespace doris::vectorized {

// VOrc scanner convert the data read from Orc to doris's columns.
class VORCScanner : public VArrowScanner {
public:
    VORCScanner(RuntimeState* state, RuntimeProfile* profile, const TBrokerScanRangeParams& params,
                const std::vector<TBrokerRangeDesc>& ranges,
                const std::vector<TNetworkAddress>& broker_addresses,
                const std::vector<TExpr>& pre_filter_texprs, ScannerCounter* counter);

    ~VORCScanner();

protected:
    ArrowReaderWrap* _new_arrow_reader(FileReader* file_reader, int64_t batch_size,
                                       int32_t num_of_columns_from_file) override;
};

} // namespace doris::vectorized
