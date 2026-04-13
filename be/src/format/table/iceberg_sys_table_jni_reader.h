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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>

#include <cstddef>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "format/jni/jni_reader.h"
#include "storage/olap_common.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class Block;
} // namespace doris

namespace doris {

class IcebergSysTableJniReader : public JniReader {
    ENABLE_FACTORY_CREATOR(IcebergSysTableJniReader);

public:
    IcebergSysTableJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                             RuntimeState* state, RuntimeProfile* profile,
                             const TFileRangeDesc& range, const TFileScanRangeParams* range_params);

    ~IcebergSysTableJniReader() override = default;

    static Status validate_scan_range(const TFileRangeDesc& range);

    Status init_reader();

private:
    Status _init_status;
};

} // namespace doris
