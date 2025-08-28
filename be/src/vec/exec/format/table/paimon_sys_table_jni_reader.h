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
#include "exec/olap_common.h"
#include "vec/exec/format/jni_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
namespace vectorized {
class Block;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class PaimonSysTableJniReader : public JniReader {
    ENABLE_FACTORY_CREATOR(PaimonSysTableJniReader);

public:
    static const std::string HADOOP_OPTION_PREFIX;
    static const std::string PAIMON_OPTION_PREFIX;
    PaimonSysTableJniReader(const std::vector<SlotDescriptor*>& file_slot_descs,
                            RuntimeState* state, RuntimeProfile* profile,
                            const TPaimonMetadataParams& range_params);

    ~PaimonSysTableJniReader() override = default;

    Status init_reader(
            const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

private:
    const std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    const TPaimonMetadataParams& _range_params;
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
