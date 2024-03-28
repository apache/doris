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

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "exec/olap_common.h"
#include "vec/exec/format/generic_reader.h"
#include "vec/exec/jni_connector.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;

namespace vectorized {
class Block;
} // namespace vectorized
struct TypeDescriptor;
} // namespace doris

namespace doris::vectorized {
class LakeSoulJniReader : public ::doris::vectorized::GenericReader {
    ENABLE_FACTORY_CREATOR(LakeSoulJniReader);

public:
    LakeSoulJniReader(const TLakeSoulFileDesc& lakesoul_params,
                      const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                      RuntimeProfile* profile);

    ~LakeSoulJniReader() override = default;

    Status get_next_block(::doris::vectorized::Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status init_reader(
            std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range);

private:
    const TLakeSoulFileDesc& _lakesoul_params;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state;
    RuntimeProfile* _profile;
    std::unordered_map<std::string, ColumnValueRangeType>* _colname_to_value_range;
    std::unique_ptr<::doris::vectorized::JniConnector> _jni_connector;
};
} // namespace doris::vectorized
