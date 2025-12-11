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

#include <string>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;
} // namespace doris

namespace doris::vectorized {
class Block;

class ParquetMetadataReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(ParquetMetadataReader);

public:
    ParquetMetadataReader(const std::vector<SlotDescriptor*>& slots, RuntimeState* state,
                          RuntimeProfile* profile, const TMetaScanRange& scan_range);

    Status init_reader();
    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;
    Status close() override;

private:
    Status _init_from_scan_range(const TMetaScanRange& scan_range);

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;
    TMetaScanRange _scan_range;
    std::vector<std::string> _paths;
    std::string _mode;
    bool _eof = false;
};

} // namespace doris::vectorized
