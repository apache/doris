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

#include <memory>
#include <string>

#include "common/status.h"
#include "format/generic_reader.h"
#include "format_v2/table_reader.h"
#include "gen_cpp/PlanNodes_types.h"

namespace doris::format::iceberg {

class IcebergPositionDeleteSysTableV2Reader final : public format::TableReader {
public:
    ~IcebergPositionDeleteSysTableV2Reader() override;

    Status prepare_split(const format::SplitReadOptions& options) override;
    Status get_block(Block* block, bool* eos) override;
    Status close() override;
    std::string debug_string() const override;

private:
    Status _open_reader();

    TFileRangeDesc _current_range;
    std::unique_ptr<GenericReader> _reader;
    bool _has_split = false;
};

} // namespace doris::format::iceberg
