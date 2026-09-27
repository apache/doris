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

#include "format/table/hive/hive_arrow_block_convertor.h"
#include "format/transformer/vparquet_writer.h"

namespace doris {

class VHiveParquetWriter final : public VParquetWriter {
public:
    using VParquetWriter::VParquetWriter;

protected:
    std::unique_ptr<ArrowBlockConvertor> _create_arrow_block_convertor(
            DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
            const cctz::time_zone& timezone) const override {
        return std::make_unique<hive::HiveArrowBlockConvertor>(std::move(types), std::move(names),
                                                               timezone_name, timezone);
    }
};

} // namespace doris
