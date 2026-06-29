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

#include "common/status.h"
#include "format_v2/table_reader.h"

namespace doris::format::hive {
// now hive self only support mixed with orc/parquet files in table and different partitions.
// But if mixed with orc/parquet files in table and same partition, will failed when read.
// now fe will plan table format for all files dirctly, and BE could not handle mixed files also.
class HiveReader final : public format::TableReader {
public:
    ENABLE_FACTORY_CREATOR(HiveReader);
    ~HiveReader() final = default;

    Status prepare_split(const format::SplitReadOptions& options) override;
    format::TableColumnMappingMode mapping_mode() const override;
    Status annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                     format::ProjectedColumnBuildContext* context,
                                     format::ColumnDefinition* column) const override;
    Status validate_projected_columns(
            const format::ProjectedColumnBuildContext& context) const override;
};

} // namespace doris::format::hive
