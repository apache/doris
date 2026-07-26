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

#include "format_v2/table_reader.h"

namespace doris::format::wal {

class WalTableReader final : public TableReader {
public:
    Status annotate_projected_column(const TFileScanSlotInfo& slot_info,
                                     ProjectedColumnBuildContext* context,
                                     ColumnDefinition* column) const override;

protected:
    Status create_file_reader(std::unique_ptr<FileReader>* reader) override;
    TableColumnMappingMode mapping_mode() const override {
        return TableColumnMappingMode::BY_FIELD_ID;
    }
};

} // namespace doris::format::wal
