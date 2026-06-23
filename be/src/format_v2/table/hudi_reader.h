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

#include <vector>

#include "format_v2/table_reader.h"

namespace doris::format::hudi {

class HudiReader final : public format::TableReader {
public:
    ENABLE_FACTORY_CREATOR(HudiReader);
    ~HudiReader() final = default;

    Status prepare_split(const format::SplitReadOptions& options) override;

#ifdef BE_TEST
    void TEST_set_scan_params(TFileScanRangeParams* params) { _scan_params = params; }
    format::TableColumnMappingMode TEST_mapping_mode() const { return mapping_mode(); }
    Status TEST_annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
        return annotate_file_schema(file_schema);
    }
#endif

protected:
    format::TableColumnMappingMode mapping_mode() const override;
    Status annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) override;

private:
    int64_t _split_schema_id = -1;
};

} // namespace doris::format::hudi
