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

#include "format_v2/table/hudi_reader.h"

#include "format_v2/table/schema_history_util.h"

namespace doris::format::hudi {

Status HudiReader::prepare_split(const format::SplitReadOptions& options) {
    _split_schema_id = -1;
    if (options.current_range.__isset.table_format_params &&
        options.current_range.table_format_params.__isset.hudi_params &&
        options.current_range.table_format_params.hudi_params.__isset.schema_id) {
        _split_schema_id = options.current_range.table_format_params.hudi_params.schema_id;
    }
    return format::TableReader::prepare_split(options);
}

format::TableColumnMappingMode HudiReader::mapping_mode() const {
    return format::can_map_by_history_schema(_scan_params, _split_schema_id)
                   ? format::TableColumnMappingMode::BY_FIELD_ID
                   : format::TableColumnMappingMode::BY_NAME;
}

Status HudiReader::annotate_file_schema(std::vector<format::ColumnDefinition>* file_schema) {
    DORIS_CHECK(file_schema != nullptr);
    if (mapping_mode() != format::TableColumnMappingMode::BY_FIELD_ID) {
        return Status::OK();
    }
    return format::annotate_file_schema_from_history(_scan_params, _split_schema_id, file_schema);
}

} // namespace doris::format::hudi
