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

#include "format/reader/table/hive_reader.h"

#include <utility>

#include "format/reader/column_mapper.h"
#include "format/reader/file_reader.h"
#include "runtime/runtime_state.h"

namespace doris::hive {

Status HiveReader::init(reader::TableReadOptions&& options) {
    const bool allow_missing_columns = options.allow_missing_columns;
    const reader::FileFormat format = options.format;
    RETURN_IF_ERROR(reader::TableReader::init(std::move(options)));

    // Hive-specific behavior: choose the column matching mode based on file format and the
    // matching session variable.
    //   - hive_orc_use_column_names / hive_parquet_use_column_names == true
    //     => BY_NAME (modern Hive default, match by column name)
    //   - those options == false
    //     => BY_INDEX (mainly for Hive1 ORC `_col0` / `_col1`, match by top-level position;
    //                  Parquet exposes the same switch for consistency)
    // The base init path does not accept file-format-specific mapper configuration, so the mapper
    // must be replaced here after the base initialization completes.
    DORIS_CHECK(_runtime_state != nullptr);
    const auto& query_options = _runtime_state->query_options();
    bool use_column_names = true;
    switch (format) {
    case reader::FileFormat::ORC:
        use_column_names = query_options.hive_orc_use_column_names;
        break;
    case reader::FileFormat::PARQUET:
        use_column_names = query_options.hive_parquet_use_column_names;
        break;
    case reader::FileFormat::CSV:
        // CSV does not really have a "column name vs position" choice. The format is inherently
        // positional, so BY_INDEX is the closest match to the original behavior.
        use_column_names = false;
        break;
    }

    _mapper_options.mode = use_column_names ? reader::TableColumnMappingMode::BY_NAME
                                           : reader::TableColumnMappingMode::BY_INDEX;
    _mapper_options.allow_missing_columns = allow_missing_columns;
    return Status::OK();
}

} // namespace doris::hive
