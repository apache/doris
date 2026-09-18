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
#include <cstdint>
#include <optional>

#include "core/field.h"
#include "storage/olap_common.h"

namespace doris {

class IColumn;
class TabletSchema;

enum class ReadTimeHiddenColumn { NONE, VERSION, COMMIT_TSO, BINLOG_TSO };

ReadTimeHiddenColumn get_read_time_hidden_column(const TabletSchema& schema,
                                                 int32_t column_unique_id);

std::optional<Field> get_read_time_hidden_column_value(ReadTimeHiddenColumn column,
                                                       const Version& version,
                                                       const TsoRange& commit_tso,
                                                       bool read_row_binlog);

void replace_suffix_with_read_time_hidden_column(ReadTimeHiddenColumn hidden_column,
                                                 const Version& version, const TsoRange& commit_tso,
                                                 bool read_row_binlog, size_t num_rows,
                                                 IColumn& column);

} // namespace doris
