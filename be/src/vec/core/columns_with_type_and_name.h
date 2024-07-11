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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/ColumnsWithTypeAndName.h
// and modified by Doris

#pragma once

#include <string>
#include <utility>
#include <vector>

#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {

using ColumnsWithTypeAndName = std::vector<ColumnWithTypeAndName>;
// only used in inverted index
// <field_name, storage_type>
// field_name is the name of inverted index document's filed
//     1. for inverted_index_storage_format_v1, field_name is the `column_name` in Doris
//     2. for inverted_index_storage_format_v2
//         2.1 for normal column, field_name is the `column_unique_id` in Doris
//         2.2 for variant column, field_name is the `parent_column_unique_id.sub_column_name` in Doris
// storage_type is the data type in Doris
using IndexFieldNameAndTypePair = std::pair<std::string, DataTypePtr>;
using NameAndTypePairs = std::vector<std::pair<std::string, DataTypePtr>>;
} // namespace doris::vectorized
