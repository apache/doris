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
#include <vector>

#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format/reader/file_reader.h"

namespace doris::reader {

Status rebuild_projected_type(const DataTypePtr& original_type,
                              const std::vector<DataTypePtr>& child_types,
                              const std::vector<std::string>& child_names,
                              DataTypePtr* projected_type);

// Build the file-local schema field after applying a LocalColumnIndex projection.
//
// The projection is matched by SchemaField::id, not by vector ordinal. This keeps nested schema
// evolution semantics in the common helper and lets callers use the same projection tree for type
// rebuilding and file block layout.
Status project_schema_field(const SchemaField& field, const LocalColumnIndex& projection,
                            SchemaField* projected_field);

} // namespace doris::reader
