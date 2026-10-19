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
#include "format_v2/file_reader.h"

namespace doris::format {

// Build a projected file-local semantic schema node from a full schema node and a nested
// LocalColumnIndex projection.
//
// This module is deliberately about semantic ColumnDefinition trees, not physical file-format
// trees. FileReader::get_schema() returns file-local columns after type conversion to Doris
// DataType, and their children must follow Doris semantics:
//
//   STRUCT children = fields
//   ARRAY children = [element]
//   MAP children = [key, value]
//
// Format-specific wrappers, such as Parquet MAP key_value/entry nodes, are intentionally hidden
// from this API. A format reader that needs those wrappers for its physical reader tree should
// translate the semantic projection back to its physical layout internally.
//
// The function does three things:
// - Copies `field` metadata to `projected_field`.
// - Recursively prunes children according to `projection.children`, matching children by
//   ColumnDefinition::file_local_id() rather than vector ordinal. The root projection id is not
//   interpreted here because the caller has already selected `field`.
// - Rebuilds the node DataType from the projected semantic children so the returned definition is
//   self-consistent. STRUCT uses projected child names/types, ARRAY uses the projected element
//   type, and MAP preserves the original key type while rebuilding the projected value type.
//
// A full projection copies `field` unchanged. Partial MAP projection only uses the value child for
// type rebuilding. MAP is materialized as offsets + keys + values, so the reader must still read
// the complete key stream to build entry shape and offsets. If the semantic projection includes
// the key child, it is ignored here; key-only MAP projections are rejected because they do not
// define a value shape.
Status project_column_definition(const ColumnDefinition& field, const LocalColumnIndex& projection,
                                 ColumnDefinition* projected_field);

} // namespace doris::format
