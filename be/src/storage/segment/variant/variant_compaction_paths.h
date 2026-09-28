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

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "storage/tablet/tablet_schema.h"

namespace doris {

// How one variant column's paths are laid out in a compaction output schema: which become typed
// or extracted columns, which fall back to the sparse column, and the indexes on the materialized
// ones. Paths are relative to the parent variant column. Never persisted with the schema, so only
// a running compaction has one.
struct VariantCompactionPaths {
    std::unordered_map<std::string, TabletSchema::SubColumnInfo> typed_path_set;
    std::unordered_map<std::string, TabletIndexes> subcolumn_indexes;
    // extracted columns
    PathSet sub_path_set;
    // paths left to the sparse column
    PathSet sparse_path_set;
};

// Parent variant column unique id -> that column's layout. Shared, and read-only once built: a
// compaction's rowset writer and every reader it spawns hold the same layout.
using VariantCompactionPathsMap = std::unordered_map<int32_t, VariantCompactionPaths>;
using VariantCompactionPathsSPtr = std::shared_ptr<const VariantCompactionPathsMap>;

// The inverted indexes `paths` attached to `col`, an extracted variant column. Empty when `paths`
// is null (any write that is not a compaction), when it does not cover this column's path, or
// when the column's type cannot carry an inverted index.
std::vector<const TabletIndex*> variant_subcolumn_indexes(const VariantCompactionPathsMap* paths,
                                                          const TabletColumn& col);

} // namespace doris
