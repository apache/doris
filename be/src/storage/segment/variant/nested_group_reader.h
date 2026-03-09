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
#include <memory>
#include <string>
#include <unordered_map>

namespace doris::segment_v2 {

class ColumnReader;
class NestedOffsetsMappingIndex;

struct NestedGroupReader;
using NestedGroupReaders = std::unordered_map<std::string, std::unique_ptr<NestedGroupReader>>;

// Holds readers for a single NestedGroup (offsets + child columns + nested groups)
struct NestedGroupReader {
    std::string array_path;
    size_t depth = 1; // Nesting depth (1 = first level)
    std::shared_ptr<ColumnReader> offsets_reader;
    std::shared_ptr<NestedOffsetsMappingIndex> offsets_mapping_index;
    std::unordered_map<std::string, std::shared_ptr<ColumnReader>> child_readers;
    // Nested groups within this group (for multi-level nesting)
    NestedGroupReaders nested_group_readers;

    bool is_valid() const { return offsets_reader != nullptr; }
};

} // namespace doris::segment_v2
