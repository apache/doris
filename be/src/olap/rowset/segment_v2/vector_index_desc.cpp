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

#include "olap/rowset/segment_v2/vector_index_desc.h"

#include "gutil/strings/strip.h"

namespace doris::segment_v2 {

const std::string segment_suffix = ".dat";
const std::string index_suffix = ".vi";
const std::string index_name_separator = "_";

std::string VectorIndexDescriptor::get_index_file_name(const std::string& segment_path,
                                                       int64_t index_id,
                                                       const std::string& index_suffix_path) {
    std::string suffix = index_suffix_path.empty() ? "" : "@" + index_suffix_path;
    return StripSuffixString(segment_path, segment_suffix) + index_name_separator +
           std::to_string(index_id) + suffix + index_suffix;
}

}