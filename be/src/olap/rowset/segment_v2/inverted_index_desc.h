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

namespace doris {
namespace segment_v2 {

class InvertedIndexDescriptor {
public:
    static std::string get_temporary_index_path(const std::string& segment_path, uint32_t uuid);
    static std::string get_index_file_name(const std::string& path, uint32_t uuid);
    static const std::string get_temporary_bkd_index_data_file_name() { return "bkd"; }
    static const std::string get_temporary_bkd_index_meta_file_name() { return "bkd_meta"; }
    static const std::string get_temporary_bkd_index_file_name() { return "bkd_index"; }
};

} // namespace segment_v2
} // namespace doris