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

#include <gen_cpp/DataSinks_types.h>

#include <algorithm>
#include <iostream>
#include <regex>
#include <sstream>
#include <string>
#include <vector>

namespace doris {
namespace vectorized {

class VHiveUtils {
private:
    VHiveUtils();

public:
    static const std::regex PATH_CHAR_TO_ESCAPE;

    static std::string make_partition_name(const std::vector<THiveColumn>& columns,
                                           const std::vector<int>& partition_columns_input_index,
                                           const std::vector<std::string>& values);

    static std::string escape_path_name(const std::string& path);
};
} // namespace vectorized
} // namespace doris
