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

#include <gen_cpp/PlanNodes_types.h>

#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "roaring/roaring64map.hh"

namespace doris {

class RuntimeState;
class RuntimeProfile;

class VIcebergDeleteRewriteLoader {
public:
    static Status load(RuntimeState* state, RuntimeProfile* profile,
                       const std::string& referenced_data_file_path,
                       const std::vector<TIcebergDeleteFileDesc>& delete_files,
                       const std::map<std::string, std::string>& hadoop_conf,
                       TFileType::type file_type,
                       const std::vector<TNetworkAddress>& broker_addresses,
                       roaring::Roaring64Map* rows_to_delete);
};

} // namespace doris
