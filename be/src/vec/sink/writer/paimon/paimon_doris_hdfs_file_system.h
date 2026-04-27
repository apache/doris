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

#ifdef WITH_PAIMON_CPP

#include <string>

#include "common/status.h"
#include "paimon/status.h"

namespace doris::vectorized {

constexpr const char* kPaimonDorisHdfsFsIdentifier = "doris_hdfs";

void ensure_paimon_doris_hdfs_file_system_registered();
std::string extract_hdfs_fs_name_for_test(const std::string& uri);
paimon::Status to_paimon_status_for_test(const doris::Status& st);

} // namespace doris::vectorized

#endif
