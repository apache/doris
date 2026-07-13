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

#include "format_v2/jni/iceberg_sys_table_reader.h"

namespace doris::format::iceberg {

Status IcebergSysTableJniReader::validate_scan_range(const TFileRangeDesc& range) const {
    return Status::NotSupported(
            "native Iceberg system-table splits are unavailable on branch-4.1");
}

std::string IcebergSysTableJniReader::connector_class() const {
    return "org/apache/doris/iceberg/IcebergSysTableJniScanner";
}

Status IcebergSysTableJniReader::build_scanner_params(
        std::map<std::string, std::string>* params) const {
    DORIS_CHECK(params != nullptr);
    params->clear();
    return Status::NotSupported(
            "native Iceberg system-table splits are unavailable on branch-4.1");
}

} // namespace doris::format::iceberg
