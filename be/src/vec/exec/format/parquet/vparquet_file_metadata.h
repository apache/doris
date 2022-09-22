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
#include "gen_cpp/parquet_types.h"
#include "schema_desc.h"

namespace doris::vectorized {

class FileMetaData {
public:
    FileMetaData(tparquet::FileMetaData& metadata);
    ~FileMetaData() = default;
    Status init_schema();
    const FieldDescriptor& schema() const { return _schema; };
    const tparquet::FileMetaData& to_thrift();
    std::string debug_string() const;

private:
    tparquet::FileMetaData _metadata;
    FieldDescriptor _schema;
};

} // namespace doris::vectorized
