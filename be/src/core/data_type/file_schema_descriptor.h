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

#include <optional>
#include <string_view>

#include "core/data_type/data_type.h"

namespace doris {

struct FileFieldDesc {
    const char* name;
    DataTypePtr type;
};

class FileSchemaDescriptor final {
public:
    static const FileSchemaDescriptor& instance();
    const FileFieldDesc& field(size_t idx) const { return _fields[idx]; }
    std::optional<size_t> try_get_position(std::string_view name) const;

private:
    FileSchemaDescriptor();
    std::vector<FileFieldDesc> _fields;
};

} // namespace doris
