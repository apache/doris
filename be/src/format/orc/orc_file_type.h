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

namespace orc {
class Type;
}

namespace doris {
class Status;
class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;

bool is_orc_file_type(const orc::Type& type);
std::unique_ptr<orc::Type> create_orc_file_type();
// URI is required. Optional children may be absent; present children retain canonical order/types.
Status validate_orc_file_type(const orc::Type& type);
// Apply FILE markers to an explicitly supplied output schema, including nested FILE nodes.
Status annotate_orc_file_types(const DataTypePtr& data_type, orc::Type& type);

} // namespace doris
