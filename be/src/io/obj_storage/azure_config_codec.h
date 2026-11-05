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

#include <string_view>

#include "common/status.h"
#include "io/obj_storage/obj_storage_config.h"

namespace doris {
class TAzureClientConfig;
} // namespace doris

namespace doris::io {

// Decodes the FE-sent TAzureClientConfig into AzureClientConfig. It only resolves the shape of
// the message; field requirements are checked by AzureConfigValidator afterwards.
class AzureConfigCodec {
public:
    // location_container is the container parsed from the file location. FE normally leaves
    // TAzureClientConfig::container unset and lets each file name its container; when both are
    // present they must match, e.g. config container "c1" with location s3://c2/key is rejected
    // instead of silently reading c2 with credentials scoped to c1.
    static Result<AzureClientConfig> decode(const TAzureClientConfig& config,
                                            std::string_view location_container);
};

} // namespace doris::io
