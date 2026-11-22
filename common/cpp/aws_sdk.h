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

namespace Aws::Client {
struct ClientConfiguration;
}

namespace doris {

class AwsSdkLifetime;
std::shared_ptr<AwsSdkLifetime> acquire_aws_sdk(int log_level = 2);
void ensure_aws_sdk_initialized(int log_level);

// Reuse the SDK defaults: constructing ClientConfiguration may query EC2 metadata.
const Aws::Client::ClientConfiguration& default_aws_client_configuration();
unsigned default_aws_max_connections();

} // namespace doris
