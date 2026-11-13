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

#include <string>

namespace doris::io {

// The CA bundle that object storage clients of every backend, and the AWS credentials
// providers, verify TLS with: the first valid path of config::ca_cert_file_paths. It is resolved
// lazily and cached; while no path is valid the result is empty and the next call looks again.
std::string obj_client_ca_cert_file_path();

#ifdef BE_TEST
// Replaces the cached path and returns the previous one; an empty path makes the next
// obj_client_ca_cert_file_path() resolve it again.
std::string exchange_obj_client_ca_cert_file_path_for_test(std::string path);
#endif

} // namespace doris::io
