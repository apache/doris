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

namespace doris {

/// Absolute path of the SQLite ADBC driver that thirdparty builds for tests.
///
/// It is the only real, self-contained ADBC driver available to unit tests: it needs no server and
/// no container, so it is what proves the ADBC C API call sequence rather than just our own mocks.
/// It is not shipped -- see the D8 exception in the design doc.
std::string adbc_sqlite_driver_path();

/// Whether that driver exists. Tests skip themselves when thirdparty has not been rebuilt since
/// arrow-adbc was added, rather than failing with an unrelated-looking dlopen error.
bool adbc_sqlite_driver_available();

} // namespace doris
