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

namespace doris::cloud {

/**
 * Converts 10-byte fdb version timestamp to 8-byte doris txn id
 * 
 * @param fdb_vts 10 bytes fdb version timestamp
 * @param txn_id 8-byte output txn_id for doris
 * @return 0 for success otherwise error
 */
int get_txn_id_from_fdb_ts(std::string_view fdb_vts, int64_t* txn_id);

} // namespace doris::cloud
