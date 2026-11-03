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
#include "gen_cpp/Descriptors_types.h"

struct paimon_result_get_table;

namespace doris::paimon {

// Validation is independent of reader/writer selection and performs no filesystem IO.
Status validate_table_descriptor(const TPaimonTableDesc& descriptor);

// Caller owns both returned handles and applies its reader/writer error policy.
// The descriptor must pass validate_table_descriptor before this call.
paimon_result_get_table open_table_from_descriptor(const TPaimonTableDesc& descriptor);

} // namespace doris::paimon
