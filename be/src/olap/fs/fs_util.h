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
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/Types_types.h"
#include "olap/fs/block_manager.h"

namespace doris {
namespace fs {
namespace fs_util {

// Each BlockManager type may have different params, so we provide a separate
// method for each type(instead of a factory method which require same params)
BlockManager* block_manager(TStorageMedium::type storage_medium);

StorageMediumPB get_storage_medium_pb(TStorageMedium::type t_storage_medium);

} // namespace fs_util
} // namespace fs
} // namespace doris
