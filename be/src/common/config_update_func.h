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

#ifndef DORIS_BE_SRC_COMMON_CONFIG_UPDATE_FUNC_H
#define DORIS_BE_SRC_COMMON_CONFIG_UPDATE_FUNC_H

#include "common/status.h"
#include "configbase.h"
#include "common/config.h"
#include "gutil/strings/substitute.h"

namespace doris {
class Status;

namespace config {

/// regist update check function of config `cumulative_size_based_promotion_size_mbytes`
/// first, check `cumulative_size_based_promotion_size_mbytes` is greater than or equals 
/// `cumulative_size_based_promotion_min_size_mbytes`
/// second, check `cumulative_size_based_promotion_size_mbytes` is greater than or equals 
/// 2 * `cumulative_size_based_compaction_lower_size_mbytes`
CONF_Update_Check(cumulative_size_based_promotion_size_mbytes, [](void* config) {
    int64_t* modify_ptr = (int64_t*)config;
    // check `cumulative_size_based_promotion_size_mbytes` is greater than or equals 
    // `cumulative_size_based_promotion_min_size_mbytes`
    if (*modify_ptr < config::cumulative_size_based_promotion_min_size_mbytes) {
        return Status::InvalidArgument(
                strings::Substitute("cumulative_size_based_promotion_size_mbytes '$0' must be "
                                    "ge cumulative_size_based_promotion_min_size_mbytes",
                                    *modify_ptr));
    }
    // check `cumulative_size_based_promotion_size_mbytes` is greater than or equals 
    // 2 * `cumulative_size_based_compaction_lower_size_mbytes`
    if (*modify_ptr < 2 * config::cumulative_size_based_compaction_lower_size_mbytes) {
        return Status::InvalidArgument(
                strings::Substitute("cumulative_size_based_promotion_size_mbytes '$0' must be "
                                    "ge 2 * cumulative_size_based_compaction_lower_size_mbytes",
                                    *modify_ptr));
    }
    return Status::OK();
});

/// regist update check function of config `cumulative_size_based_promotion_min_size_mbytes`
/// check `cumulative_size_based_promotion_min_size_mbytes` is less than or equals 
/// `cumulative_size_based_promotion_size_mbytes`
CONF_Update_Check(cumulative_size_based_promotion_min_size_mbytes, [](void* config) {
    int64_t* modify_ptr = (int64_t*)config;
    if (*modify_ptr > config::cumulative_size_based_promotion_size_mbytes) {
        return Status::InvalidArgument(
                strings::Substitute("cumulative_size_based_promotion_min_size_mbytes '$0' must be "
                                    "lt cumulative_size_based_promotion_size_mbytes",
                                    *modify_ptr));
    }
    return Status::OK();
});

// regist update check function of config `cumulative_size_based_compaction_lower_size_mbytes`
/// check 2 * `cumulative_size_based_compaction_lower_size_mbytes` is less than or equals 
/// `cumulative_size_based_promotion_size_mbytes`
CONF_Update_Check(cumulative_size_based_compaction_lower_size_mbytes, [](void* config) {
    int64_t* modify_ptr = (int64_t*)config;
    if (*modify_ptr * 2> config::cumulative_size_based_promotion_size_mbytes ) {
        return Status::InvalidArgument(
                strings::Substitute("2 * cumulative_size_based_compaction_lower_size_mbytes '$0' must be "
                                    "lt cumulative_size_based_promotion_size_mbytes",
                                    *modify_ptr));
    }
    return Status::OK();
});

} // namespace config

} // namespace doris

#endif // DORIS_BE_SRC_COMMON_CONFIG_UPDATE_FUNC_H
