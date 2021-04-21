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

#ifndef DORIS_BE_SRC_UTIL_LOAD_ERROR_HUB_H
#define DORIS_BE_SRC_UTIL_LOAD_ERROR_HUB_H

#include <memory>

#include "common/status.h"

namespace doris {

class ExecEnv;
class TLoadErrorHubInfo;

class LoadErrorHub {
public:
    struct ErrorMsg {
        int64_t job_id;
        std::string msg;
        // enum ErrType type;

        ErrorMsg(int64_t id, const std::string& message) : job_id(id), msg(message) {}
    };

    LoadErrorHub() {}

    virtual ~LoadErrorHub() {}

    static Status create_hub(ExecEnv* env, const TLoadErrorHubInfo* t_hub_info,
                             const std::string& error_log_file_name,
                             std::unique_ptr<LoadErrorHub>* hub);

    virtual Status prepare() = 0;

    virtual Status export_error(const ErrorMsg& error_msg) = 0;

    virtual Status close() = 0;

    virtual std::string debug_string() const = 0;

protected:
    // to show mysql url is valid
    bool _is_valid = false;

    int32_t _total_error_num = 0;

}; // end class LoadErrorHub

} // end namespace doris

#endif // DORIS_BE_SRC_UTIL_LOAD_ERROR_HUB_H
