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

#ifndef DORIS_BE_SRC_UTIL_NULL_LOAD_ERROR_HUB_H
#define DORIS_BE_SRC_UTIL_NULL_LOAD_ERROR_HUB_H

#include <mutex>
#include <queue>
#include <sstream>
#include <string>

#include "load_error_hub.h"

namespace doris {

//  do not export error.
//  only record some metric to some memory(like total error row) for now.

class NullLoadErrorHub : public LoadErrorHub {
public:
    NullLoadErrorHub();

    virtual ~NullLoadErrorHub();

    virtual Status prepare();

    virtual Status export_error(const ErrorMsg& error_msg);

    virtual Status close();

    virtual std::string debug_string() const;

private:
    std::mutex _mtx;
    std::queue<ErrorMsg> _error_msgs;

}; // end class NullLoadErrorHub

} // end namespace doris

#endif // DORIS_BE_SRC_UTIL_NULL_LOAD_ERROR_HUB_H
