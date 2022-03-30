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

#include "util/error_util.h"

#include <errno.h>

#include <cstring>
#include <sstream>
#include <vector>

using std::string;
using std::stringstream;
using std::vector;
using std::ostream;

namespace doris {

string get_str_err_msg() {
    // Save errno. "<<" could reset it.
    int e = errno;
    if (e == 0) {
        return "";
    }
    stringstream ss;
    char buf[1024];
    ss << "Error(" << e << "): " << strerror_r(e, buf, 1024);
    return ss.str();
}

} // namespace doris
