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
//
#ifndef DORIS_BE_EXPRS_TIMEZONE_DB_H
#define DORIS_BE_EXPRS_TIMEZONE_DB_H

#include <stdint.h>
#include <iostream>
#include <cstddef>
#include <sstream>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>

#include "common/logging.h"

namespace doris {

class TimezoneDatabase {
public:
    static void init();
    static boost::local_time::time_zone_ptr find_timezone(const std::string &tz);
    static const std::string default_time_zone;
private:
    static const char *_s_timezone_database_str;
    static boost::local_time::tz_database _s_tz_database;
};
}
#endif
