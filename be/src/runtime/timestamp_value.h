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

#ifndef DORIS_BE_RUNTIME_TIMESTAMP_VALUE_H
#define DORIS_BE_RUNTIME_TIMESTAMP_VALUE_H

#include <stdint.h>

#include <iostream>
#include <cstddef>
#include <sstream>
#include <exprs/timestamp_functions.h>

#include "udf/udf.h"
#include "util/hash_util.hpp"

#include "timestamp_value.h"

namespace doris {

// Functions to load and access the timestamp database.
class TimezoneDatabase {
public:
    TimezoneDatabase();

    ~TimezoneDatabase();

    static void init() {
        TimezoneDatabase();
    }

    static boost::local_time::time_zone_ptr find_timezone(const std::string &tz);

private:
    static const char *_s_timezone_database_str;
    static boost::local_time::tz_database _s_tz_database;
    static std::vector<std::string> _s_tz_region_list;
};

class TimestampValue {
public:
    TimestampValue() { }

    TimestampValue(time_t timestamp) {
        val = timestamp;
    }

    bool from_date_time_value(DateTimeValue tv, std::string timezone) {
        boost::local_time::time_zone_ptr local_time_zone = TimezoneDatabase::find_timezone(timezone);
        if (local_time_zone == nullptr) {
            return false;
        }

        std::stringstream ss;
        ss << tv;
        boost::posix_time::ptime pt = boost::posix_time::time_from_string(ss.str());
        boost::local_time::local_date_time lt(pt.date(), pt.time_of_day(), local_time_zone,
                                              boost::local_time::local_date_time::NOT_DATE_TIME_ON_ERROR);

        boost::posix_time::ptime utc_ptime = lt.utc_time();
        boost::posix_time::ptime utc_start(boost::gregorian::date(1970, 1, 1));
        boost::posix_time::time_duration dur = utc_ptime - utc_start;
        val = dur.total_milliseconds();
        return true;
    }

    bool to_datetime_value(DateTimeValue &dt_val, std::string timezone) {
        boost::local_time::time_zone_ptr local_time_zone = TimezoneDatabase::find_timezone(timezone);
        if (local_time_zone == nullptr) {
            return false;
        }
        boost::local_time::local_date_time lt(boost::posix_time::from_time_t(val), local_time_zone);
        boost::posix_time::ptime locat_ptime = lt.local_time();

        dt_val.set_type(TIME_DATETIME);
        dt_val.from_olap_datetime(
                locat_ptime.date().year() * 10000000000 +
                locat_ptime.date().month() * 100000000 +
                locat_ptime.date().day() * 1000000 +
                locat_ptime.time_of_day().hours() * 10000 +
                locat_ptime.time_of_day().minutes() * 100 +
                locat_ptime.time_of_day().seconds());
        return true;
    }

    std::string to_datetime_string(std::string timezone) {
        boost::local_time::time_zone_ptr local_time_zone = TimezoneDatabase::find_timezone(timezone);
        if (local_time_zone == nullptr) {
            return "";
        }
        boost::local_time::local_date_time lt(boost::posix_time::from_time_t(val), local_time_zone);
        boost::posix_time::ptime ret_ptime = lt.local_time();

        std::stringstream ss;
        ss << std::setw(4) << std::setfill('0') << ret_ptime.date().year() << "-"
           << std::setw(2) << std::setfill('0') << ret_ptime.date().month().as_number() << "-"
           << std::setw(2) << std::setfill('0') << ret_ptime.date().day() << " "
           << std::setw(2) << std::setfill('0') << ret_ptime.time_of_day().hours() << ":"
           << std::setw(2) << std::setfill('0') << ret_ptime.time_of_day().minutes() << ":"
           << std::setw(2) << std::setfill('0') << ret_ptime.time_of_day().seconds();
        return ss.str();
    }

    void to_datetime_val(doris_udf::DateTimeVal *tv) const {
        boost::posix_time::ptime p = boost::posix_time::from_time_t(val / 1000);
        int _year = p.date().year();
        int _month = p.date().month();
        int _day = p.date().day();
        int64_t _hour = p.time_of_day().hours();
        int64_t _minute = p.time_of_day().minutes();
        int64_t _second = p.time_of_day().seconds();
        int _microsecond = 0;

        int64_t ymd = ((_year * 13 + _month) << 5) | _day;
        int64_t hms = (_hour << 12) | (_minute << 6) | _second;
        tv->packed_time = (((ymd << 17) | hms) << 24) + _microsecond;
        tv->type = TIME_DATETIME;
    }

    bool to_datetime_val(doris_udf::DateTimeVal *tv, std::string timezone) const {
        boost::local_time::time_zone_ptr local_time_zone = TimezoneDatabase::find_timezone(timezone);
        if (local_time_zone == nullptr) {
            return false;
        }
        boost::local_time::local_date_time lt(boost::posix_time::from_time_t(val / 1000), local_time_zone);
        boost::posix_time::ptime p = lt.local_time();

        int _year = p.date().year();
        int _month = p.date().month();
        int _day = p.date().day();
        int64_t _hour = p.time_of_day().hours();
        int64_t _minute = p.time_of_day().minutes();
        int64_t _second = p.time_of_day().seconds();
        int _microsecond = 0;

        int64_t ymd = ((_year * 13 + _month) << 5) | _day;
        int64_t hms = (_hour << 12) | (_minute << 6) | _second;
        tv->packed_time = (((ymd << 17) | hms) << 24) + _microsecond;
        tv->type = TIME_DATETIME;
        return true;
    }

    bool to_time_val(doris_udf::DoubleVal *tv, std::string timezone) const {
        boost::local_time::time_zone_ptr local_time_zone = TimezoneDatabase::find_timezone(timezone);
        if (local_time_zone == nullptr) {
            return false;
        }
        boost::local_time::local_date_time lt(boost::posix_time::from_time_t(val / 1000), local_time_zone);
        boost::posix_time::ptime p = lt.local_time();

        int64_t _hour = p.time_of_day().hours();
        int64_t _minute = p.time_of_day().minutes();
        int64_t _second = p.time_of_day().seconds();

        tv->val = _hour * 3600 + _minute * 60 + _second;
        return true;
    }

public:
    int64_t val;
};

}
#endif //DORIS_BE_RUNTIME_TIMESTAMP_VALUE_H
