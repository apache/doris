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
#include "runtime/timestamp_value.h"

#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/date_time/time_zone_base.hpp>
#include <boost/date_time/local_time/local_time.hpp>
#include <boost/thread/thread.hpp>
#include "runtime/string_value.h"



namespace doris {

boost::local_time::tz_database TimezoneDatabase::_s_tz_database;
std::vector <std::string> TimezoneDatabase::_s_tz_region_list;

TimezoneDatabase::TimezoneDatabase() {
    // Create a temporary file and write the timezone information.  The boost
    // interface only loads this format from a file.  We don't want to raise
    // an error here since this is done when the backend is created and this
    // information might not actually get used by any queries.
    char filestr[] = "/tmp/doris.tzdb.XXXXXXX";
    FILE *file = NULL;
    int fd = -1;

    if ((fd = mkstemp(filestr)) == -1) {
        LOG(ERROR) << "Could not create temporary timezone file: " << filestr;
        return;
    }

    if ((file = fopen(filestr, "w")) == NULL) {
        unlink(filestr);
        close(fd);
        LOG(ERROR) << "Could not open temporary timezone file: " << filestr;
        return;
    }

    if (fputs(_s_timezone_database_str, file) == EOF) {
        unlink(filestr);
        close(fd);
        fclose(file);
        LOG(ERROR) << "Could not load temporary timezone file: " << filestr;
        return;
    }

    fclose(file);
    _s_tz_database.load_from_file(std::string(filestr));
    _s_tz_region_list = _s_tz_database.region_list();
    unlink(filestr);
    close(fd);
}

TimezoneDatabase::~TimezoneDatabase() {}

boost::local_time::time_zone_ptr TimezoneDatabase::find_timezone(const std::string &tz) {
    try {
        // See if they specified a zone id
        if (tz.find_first_of('/') != std::string::npos) {
            return _s_tz_database.time_zone_from_region(tz);
        } else {
            //eg. +08:00
            boost::local_time::time_zone_ptr tzp(new boost::local_time::posix_time_zone(std::string("TMP") + tz));
            return tzp;
        }
    } catch (boost::exception& e) {
        return nullptr;
    }
}

}
