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

#include "util/mem_info.h"

#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <unistd.h>

#include "gutil/strings/split.h"

#include "util/pretty_printer.h"
#include "util/string_parser.hpp"
#include "util/cgroup_util.h"

namespace doris {

bool MemInfo::_s_initialized = false;
int64_t MemInfo::_s_physical_mem = -1;

void MemInfo::init() {
    // Read from /proc/meminfo
    std::ifstream meminfo("/proc/meminfo", std::ios::in);
    std::string line;

    while (meminfo.good() && !meminfo.eof()) {
        getline(meminfo, line);
        std::vector<std::string> fields = strings::Split(line, " ", strings::SkipWhitespace());

        // We expect lines such as, e.g., 'MemTotal: 16129508 kB'
        if (fields.size() < 3) {
            continue;
        }

        if (fields[0].compare("MemTotal:") != 0) {
            continue;
        }

        StringParser::ParseResult result;
        int64_t mem_total_kb = StringParser::string_to_int<int64_t>(fields[1].data(),
                               fields[1].size(), &result);

        if (result == StringParser::PARSE_SUCCESS) {
            // Entries in /proc/meminfo are in KB.
            _s_physical_mem = mem_total_kb * 1024L;
        }

        break;
    }

    int64_t cgroup_mem_limit = 0;
    Status status = CGroupUtil::find_cgroup_mem_limit(&cgroup_mem_limit);
    if (status.ok() && cgroup_mem_limit > 0) {
        _s_physical_mem = min(_s_physical_mem, cgroup_mem_limit);
    }

    if (meminfo.is_open()) {
        meminfo.close();
    }

    if (_s_physical_mem == -1) {
        LOG(WARNING) << "Could not determine amount of physical memory on this machine.";
    }

    LOG(INFO) << "Physical Memory: "
              << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES);

    _s_initialized = true;
}

std::string MemInfo::debug_string() {
    DCHECK(_s_initialized);
    CGroupUtil util;
    std::stringstream stream;
    stream << "Mem Info: " << PrettyPrinter::print(_s_physical_mem, TUnit::BYTES)
           << std::endl;
    stream << "CGroup Info: " << util.debug_string() << std::endl;
    return stream.str();
}

}
