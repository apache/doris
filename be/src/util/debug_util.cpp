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

#include "util/debug_util.h"

#include <iomanip>
#include <sstream>

#include "common/logging.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/types.pb.h"
#include "gen_cpp/version.h"
#include "util/cpu_info.h"

#define PRECISION 2
#define KILOBYTE (1024)
#define MEGABYTE (1024 * 1024)
#define GIGABYTE (1024 * 1024 * 1024)

#define SECOND (1000)
#define MINUTE (1000 * 60)
#define HOUR (1000 * 60 * 60)

#define THOUSAND (1000)
#define MILLION (THOUSAND * 1000)
#define BILLION (MILLION * 1000)

namespace doris {

#define THRIFT_ENUM_OUTPUT_FN_IMPL(E, MAP)                         \
    std::ostream& operator<<(std::ostream& os, const E::type& e) { \
        std::map<int, const char*>::const_iterator i;              \
        i = MAP.find(e);                                           \
        if (i != MAP.end()) {                                      \
            os << i->second;                                       \
        }                                                          \
        return os;                                                 \
    }

// Macro to stamp out operator<< for thrift enums.  Why doesn't thrift do this?
#define THRIFT_ENUM_OUTPUT_FN(E) THRIFT_ENUM_OUTPUT_FN_IMPL(E, _##E##_VALUES_TO_NAMES)

// Macro to implement Print function that returns string for thrift enums
#define THRIFT_ENUM_PRINT_FN(E)              \
    std::string Print##E(const E::type& e) { \
        std::stringstream ss;                \
        ss << e;                             \
        return ss.str();                     \
    }

THRIFT_ENUM_OUTPUT_FN(TExprOpcode);
THRIFT_ENUM_OUTPUT_FN(TAggregationOp);
THRIFT_ENUM_OUTPUT_FN(THdfsFileFormat);
THRIFT_ENUM_OUTPUT_FN(THdfsCompression);
THRIFT_ENUM_OUTPUT_FN(TStmtType);
THRIFT_ENUM_OUTPUT_FN(QueryState);
THRIFT_ENUM_OUTPUT_FN(TAgentServiceVersion);

THRIFT_ENUM_PRINT_FN(TStmtType);
THRIFT_ENUM_PRINT_FN(QueryState);

THRIFT_ENUM_PRINT_FN(TMetricKind);
THRIFT_ENUM_PRINT_FN(TUnit);

std::string print_plan_node_type(const TPlanNodeType::type& type) {
    std::map<int, const char*>::const_iterator i;
    i = _TPlanNodeType_VALUES_TO_NAMES.find(type);

    if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        return i->second;
    }

    return "Invalid plan node type";
}

std::string get_build_version(bool compact) {
    std::stringstream ss;
    ss << DORIS_BUILD_VERSION
#ifdef NDEBUG
       << " RELEASE"
#else
       << " DEBUG"
#endif
       << " (build " << DORIS_BUILD_HASH << ")";

    if (!compact) {
        ss << std::endl << "Built on " << DORIS_BUILD_TIME << " by " << DORIS_BUILD_INFO;
    }

    return ss.str();
}

std::string get_short_version() {
    static std::string short_version(std::string(DORIS_BUILD_VERSION) + "-" +
                                     DORIS_BUILD_SHORT_HASH);
    return short_version;
}

std::string get_version_string(bool compact) {
    std::stringstream ss;
    ss << " version " << get_build_version(compact);
    return ss.str();
}

std::string hexdump(const char* buf, int len) {
    std::stringstream ss;
    ss << std::hex << std::uppercase;
    for (int i = 0; i < len; ++i) {
        ss << std::setfill('0') << std::setw(2) << ((uint16_t)buf[i] & 0xff);
    }
    return ss.str();
}

} // namespace doris
