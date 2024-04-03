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

#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <stdint.h>

#include <iomanip>
#include <map>
#include <sstream> // IWYU pragma: keep
#include <utility>

#include "common/version_internal.h"
#include "fmt/core.h"
#include "util/uid_util.h"

namespace doris {

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
    ss << version::doris_build_version()
#if defined(__x86_64__) || defined(_M_X64)
#ifdef __AVX2__
       << "(AVX2)"
#else
       << "(SSE4.2)"
#endif
#elif defined(__aarch64__)
       << "(AArch64)"
#endif
#ifdef NDEBUG
       << " RELEASE"
#else
       << " DEBUG"
#if defined(ADDRESS_SANITIZER)
       << " with ASAN"
#elif defined(LEAK_SANITIZER)
       << " with LSAN"
#elif defined(THREAD_SANITIZER)
       << " with TSAN"
#elif defined(UNDEFINED_BEHAVIOR_SANITIZER)
       << " with UBSAN"
#elif defined(MEMORY_SANITIZER)
       << " with MSAN"
#elif defined(BLACKLIST_SANITIZER)
       << " with BLSAN"
#endif
#endif
       << " (build " << version::doris_build_hash() << ")";

    if (!compact) {
        ss << std::endl
           << "Built on " << version::doris_build_time() << " by " << version::doris_build_info();
    }

    return ss.str();
}

std::string get_short_version() {
    static std::string short_version(std::string(version::doris_build_version()) + "-" +
                                     version::doris_build_short_hash());
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

std::string PrintThriftNetworkAddress(const TNetworkAddress& add) {
    std::stringstream ss;
    add.printTo(ss);
    return ss.str();
}

std::string PrintFrontendInfos(const std::vector<TFrontendInfo>& fe_infos) {
    std::stringstream ss;
    const size_t count = fe_infos.size();

    for (int i = 0; i < count; ++i) {
        fe_infos[i].printTo(ss);
        ss << ' ';
    }

    return ss.str();
}

std::string PrintFrontendInfo(const TFrontendInfo& fe_info) {
    std::stringstream ss;
    fe_info.printTo(ss);
    return ss.str();
}

std::string PrintInstanceStandardInfo(const TUniqueId& qid, const TUniqueId& iid) {
    return fmt::format("{}|{}", print_id(iid), print_id(qid));
}

} // namespace doris
