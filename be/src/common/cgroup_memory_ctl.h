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

#include "common/status.h"

namespace doris {

class CGroupMemoryCtl {
public:
    // Inherited by cgroup v1 and v2
    struct ICgroupsReader {
        virtual ~ICgroupsReader() = default;

        virtual Status read_memory_limit(int64_t* value) = 0;

        virtual Status read_memory_usage(int64_t* value) = 0;
    };

    // Determines the CGroup memory limit from the current processes' cgroup.
    // If the limit is more than INT64_MAX, INT64_MAX is returned (since that is
    // effectively unlimited anyway). Does not take into account memory limits
    // set on any ancestor CGroups.
    static Status find_cgroup_mem_limit(int64_t* bytes);

    // https://serverfault.com/questions/902009/the-memory-usage-reported-in-cgroup-differs-from-the-free-command
    static Status find_cgroup_mem_usage(int64_t* bytes);

    // Returns a human-readable string with information about CGroups.
    static std::string debug_string();
};
} // namespace doris
