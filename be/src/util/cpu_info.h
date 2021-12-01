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

#ifndef DORIS_BE_SRC_UTIL_CPU_INFO_H
#define DORIS_BE_SRC_UTIL_CPU_INFO_H

#include <memory>
#include <string>
#include <vector>

#include "common/logging.h"

namespace doris {

/// CpuInfo is an interface to query for cpu information at runtime.  The caller can
/// ask for the sizes of the caches and what hardware features are supported.
/// On Linux, this information is pulled from a couple of sys files (/proc/cpuinfo and
/// /sys/devices)
class CpuInfo {
public:
    static const int64_t SSSE3 = (1 << 1);
    static const int64_t SSE4_1 = (1 << 2);
    static const int64_t SSE4_2 = (1 << 3);
    static const int64_t POPCNT = (1 << 4);
    static const int64_t AVX = (1 << 5);
    static const int64_t AVX2 = (1 << 6);

    /// Cache enums for L1 (data), L2 and L3
    enum CacheLevel {
        L1_CACHE = 0,
        L2_CACHE = 1,
        L3_CACHE = 2,
    };
    static const int NUM_CACHE_LEVELS = L3_CACHE + 1;

    /// Initialize CpuInfo.
    static void init();

    /// Determine if the CPU meets the minimum CPU requirements and if not, log an error.
    static void verify_cpu_requirements();

    /// Determine if the CPU scaling governor is set to 'performance' and if not, issue an
    /// error.
    static void verify_performance_governor();

    /// Determine if CPU turbo is disabled and if not, issue an error.
    static void verify_turbo_disabled();

    /// Returns all the flags for this cpu
    static int64_t hardware_flags() {
        DCHECK(initialized_);
        return hardware_flags_;
    }

    /// Returns whether of not the cpu supports this flag
    inline static bool is_supported(long flag) {
        DCHECK(initialized_);
        return (hardware_flags_ & flag) != 0;
    }

    /// Toggle a hardware feature on and off.  It is not valid to turn on a feature
    /// that the underlying hardware cannot support. This is useful for testing.
    static void enable_feature(long flag, bool enable);

    /// Returns the number of cpu cycles per millisecond
    static int64_t cycles_per_ms() {
        DCHECK(initialized_);
        return cycles_per_ms_;
    }

    /// Returns the number of cores (including hyper-threaded) on this machine that are
    /// available for use by Impala (either the number of online cores or the value of
    /// the --num_cores command-line flag).
    static int num_cores() {
        DCHECK(initialized_);
        return num_cores_;
    }

    /// Returns the maximum number of cores that will be online in the system, including
    /// any offline cores or cores that could be added via hot-plugging.
    static int get_max_num_cores() { return max_num_cores_; }

    /// Returns the core that the current thread is running on. Always in range
    /// [0, GetMaxNumCores()). Note that the thread may be migrated to a different core
    /// at any time by the scheduler, so the caller should not assume the answer will
    /// remain stable.
    static int get_current_core();

    /// Returns the maximum number of NUMA nodes that will be online in the system,
    /// including any that may be offline or disabled.
    static int get_max_num_numa_nodes() { return max_num_numa_nodes_; }

    /// Returns the NUMA node of the core provided. 'core' must be in the range
    /// [0, GetMaxNumCores()).
    static int get_numa_node_of_core(int core) {
        DCHECK_LE(0, core);
        DCHECK_LT(core, max_num_cores_);
        return core_to_numa_node_[core];
    }

    /// Returns the cores in a NUMA node. 'node' must be in the range
    /// [0, GetMaxNumNumaNodes()).
    static const std::vector<int>& get_cores_of_numa_node(int node) {
        DCHECK_LE(0, node);
        DCHECK_LT(node, max_num_numa_nodes_);
        return numa_node_to_cores_[node];
    }

    /// Returns the cores in the same NUMA node as 'core'. 'core' must be in the range
    /// [0, GetMaxNumCores()).
    static const std::vector<int>& get_cores_of_same_numa_node(int core) {
        DCHECK_LE(0, core);
        DCHECK_LT(core, max_num_cores_);
        return get_cores_of_numa_node(get_numa_node_of_core(core));
    }

    /// Returns the index of the given core within the vector returned by
    /// GetCoresOfNumaNode() and GetCoresOfSameNumaNode(). 'core' must be in the range
    /// [0, GetMaxNumCores()).
    static int get_numa_node_core_idx(int core) {
        DCHECK_LE(0, core);
        DCHECK_LT(core, max_num_cores_);
        return numa_node_core_idx_[core];
    }

    /// Returns the model name of the cpu (e.g. Intel i7-2600)
    static std::string model_name() {
        DCHECK(initialized_);
        return model_name_;
    }

    static std::string debug_string();

    /// A utility class for temporarily disabling CPU features. Usage:
    ///
    /// {
    ///   CpuInfo::TempDisable disabler(CpuInfo::AVX2);
    ///   // On the previous line, the constructor disables AVX2 instructions. On the next
    ///   // line, CpuInfo::IsSupported(CpuInfo::AVX2) will return false.
    ///   SomeOperation();
    ///   // On the next line, the block closes, 'disabler's destructor runs, and AVX2
    ///   // instructions are re-enabled.
    /// }
    ///
    /// TempDisable's destructor never re-enables features that were not enabled when then
    /// constructor ran.
    struct TempDisable {
        TempDisable(int64_t feature)
                : feature_(feature), reenable_(CpuInfo::is_supported(feature)) {
            CpuInfo::enable_feature(feature_, false);
        }
        ~TempDisable() {
            if (reenable_) {
                CpuInfo::enable_feature(feature_, true);
            }
        }

    private:
        int64_t feature_;
        bool reenable_;
    };

protected:
    friend class CpuTestUtil;

    /// Setup fake NUMA info to simulate NUMA for backend tests. Sets up CpuInfo to
    /// simulate 'max_num_numa_nodes' with 'core_to_numa_node' specifying the NUMA node
    /// of each core in [0, GetMaxNumCores()).
    static void _init_fake_numa_for_test(int max_num_numa_nodes,
                                         const std::vector<int>& core_to_numa_node);

private:
    /// Initialize NUMA-related state - called from Init();
    static void _init_numa();

    /// Initialize 'numa_node_to_cores_' based on 'max_num_numa_nodes_' and
    /// 'core_to_numa_node_'. Called from InitNuma();
    static void _init_numa_node_to_cores();

    /// Populates the arguments with information about this machine's caches.
    /// The values returned are not reliable in some environments, e.g. RHEL5 on EC2, so
    /// so we will keep this as a private method.
    static void _get_cache_info(long cache_sizes[NUM_CACHE_LEVELS],
                                long cache_line_sizes[NUM_CACHE_LEVELS]);

    static bool initialized_;
    static int64_t hardware_flags_;
    static int64_t original_hardware_flags_;
    static int64_t cycles_per_ms_;
    static int num_cores_;
    static int max_num_cores_;
    static std::string model_name_;

    /// Maximum possible number of NUMA nodes.
    static int max_num_numa_nodes_;

    /// Array with 'max_num_cores_' entries, each of which is the NUMA node of that core.
    static std::unique_ptr<int[]> core_to_numa_node_;

    /// Vector with 'max_num_numa_nodes_' entries, each of which is a vector of the cores
    /// belonging to that NUMA node.
    static std::vector<std::vector<int>> numa_node_to_cores_;

    /// Array with 'max_num_cores_' entries, each of which is the index of that core in its
    /// NUMA node.
    static std::vector<int> numa_node_core_idx_;
};
} // namespace doris
#endif
