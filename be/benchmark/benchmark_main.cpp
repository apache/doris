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

#include <benchmark/benchmark.h>

#include "benchmark_arrow_validation.hpp"
#include "benchmark_bit_pack.hpp"
#include "benchmark_bits.hpp"
#include "benchmark_block_bloom_filter.hpp"
#include "benchmark_column_array_view.hpp"
#include "benchmark_column_array_view_distance.hpp"
#include "benchmark_column_view.hpp"
#include "benchmark_damerau_levenshtein.hpp"
#include "benchmark_fastunion.hpp"
#include "benchmark_fmod.hpp"
#include "benchmark_hll_merge.hpp"
#include "benchmark_hybrid_set.hpp"
#include "benchmark_pdep_unpack.hpp"
#include "benchmark_string.hpp"
#include "benchmark_string_replace.hpp"
#include "benchmark_zone_map_index.hpp"
#include "binary_cast_benchmark.hpp"
#include "common/config.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_string.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/memory/thread_mem_tracker_mgr.h"
#include "runtime/thread_context.h"

// benchmark_binary_plain_page_v2.hpp must be included LAST: it transitively pulls AWS SDK
// headers (via storage/cache/page_cache.h) whose symbols shadow types used by the benchmark
// headers above (notably binary_cast_benchmark.hpp). Keeping it last avoids the clash without
// disabling any benchmark. (Do not let clang-format reorder it above the others.)
#include "benchmark_binary_plain_page_v2.hpp"

namespace doris { // change if need

static void Example1(benchmark::State& state) {
    // init. dont time it.
    state.PauseTiming();
    Block block;
    DataTypePtr str_type = std::make_shared<DataTypeString>();
    std::vector<std::string> vals {100, "content"};
    state.ResumeTiming();

    // do test
    for (auto _ : state) {
        auto str_col = ColumnString::create();
        for (auto& v : vals) {
            str_col->insert_data(v.data(), v.size());
        }
        block.insert({std::move(str_col), str_type, "col"});
        benchmark::DoNotOptimize(block); // mark the watched target
    }
}
// could BENCHMARK many functions to compare them together.
BENCHMARK(Example1);
} // namespace doris

// Custom main: benchmarks that touch DataPage allocation require a Doris
// ThreadContext + mem tracker, otherwise the allocator throws E-7412. Mirrors
// the minimal subset of be/test/testutil/run_all_tests.cpp::main.
int main(int argc, char** argv) {
    doris::config::enable_bmi2_optimizations = true;

    SCOPED_INIT_THREAD_CONTEXT();
    doris::ExecEnv::GetInstance()->init_mem_tracker();
    doris::thread_context()->thread_mem_tracker_mgr->init();
    auto bench_tracker = doris::MemTrackerLimiter::create_shared(
            doris::MemTrackerLimiter::Type::GLOBAL, "BE-BENCH");
    doris::thread_context()->thread_mem_tracker_mgr->attach_limiter_tracker(bench_tracker);
    doris::ExecEnv::set_tracking_memory(false);

    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
