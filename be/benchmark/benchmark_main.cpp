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

#include <string>

#include "vec/columns/column_string.h"
#include "vec/core/block.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_string.h"

namespace doris::vectorized { // change if need

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

} // namespace doris::vectorized

BENCHMARK_MAIN();
