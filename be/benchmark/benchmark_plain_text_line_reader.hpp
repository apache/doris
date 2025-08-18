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
#include <vector>

#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"

namespace doris {

static std::string create_test_data(size_t length, const std::string& delimiter = "", char fill_char = 'a') {
    return std::string(length, fill_char) + delimiter;
}

static void BM_FindLfCrlfLineSep(benchmark::State& state) {
    size_t data_size = state.range(0);
    size_t delimiter_type = state.range(1);
    
    std::string test_data;
    switch(delimiter_type) {
        case 0: // No delimiter
            test_data = create_test_data(data_size);
            break;
        case 1: // Delimiter is \n
            test_data = create_test_data(data_size, "\n");
            break;
        case 2: // Delimiter is \r\n
            test_data = create_test_data(data_size, "\r\n");
            break;
        default:
            test_data = create_test_data(data_size);
            break;
    }

    PlainTextLineReaderCtx ctx("\n", 1, false);
    const auto* data = reinterpret_cast<const uint8_t*>(test_data.c_str());
    const size_t size = test_data.size();
    
    for (auto _ : state) {
        const auto* result = ctx.find_lf_crlf_line_sep(data, size);
        benchmark::DoNotOptimize(result);
    }
    
    state.SetBytesProcessed(state.iterations() * test_data.size());
    
    std::string label = "size_" + std::to_string(data_size);
    switch (delimiter_type) {
        case 0: label += "_delim_no"; break;
        case 1: label += "_delim_lf"; break;
        case 2: label += "_delim_crlf"; break;
        default: label += "_delim_no"; break;
    }
    state.SetLabel(label);
}

BENCHMARK(BM_FindLfCrlfLineSep)
    ->Unit(benchmark::kNanosecond)
    ->Args({64, 0}) // 64 bytes, no delimiter
    ->Args({64, 1}) // 64 bytes, delimiter is \n
    ->Args({64, 2}) // 64 bytes, delimiter is \r\n
    ->Args({128, 0}) // 128 bytes, no delimiter
    ->Args({128, 1}) // 128 bytes, delimiter is \n
    ->Args({128, 2}) // 128 bytes, delimiter is \r\n
    ->Args({1024, 0}) // 1KB, no delimiter
    ->Args({1024, 1}) // 1KB, delimiter is \n
    ->Args({1024, 2}) // 1KB, delimiter is \r\n
    ->Args({64 * 1024, 0}) // 64KB, no delimiter
    ->Args({64 * 1024, 1}) // 64KB, delimiter is \n
    ->Args({64 * 1024, 2}) // 64KB, delimiter is \r\n
    ->Args({1024 * 1024, 0}) // 1MB, no delimiter
    ->Args({1024 * 1024, 1}) // 1MB, delimiter is \n
    ->Args({1024 * 1024, 2}) // 1MB, delimiter is \r\n
    ->Repetitions(5)
    ->DisplayAggregatesOnly()
    ->ComputeStatistics("min", [](const std::vector<double>& v) -> double {
        return *std::min_element(v.begin(), v.end());
    })
    ->ComputeStatistics("max", [](const std::vector<double>& v) -> double {
        return *std::max_element(v.begin(), v.end());
    });

} // namespace doris
