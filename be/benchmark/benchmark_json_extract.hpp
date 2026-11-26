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

#pragma once

#include <benchmark/benchmark.h>
#include <simdjson.h>

#include <string>
#include <vector>

#include "common/status.h"
#include "exprs/json_functions.h"

namespace doris {
namespace {

constexpr int JSON_EXTRACT_ROWS = 4096;

std::vector<std::string> make_json_rows() {
    std::vector<std::string> rows;
    rows.reserve(JSON_EXTRACT_ROWS);
    for (int i = 0; i < JSON_EXTRACT_ROWS; ++i) {
        rows.emplace_back(
                R"({"skuBaselineFlag":"Y","grossProfitBaseline":123.45,"handPriceBaseline":678.9,)"
                R"("target":"abc","grossRateBaseline":0.1234})");
    }
    return rows;
}

void run_json_extract(benchmark::State& state, const std::string& path) {
    const std::vector<std::string> rows = make_json_rows();
    std::vector<JsonPath> parsed_paths;
    JsonFunctions::parse_json_paths(path, &parsed_paths);
    simdjson::ondemand::parser parser;

    for (auto _ : state) {
        for (const auto& row : rows) {
            simdjson::padded_string json(row.data(), row.size());
            auto document = parser.iterate(json);
            simdjson::ondemand::object object = document.get_object();
            simdjson::ondemand::value value;
            Status status = JsonFunctions::extract_from_object(object, parsed_paths, &value);
            benchmark::DoNotOptimize(status);
            benchmark::DoNotOptimize(value);
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
}

void JsonExtractFromObjectValidField(benchmark::State& state) {
    run_json_extract(state, "$.target");
}
BENCHMARK(JsonExtractFromObjectValidField);

void JsonExtractFromObjectMissingField(benchmark::State& state) {
    run_json_extract(state, "$.does_not_exist");
}
BENCHMARK(JsonExtractFromObjectMissingField);

} // namespace
} // namespace doris
