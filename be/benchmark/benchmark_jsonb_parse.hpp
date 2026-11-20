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

#include <string>
#include <vector>

#include "common/status.h"
#include "util/jsonb_parser_simd.h"
#include "util/jsonb_writer.h"

namespace doris {
namespace {

constexpr int JSONB_PARSE_ROWS = 1024;

std::string make_jsonb_parse_row(int i, int nested_objects) {
    std::string row = R"({"ts":"2026-11-19T21:53:)" + std::to_string(10 + i % 50) +
                      R"(Z","level":"INFO","service":"checkout","host":"web-)" +
                      std::to_string(i % 32) + R"(","latency_ms":)" + std::to_string(i % 997) +
                      R"(.5,"ok":true,"msg":"order \"accepted\" for user )" + std::to_string(i) +
                      R"(","items":[)";
    for (int j = 0; j < nested_objects; ++j) {
        if (j != 0) {
            row.append(",");
        }
        row.append(R"({"sku":"SKU-)")
                .append(std::to_string(i * 7 + j))
                .append(R"(","qty":)")
                .append(std::to_string(1 + j % 5))
                .append(R"(,"price":)")
                .append(std::to_string(10 + j))
                .append(R"(.99,"tags":["a","b"]})");
    }
    row.append("]}");
    return row;
}

std::vector<std::string> make_jsonb_parse_rows(int nested_objects) {
    std::vector<std::string> rows;
    rows.reserve(JSONB_PARSE_ROWS);
    for (int i = 0; i < JSONB_PARSE_ROWS; ++i) {
        rows.push_back(make_jsonb_parse_row(i, nested_objects));
    }
    return rows;
}

// state.range(0) is the number of nested objects per row: 0 gives about 160 bytes, 2 about
// 270 bytes and 80 about 4.6 KB.
void run_jsonb_parse(benchmark::State& state, bool reuse_buffers) {
    const std::vector<std::string> rows = make_jsonb_parse_rows(static_cast<int>(state.range(0)));
    int64_t bytes = 0;
    for (const auto& row : rows) {
        bytes += static_cast<int64_t>(row.size());
    }
    JsonbWriter writer;
    JsonbParser::ParseBuffers buffers;
    for (auto _ : state) {
        for (const auto& row : rows) {
            Status status = reuse_buffers
                                    ? JsonbParser::parse(row.data(), row.size(), writer, buffers)
                                    : JsonbParser::parse(row.data(), row.size(), writer);
            auto output_size = writer.getOutput()->getSize();
            benchmark::DoNotOptimize(status);
            benchmark::DoNotOptimize(output_size);
        }
    }
    state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(rows.size()));
    state.SetBytesProcessed(state.iterations() * bytes);
}

void JsonbParseFreshBuffers(benchmark::State& state) {
    run_jsonb_parse(state, false);
}
BENCHMARK(JsonbParseFreshBuffers)->Arg(0)->Arg(2)->Arg(80);

void JsonbParseReusedBuffers(benchmark::State& state) {
    run_jsonb_parse(state, true);
}
BENCHMARK(JsonbParseReusedBuffers)->Arg(0)->Arg(2)->Arg(80);

} // namespace
} // namespace doris
