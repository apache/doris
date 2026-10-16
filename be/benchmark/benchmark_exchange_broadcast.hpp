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

#include <cstdint>
#include <memory>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "agent/be_exec_version_manager.h"
#include "core/block/block.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "exec/operator/exchange_sink_buffer.h"

namespace doris::broadcast_benchmark {

inline Block make_input(size_t columns, size_t string_bytes, bool random_strings, uint64_t seed) {
    constexpr size_t rows = 4096;
    std::mt19937_64 random(seed);
    Block block;
    for (size_t c = 0; c < columns; ++c) {
        if (string_bytes == 0) {
            auto column = ColumnInt64::create();
            for (size_t r = 0; r < rows; ++r) {
                column->insert_value(static_cast<int64_t>(random() & 0x7fffffffffffffffULL));
            }
            block.insert({std::move(column), std::make_shared<DataTypeInt64>(), std::to_string(c)});
        } else {
            auto column = ColumnString::create();
            std::string value(string_bytes, 'x');
            for (size_t r = 0; r < rows; ++r) {
                if (random_strings) {
                    for (auto& ch : value) {
                        ch = static_cast<char>(random() & 0xff);
                    }
                } else {
                    // Distinct row prefixes followed by repetitive payload, like padded log fields.
                    const auto prefix = std::to_string(random());
                    value.replace(0, prefix.size(), prefix);
                }
                column->insert_data(value.data(), value.size());
            }
            block.insert(
                    {std::move(column), std::make_shared<DataTypeString>(), std::to_string(c)});
        }
    }
    return block;
}

// Measure the producer-side kernels used by UNPARTITIONED remote broadcast, including
// compression and the shared protobuf encoding for four receivers. No RPC or SQL is run.
// The baseline retains and reuses its MutableBlock capacity exactly like the sink does.
inline std::shared_ptr<BroadcastPBlockHolder> serialize_input(
        const Block& input, bool direct, segment_v2::CompressionTypePB codec,
        std::unique_ptr<MutableBlock>& buffer) {
    auto holder = BroadcastPBlockHolder::create_shared();
    Block merged;
    const Block* source = &input;
    if (!direct) {
        if (!buffer) {
            buffer = MutableBlock::create_unique(input.clone_empty());
        }
        auto status = buffer->merge(input);
        if (!status.ok()) {
            throw std::runtime_error(status.to_string());
        }
        merged = buffer->to_block();
        source = &merged;
    }
    size_t uncompressed_bytes = 0;
    size_t compressed_bytes = 0;
    int64_t compress_time = 0;
    auto status =
            source->serialize(BeExecVersionManager::get_newest_version(), holder->get_block(),
                              &uncompressed_bytes, &compressed_bytes, &compress_time, codec, false);
    if (!status.ok()) {
        throw std::runtime_error(status.to_string());
    }
    status = holder->prepare_serialized_block(true, 4);
    if (!status.ok()) {
        throw std::runtime_error(status.to_string());
    }
    if (!direct) {
        merged.clear_column_data();
        buffer->set_mutable_columns(std::move(merged).mutate_columns());
    }
    return holder;
}

inline void ExchangeBroadcastSerialize(benchmark::State& state) {
    const bool direct = state.range(0) != 0;
    const auto codec = state.range(4) == 0 ? segment_v2::NO_COMPRESSION : segment_v2::LZ4;
    std::vector<Block> inputs;
    for (uint64_t i = 0; i < 8; ++i) {
        inputs.push_back(make_input(state.range(1), state.range(2), state.range(3), 12345 + i));
    }
    std::unique_ptr<MutableBlock> buffer;
    size_t payload_bytes = 0;
    // Warm the baseline buffer, and reject comparisons with different serialized payloads.
    // Both validation and deterministic data generation are outside the timed loop.
    for (const auto& input : inputs) {
        auto baseline = serialize_input(input, false, codec, buffer);
        auto optimized = serialize_input(input, true, codec, buffer);
        if (baseline->get_block()->SerializeAsString() !=
            optimized->get_block()->SerializeAsString()) {
            throw std::runtime_error("Broadcast benchmark serialized payloads differ");
        }
        payload_bytes += optimized->get_block()->column_values().size();
    }
    if (direct) {
        buffer.reset();
    }
    size_t index = 0;
    for ([[maybe_unused]] auto _ : state) {
        auto holder = serialize_input(inputs[index++ % inputs.size()], direct, codec, buffer);
        benchmark::DoNotOptimize(holder);
        benchmark::ClobberMemory();
    }
    state.SetBytesProcessed(state.iterations() * static_cast<int64_t>(inputs[0].bytes()));
    state.SetItemsProcessed(state.iterations() * 4096);
    state.counters["input_bytes"] = static_cast<double>(inputs[0].bytes());
    state.counters["payload_bytes"] = static_cast<double>(payload_bytes) / inputs.size();
    state.counters["buffer_bytes"] = buffer ? static_cast<double>(buffer->allocated_bytes()) : 0;
}

inline void broadcast_arguments(benchmark::internal::Benchmark* benchmark) {
    for (int64_t codec : {0, 1}) {
        for (int64_t direct : {0, 1}) {
            benchmark->Args({direct, 8, 0, 1, codec});
            benchmark->Args({direct, 4, 32, 1, codec});
            benchmark->Args({direct, 4, 256, 0, codec});
            benchmark->Args({direct, 4, 256, 1, codec});
            benchmark->Args({direct, 16, 256, 0, codec});
            benchmark->Args({direct, 16, 256, 1, codec});
        }
    }
}

BENCHMARK(ExchangeBroadcastSerialize)
        ->Apply(broadcast_arguments)
        ->ArgNames({"direct", "cols", "str_bytes", "random", "lz4"});

} // namespace doris::broadcast_benchmark
