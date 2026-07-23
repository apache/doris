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

#include <cstddef>
#include <cstdint>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace doris::parquet_benchmark {

enum class Encoding {
    PLAIN,
    DICTIONARY,
    BYTE_STREAM_SPLIT,
    DELTA_BINARY_PACKED,
    DELTA_LENGTH_BYTE_ARRAY,
    DELTA_BYTE_ARRAY
};
enum class ValueType { INT32, INT64, FLOAT, DOUBLE, BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY };
enum class Pattern { CLUSTERED, ALTERNATING };
enum class Projection { PREDICATE_ONLY, PREDICATE_PROJECTED };
enum class ReaderOperation {
    OPEN_TO_FIRST_BLOCK,
    FULL_SCAN,
    PREDICATE_SCAN,
    COMPLEX_RESIDUAL_SCAN,
    LIMIT_1,
    LIMIT_1000
};
enum class Kernel {
    BYTE_STREAM_SPLIT,
    DELTA_PREFIX_SUM,
    DICTIONARY_GATHER,
    NULLABLE_EXPAND,
    RAW_PREDICATE
};

struct DecoderScenario {
    Encoding encoding;
    ValueType value_type;
};

struct ReaderScenario {
    ReaderOperation operation;
    Encoding encoding;
    int null_percent;
    Pattern null_pattern;
    int selectivity_percent;
    Projection projection;
    int schema_width;
    int predicate_position;
};

struct KernelScenario {
    Kernel kernel;
    ValueType value_type;
    int selectivity_percent;
    int null_percent;
    Pattern pattern;
    size_t dictionary_entries;
};

struct SelectionRange {
    size_t first;
    size_t count;
};

struct SelectionPlan {
    size_t total_rows = 0;
    size_t selected_rows = 0;
    std::vector<SelectionRange> ranges;
};

inline std::vector<DecoderScenario> decoder_scenarios() {
    return {
            {Encoding::PLAIN, ValueType::INT32},
            {Encoding::PLAIN, ValueType::INT64},
            {Encoding::PLAIN, ValueType::FLOAT},
            {Encoding::PLAIN, ValueType::DOUBLE},
            {Encoding::PLAIN, ValueType::BYTE_ARRAY},
            {Encoding::PLAIN, ValueType::FIXED_LEN_BYTE_ARRAY},
            {Encoding::DICTIONARY, ValueType::INT32},
            {Encoding::DICTIONARY, ValueType::INT64},
            {Encoding::DICTIONARY, ValueType::FLOAT},
            {Encoding::DICTIONARY, ValueType::DOUBLE},
            {Encoding::DICTIONARY, ValueType::BYTE_ARRAY},
            {Encoding::DICTIONARY, ValueType::FIXED_LEN_BYTE_ARRAY},
            {Encoding::BYTE_STREAM_SPLIT, ValueType::FLOAT},
            {Encoding::BYTE_STREAM_SPLIT, ValueType::DOUBLE},
            {Encoding::BYTE_STREAM_SPLIT, ValueType::FIXED_LEN_BYTE_ARRAY},
            {Encoding::DELTA_BINARY_PACKED, ValueType::INT32},
            {Encoding::DELTA_BINARY_PACKED, ValueType::INT64},
            {Encoding::DELTA_LENGTH_BYTE_ARRAY, ValueType::BYTE_ARRAY},
            {Encoding::DELTA_BYTE_ARRAY, ValueType::BYTE_ARRAY},
    };
}

inline std::vector<KernelScenario> kernel_scenarios() {
    std::vector<KernelScenario> scenarios;
    for (const auto value_type : {ValueType::FLOAT, ValueType::DOUBLE}) {
        scenarios.push_back(
                {Kernel::BYTE_STREAM_SPLIT, value_type, 100, 0, Pattern::CLUSTERED, 256});
    }
    for (const auto value_type : {ValueType::INT32, ValueType::INT64}) {
        scenarios.push_back(
                {Kernel::DELTA_PREFIX_SUM, value_type, 100, 0, Pattern::CLUSTERED, 256});
    }
    for (const auto value_type :
         {ValueType::INT32, ValueType::INT64, ValueType::FLOAT, ValueType::DOUBLE}) {
        for (const size_t dictionary_entries : {32, 4096, 262144}) {
            scenarios.push_back({Kernel::DICTIONARY_GATHER, value_type, 100, 0, Pattern::CLUSTERED,
                                 dictionary_entries});
        }
        for (const int null_percent : {0, 1, 10, 50, 90}) {
            for (const auto pattern : {Pattern::CLUSTERED, Pattern::ALTERNATING}) {
                scenarios.push_back(
                        {Kernel::NULLABLE_EXPAND, value_type, 100, null_percent, pattern, 256});
            }
        }
        for (const int selectivity : {0, 1, 10, 50, 90, 100}) {
            scenarios.push_back(
                    {Kernel::RAW_PREDICATE, value_type, selectivity, 0, Pattern::ALTERNATING, 256});
        }
    }
    return scenarios;
}

inline std::vector<ReaderScenario> reader_scenarios() {
    std::vector<ReaderScenario> scenarios;
    std::set<std::tuple<ReaderOperation, Encoding, int, Pattern, int, Projection, int, int>> seen;
    const auto add = [&](ReaderScenario scenario) {
        const auto key = std::make_tuple(scenario.operation, scenario.encoding,
                                         scenario.null_percent, scenario.null_pattern,
                                         scenario.selectivity_percent, scenario.projection,
                                         scenario.schema_width, scenario.predicate_position);
        if (seen.insert(key).second) {
            scenarios.push_back(scenario);
        }
    };

    const ReaderScenario baseline {.operation = ReaderOperation::FULL_SCAN,
                                   .encoding = Encoding::PLAIN,
                                   .null_percent = 10,
                                   .null_pattern = Pattern::ALTERNATING,
                                   .selectivity_percent = 10,
                                   .projection = Projection::PREDICATE_PROJECTED,
                                   .schema_width = 32,
                                   .predicate_position = 0};
    for (const auto operation :
         {ReaderOperation::OPEN_TO_FIRST_BLOCK, ReaderOperation::FULL_SCAN,
          ReaderOperation::PREDICATE_SCAN, ReaderOperation::COMPLEX_RESIDUAL_SCAN,
          ReaderOperation::LIMIT_1, ReaderOperation::LIMIT_1000}) {
        auto scenario = baseline;
        scenario.operation = operation;
        add(scenario);
    }
    for (const auto encoding : {Encoding::PLAIN, Encoding::DICTIONARY, Encoding::BYTE_STREAM_SPLIT,
                                Encoding::DELTA_BINARY_PACKED}) {
        auto scenario = baseline;
        scenario.encoding = encoding;
        add(scenario);
        scenario.operation = ReaderOperation::PREDICATE_SCAN;
        add(scenario);
    }
    for (const auto encoding : {Encoding::BYTE_STREAM_SPLIT, Encoding::DELTA_BINARY_PACKED}) {
        for (const int selectivity : {1, 10, 50, 90}) {
            for (const auto projection :
                 {Projection::PREDICATE_ONLY, Projection::PREDICATE_PROJECTED}) {
                auto scenario = baseline;
                scenario.operation = ReaderOperation::PREDICATE_SCAN;
                scenario.encoding = encoding;
                scenario.selectivity_percent = selectivity;
                scenario.projection = projection;
                add(scenario);
            }
        }
    }
    for (const int width : {4, 32, 128, 512}) {
        for (const int predicate_position : {0, width - 1}) {
            auto scenario = baseline;
            scenario.operation = ReaderOperation::PREDICATE_SCAN;
            scenario.schema_width = width;
            scenario.predicate_position = predicate_position;
            add(scenario);
        }
    }
    for (const int null_percent : {0, 1, 10, 50, 90}) {
        for (const auto pattern : {Pattern::CLUSTERED, Pattern::ALTERNATING}) {
            for (const int selectivity : {0, 1, 10, 50, 90, 100}) {
                for (const auto projection :
                     {Projection::PREDICATE_ONLY, Projection::PREDICATE_PROJECTED}) {
                    auto scenario = baseline;
                    scenario.operation = ReaderOperation::PREDICATE_SCAN;
                    scenario.null_percent = null_percent;
                    scenario.null_pattern = pattern;
                    scenario.selectivity_percent = selectivity;
                    scenario.projection = projection;
                    add(scenario);
                }
            }
        }
    }
    return scenarios;
}

inline SelectionPlan make_selection_plan(size_t total_rows, int selectivity_percent,
                                         Pattern pattern) {
    SelectionPlan plan {.total_rows = total_rows, .selected_rows = 0, .ranges = {}};
    if (total_rows == 0 || selectivity_percent <= 0) {
        return plan;
    }
    if (selectivity_percent >= 100) {
        plan.selected_rows = total_rows;
        plan.ranges.push_back({.first = 0, .count = total_rows});
        return plan;
    }
    plan.selected_rows = total_rows * static_cast<size_t>(selectivity_percent) / 100;
    if (plan.selected_rows == 0) {
        plan.selected_rows = 1;
    }
    if (pattern == Pattern::CLUSTERED) {
        plan.ranges.push_back({.first = 0, .count = plan.selected_rows});
        return plan;
    }

    // Evenly spaced rows deliberately maximize the number of physical ranges. This is the
    // adversarial sparse shape that exposes per-run decoder and cursor overhead.
    for (size_t selected = 0; selected < plan.selected_rows; ++selected) {
        const size_t row = selected * total_rows / plan.selected_rows;
        if (!plan.ranges.empty() && plan.ranges.back().first + plan.ranges.back().count == row) {
            ++plan.ranges.back().count;
        } else {
            plan.ranges.push_back({.first = row, .count = 1});
        }
    }
    return plan;
}

template <typename Visitor>
inline void visit_selected_rows(const SelectionPlan& plan, Visitor visitor) {
    for (const auto& range : plan.ranges) {
        for (size_t offset = 0; offset < range.count; ++offset) {
            visitor(range.first + offset);
        }
    }
}

inline std::string to_string(Encoding value) {
    switch (value) {
    case Encoding::PLAIN:
        return "plain";
    case Encoding::DICTIONARY:
        return "dictionary";
    case Encoding::BYTE_STREAM_SPLIT:
        return "byte_stream_split";
    case Encoding::DELTA_BINARY_PACKED:
        return "delta_binary_packed";
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
        return "delta_length_byte_array";
    case Encoding::DELTA_BYTE_ARRAY:
        return "delta_byte_array";
    }
    return "unknown";
}

inline std::string to_string(ValueType value) {
    switch (value) {
    case ValueType::INT32:
        return "int32";
    case ValueType::INT64:
        return "int64";
    case ValueType::FLOAT:
        return "float";
    case ValueType::DOUBLE:
        return "double";
    case ValueType::BYTE_ARRAY:
        return "byte_array";
    case ValueType::FIXED_LEN_BYTE_ARRAY:
        return "fixed_len_byte_array";
    }
    return "unknown";
}

inline std::string to_string(Pattern value) {
    return value == Pattern::CLUSTERED ? "clustered" : "alternating";
}

inline std::string to_string(Projection value) {
    return value == Projection::PREDICATE_ONLY ? "predicate_only" : "predicate_projected";
}

inline std::string to_string(ReaderOperation value) {
    switch (value) {
    case ReaderOperation::OPEN_TO_FIRST_BLOCK:
        return "open_to_first_block";
    case ReaderOperation::FULL_SCAN:
        return "full_scan";
    case ReaderOperation::PREDICATE_SCAN:
        return "predicate_scan";
    case ReaderOperation::COMPLEX_RESIDUAL_SCAN:
        return "complex_residual_scan";
    case ReaderOperation::LIMIT_1:
        return "limit_1";
    case ReaderOperation::LIMIT_1000:
        return "limit_1000";
    }
    return "unknown";
}

inline std::string reader_scenario_name(const ReaderScenario& scenario) {
    return to_string(scenario.operation) + "/" + to_string(scenario.encoding) + "/null_" +
           std::to_string(scenario.null_percent) + "/" + to_string(scenario.null_pattern) +
           "/sel_" + std::to_string(scenario.selectivity_percent) + "/" +
           to_string(scenario.projection) + "/width_" + std::to_string(scenario.schema_width) +
           "/predicate_" + std::to_string(scenario.predicate_position);
}

inline std::string to_string(Kernel value) {
    switch (value) {
    case Kernel::BYTE_STREAM_SPLIT:
        return "byte_stream_split";
    case Kernel::DELTA_PREFIX_SUM:
        return "delta_prefix_sum";
    case Kernel::DICTIONARY_GATHER:
        return "dictionary_gather";
    case Kernel::NULLABLE_EXPAND:
        return "nullable_expand";
    case Kernel::RAW_PREDICATE:
        return "raw_predicate";
    }
    return "unknown";
}

} // namespace doris::parquet_benchmark
