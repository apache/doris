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

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <benchmark/benchmark.h>
#include <parquet/api/reader.h>
#include <parquet/arrow/writer.h>

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <vector>

#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column_nullable.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "exprs/vcompound_pred.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format_v2/file_reader.h"
#include "format_v2/parquet/parquet_reader.h"
#include "gen_cpp/Types_types.h"
#include "io/io_common.h"
#include "parquet_benchmark_scenarios.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "storage/index/zone_map/zonemap_eval_context.h"
#include "storage/index/zone_map/zonemap_filter_result.h"

namespace doris::parquet_benchmark {
namespace reader_detail {

constexpr size_t READER_ROWS = 1UL << 14;
constexpr size_t READER_ROW_GROUP_ROWS = 1UL << 12;

inline void throw_if_error(const Status& status) {
    if (!status.ok()) {
        throw std::runtime_error(status.to_string());
    }
}

inline bool is_null_row(size_t row, int null_percent, Pattern pattern) {
    if (null_percent <= 0) {
        return false;
    }
    if (pattern == Pattern::ALTERNATING) {
        constexpr size_t NULL_PERIOD = 101;
        const size_t null_slots = (static_cast<size_t>(null_percent) * NULL_PERIOD + 99) / 100;
        return (row * 37) % NULL_PERIOD < null_slots;
    }
    constexpr size_t CLUSTER_ROWS = 1024;
    return row % CLUSTER_ROWS < CLUSTER_ROWS * static_cast<size_t>(null_percent) / 100;
}

inline std::shared_ptr<arrow::Array> build_int32_array(int null_percent, Pattern pattern) {
    arrow::Int32Builder builder;
    PARQUET_THROW_NOT_OK(builder.Reserve(READER_ROWS));
    for (size_t row = 0; row < READER_ROWS; ++row) {
        if (is_null_row(row, null_percent, pattern)) {
            PARQUET_THROW_NOT_OK(builder.AppendNull());
        } else {
            PARQUET_THROW_NOT_OK(builder.Append(static_cast<int32_t>(row % 100)));
        }
    }
    return builder.Finish().ValueOrDie();
}

inline ::parquet::Encoding::type file_encoding(Encoding encoding) {
    switch (encoding) {
    case Encoding::PLAIN:
        return ::parquet::Encoding::PLAIN;
    case Encoding::BYTE_STREAM_SPLIT:
        return ::parquet::Encoding::BYTE_STREAM_SPLIT;
    case Encoding::DELTA_BINARY_PACKED:
        return ::parquet::Encoding::DELTA_BINARY_PACKED;
    case Encoding::DICTIONARY:
        return ::parquet::Encoding::RLE_DICTIONARY;
    case Encoding::DELTA_LENGTH_BYTE_ARRAY:
        return ::parquet::Encoding::DELTA_LENGTH_BYTE_ARRAY;
    case Encoding::DELTA_BYTE_ARRAY:
        return ::parquet::Encoding::DELTA_BYTE_ARRAY;
    }
    throw std::logic_error("unknown Parquet benchmark encoding");
}

inline std::string fixture_name(const ReaderScenario& scenario) {
    return "v2_" + to_string(scenario.encoding) + "_null" + std::to_string(scenario.null_percent) +
           "_" + to_string(scenario.null_pattern) + "_w" + std::to_string(scenario.schema_width) +
           "_p" + std::to_string(scenario.predicate_position) + ".parquet";
}

inline void verify_fixture_encoding(const std::filesystem::path& path,
                                    const ReaderScenario& scenario) {
    auto reader = ::parquet::ParquetFileReader::OpenFile(path.string(), false);
    const auto metadata = reader->metadata();
    if (metadata->num_rows() != static_cast<int64_t>(READER_ROWS) ||
        metadata->num_columns() != scenario.schema_width) {
        throw std::runtime_error("Parquet benchmark fixture has unexpected shape: " +
                                 path.string());
    }
    const auto expected = file_encoding(scenario.encoding);
    for (int row_group = 0; row_group < metadata->num_row_groups(); ++row_group) {
        for (int column = 0; column < metadata->num_columns(); ++column) {
            const auto encodings = metadata->RowGroup(row_group)->ColumnChunk(column)->encodings();
            if (std::ranges::find(encodings, expected) == encodings.end()) {
                throw std::runtime_error("Parquet benchmark fixture did not use " +
                                         to_string(scenario.encoding) +
                                         " encoding: " + path.string());
            }
        }
    }
}

inline std::filesystem::path ensure_fixture(const ReaderScenario& scenario) {
    static std::mutex fixture_mutex;
    const auto directory =
            std::filesystem::temp_directory_path() / "doris_parquet_reader_benchmark";
    const auto path = directory / fixture_name(scenario);
    std::lock_guard guard(fixture_mutex);
    if (std::filesystem::exists(path)) {
        verify_fixture_encoding(path, scenario);
        return path;
    }

    std::filesystem::create_directories(directory);
    const auto temporary_path = path.string() + ".tmp";
    std::filesystem::remove(temporary_path);
    const auto values = build_int32_array(scenario.null_percent, scenario.null_pattern);
    std::vector<std::shared_ptr<arrow::Field>> fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;
    fields.reserve(scenario.schema_width);
    columns.reserve(scenario.schema_width);
    for (int column = 0; column < scenario.schema_width; ++column) {
        fields.push_back(arrow::field("c" + std::to_string(column), arrow::int32(), true));
        columns.push_back(std::make_shared<arrow::ChunkedArray>(values));
    }
    const auto table = arrow::Table::Make(arrow::schema(std::move(fields)), std::move(columns));

    const auto output_result = arrow::io::FileOutputStream::Open(temporary_path);
    if (!output_result.ok()) {
        throw std::runtime_error(output_result.status().ToString());
    }
    const auto output = *output_result;
    ::parquet::WriterProperties::Builder properties;
    properties.version(::parquet::ParquetVersion::PARQUET_2_6);
    properties.data_page_version(::parquet::ParquetDataPageVersion::V2);
    properties.compression(::parquet::Compression::UNCOMPRESSED);
    properties.disable_statistics();
    if (scenario.encoding == Encoding::DICTIONARY) {
        properties.enable_dictionary();
    } else {
        properties.disable_dictionary();
        properties.encoding(file_encoding(scenario.encoding));
    }
    PARQUET_THROW_NOT_OK(::parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), output,
                                                      READER_ROW_GROUP_ROWS, properties.build()));
    PARQUET_THROW_NOT_OK(output->Close());
    std::filesystem::rename(temporary_path, path);
    verify_fixture_encoding(path, scenario);
    return path;
}

class Int32LessThanExpr final : public VExpr {
public:
    Int32LessThanExpr(int column_id, int32_t upper_bound)
            : VExpr(std::make_shared<DataTypeUInt8>(), false),
              _column_id(column_id),
              _upper_bound(upper_bound) {}

    Status execute_column_impl(VExprContext*, const Block* block, const Selector* selector,
                               size_t count, ColumnPtr& result_column) const override {
        DORIS_CHECK(block != nullptr);
        const auto& nullable =
                assert_cast<const ColumnNullable&>(*block->get_by_position(_column_id).column);
        const auto& values = assert_cast<const ColumnInt32&>(nullable.get_nested_column());
        const auto& nulls = nullable.get_null_map_data();
        auto result = ColumnUInt8::create(count, 0);
        auto& matches = result->get_data();
        for (size_t row = 0; row < count; ++row) {
            const size_t input_row = selector == nullptr ? row : (*selector)[row];
            matches[row] = !nulls[input_row] && values.get_element(input_row) < _upper_bound;
        }
        result_column = std::move(result);
        return Status::OK();
    }

    bool can_execute_on_raw_fixed_values(const DataTypePtr& data_type,
                                         int column_id) const override {
        return column_id == _column_id &&
               remove_nullable(data_type)->get_primitive_type() == TYPE_INT;
    }

    Status execute_on_raw_fixed_values(const uint8_t* values, size_t num_values, size_t value_width,
                                       const DataTypePtr&, int column_id,
                                       uint8_t* matches) const override {
        DORIS_CHECK(column_id == _column_id);
        DORIS_CHECK(value_width == sizeof(int32_t));
        const auto* typed_values = reinterpret_cast<const int32_t*>(values);
        for (size_t row = 0; row < num_values; ++row) {
            matches[row] &= typed_values[row] < _upper_bound;
        }
        return Status::OK();
    }

    bool can_evaluate_zonemap_filter() const override { return true; }

    ZoneMapFilterResult evaluate_zonemap_filter(const ZoneMapEvalContext& ctx) const override {
        const auto zone_map = ctx.zone_map(_column_id);
        if (zone_map == nullptr) {
            return unsupported_zonemap_filter(ctx);
        }
        if (!zone_map->has_not_null) {
            return ZoneMapFilterResult::kNoMatch;
        }
        const auto upper_bound = Field::create_field<TYPE_INT>(_upper_bound);
        return zone_map->min_value >= upper_bound ? ZoneMapFilterResult::kNoMatch
                                                  : ZoneMapFilterResult::kMayMatch;
    }

    bool can_evaluate_dictionary_filter() const override { return true; }

    ZoneMapFilterResult evaluate_dictionary_filter(
            const DictionaryEvalContext& ctx) const override {
        const auto* dictionary = ctx.slot(_column_id);
        if (dictionary == nullptr) {
            return ZoneMapFilterResult::kUnsupported;
        }
        const auto upper_bound = Field::create_field<TYPE_INT>(_upper_bound);
        return std::ranges::any_of(dictionary->values,
                                   [&](const Field& value) { return value < upper_bound; })
                       ? ZoneMapFilterResult::kMayMatch
                       : ZoneMapFilterResult::kNoMatch;
    }

    void collect_slot_column_ids(std::set<int>& column_ids) const override {
        column_ids.insert(_column_id);
    }

    const std::string& expr_name() const override { return _expr_name; }

private:
    const int _column_id;
    const int32_t _upper_bound;
    const std::string _expr_name = "ParquetBenchmarkInt32LessThan";
};

inline VExprContextSPtr make_predicate(int column_position, int selectivity_percent) {
    auto context = VExprContext::create_shared(
            std::make_shared<Int32LessThanExpr>(column_position, selectivity_percent));
    context->_prepared = true;
    context->_opened = true;
    return context;
}

inline VExprSPtr make_int32_comparison(const std::string& function_name, TExprOpcode::type opcode,
                                       VExprSPtr left, VExprSPtr right) {
    const auto bool_type = make_nullable(std::make_shared<DataTypeUInt8>());
    TFunctionName name;
    name.__set_function_name(function_name);
    TFunction function;
    function.__set_name(name);
    function.__set_binary_type(TFunctionBinaryType::BUILTIN);
    function.__set_arg_types({left->data_type()->to_thrift(), right->data_type()->to_thrift()});
    function.__set_ret_type(bool_type->to_thrift());
    function.__set_has_var_args(false);
    TExprNode node;
    node.__set_node_type(TExprNodeType::BINARY_PRED);
    node.__set_opcode(opcode);
    node.__set_type(bool_type->to_thrift());
    node.__set_fn(function);
    node.__set_num_children(2);
    node.__set_is_nullable(true);
    auto comparison = VectorizedFnCall::create_shared(node);
    comparison->add_child(std::move(left));
    comparison->add_child(std::move(right));
    return comparison;
}

inline VExprContextSPtr make_complex_residual_predicate(int selectivity_percent, int first_position,
                                                        int later_left_position,
                                                        int later_right_position,
                                                        const DataTypePtr& int_type) {
    const auto bool_type = make_nullable(std::make_shared<DataTypeUInt8>());
    TExprNode node;
    node.__set_node_type(TExprNodeType::COMPOUND_PRED);
    node.__set_opcode(TExprOpcode::COMPOUND_AND);
    node.__set_type(bool_type->to_thrift());
    node.__set_num_children(2);
    node.__set_is_nullable(true);
    auto compound = VCompoundPred::create_shared(node);
    compound->add_child(make_int32_comparison(
            "lt", TExprOpcode::LT,
            VSlotRef::create_shared(first_position, first_position, -1, int_type, "c0"),
            VLiteral::create_shared(remove_nullable(int_type),
                                    Field::create_field<TYPE_INT>(selectivity_percent))));
    compound->add_child(make_int32_comparison(
            "eq", TExprOpcode::EQ,
            VSlotRef::create_shared(later_left_position, later_left_position, -1, int_type, "c2"),
            VSlotRef::create_shared(later_right_position, later_right_position, -1, int_type,
                                    "c3")));
    return VExprContext::create_shared(std::move(compound));
}

inline Block make_block(const std::vector<format::ColumnDefinition>& schema) {
    Block block;
    for (const auto& column : schema) {
        block.insert({column.type->create_column(), column.type, column.name});
    }
    return block;
}

struct ReaderSession {
    ~ReaderSession() {
        for (const auto& context : opened_conjuncts) {
            context->close();
        }
    }

    RuntimeState runtime_state {TQueryOptions(), TQueryGlobals()};
    std::unique_ptr<format::parquet::ParquetReader> reader;
    std::vector<format::ColumnDefinition> schema;
    std::shared_ptr<format::FileScanRequest> request;
    VExprContextSPtrs opened_conjuncts;
};

inline std::unique_ptr<ReaderSession> open_reader(const std::filesystem::path& path,
                                                  const ReaderScenario& scenario) {
    auto session = std::make_unique<ReaderSession>();
    auto properties = std::make_shared<io::FileSystemProperties>();
    properties->system_type = TFileType::FILE_LOCAL;
    auto description = std::make_unique<io::FileDescription>();
    description->path = path.string();
    description->file_size = static_cast<int64_t>(std::filesystem::file_size(path));
    description->range_start_offset = 0;
    description->range_size = -1;
    session->reader = std::make_unique<format::parquet::ParquetReader>(properties, description,
                                                                       nullptr, nullptr);
    throw_if_error(session->reader->init(&session->runtime_state));
    throw_if_error(session->reader->get_schema(&session->schema));

    session->request = std::make_shared<format::FileScanRequest>();
    format::FileScanRequestBuilder request_builder(session->request.get());
    if (scenario.operation == ReaderOperation::FULL_SCAN) {
        for (int column = 0; column < scenario.schema_width; ++column) {
            throw_if_error(request_builder.add_non_predicate_column(format::LocalColumnId(column)));
        }
    } else if (scenario.operation == ReaderOperation::PREDICATE_SCAN) {
        const auto predicate_id = format::LocalColumnId(scenario.predicate_position);
        throw_if_error(request_builder.add_predicate_column(predicate_id));
        if (scenario.projection == Projection::PREDICATE_ONLY) {
            session->request->predicate_only_columns.push_back(predicate_id);
        } else {
            const int payload = scenario.predicate_position == 0 ? scenario.schema_width - 1 : 0;
            throw_if_error(
                    request_builder.add_non_predicate_column(format::LocalColumnId(payload)));
        }
        const auto predicate_position = session->request->local_positions.at(predicate_id).value();
        session->request->conjuncts.push_back(
                make_predicate(static_cast<int>(predicate_position), scenario.selectivity_percent));
    } else if (scenario.operation == ReaderOperation::COMPLEX_RESIDUAL_SCAN) {
        DORIS_CHECK(scenario.schema_width >= 5);
        std::array<int, 3> predicate_columns {0, 2, 3};
        std::array<int, 3> predicate_positions {};
        for (size_t index = 0; index < predicate_columns.size(); ++index) {
            const int column = predicate_columns[index];
            const auto predicate_id = format::LocalColumnId(column);
            throw_if_error(request_builder.add_predicate_column(predicate_id));
            session->request->predicate_only_columns.push_back(predicate_id);
            predicate_positions[index] =
                    static_cast<int>(session->request->local_positions.at(predicate_id).value());
        }
        throw_if_error(request_builder.add_non_predicate_column(
                format::LocalColumnId(scenario.schema_width - 1)));
        auto context = make_complex_residual_predicate(
                scenario.selectivity_percent, predicate_positions[0], predicate_positions[1],
                predicate_positions[2], session->schema[0].type);
        throw_if_error(context->prepare(&session->runtime_state, RowDescriptor()));
        throw_if_error(context->open(&session->runtime_state));
        session->request->conjuncts.push_back(context);
        session->opened_conjuncts.push_back(std::move(context));
    } else {
        throw_if_error(request_builder.add_non_predicate_column(format::LocalColumnId(0)));
        if (scenario.schema_width > 1) {
            throw_if_error(request_builder.add_non_predicate_column(format::LocalColumnId(1)));
        }
    }

    if (scenario.operation == ReaderOperation::LIMIT_1) {
        session->reader->set_batch_size(1);
    } else if (scenario.operation == ReaderOperation::LIMIT_1000) {
        session->reader->set_batch_size(1000);
    }
    throw_if_error(session->reader->open(session->request));
    return session;
}

inline size_t scan_reader(ReaderSession* session, const ReaderScenario& scenario) {
    size_t output_rows = 0;
    bool eof = false;
    const size_t limit = scenario.operation == ReaderOperation::LIMIT_1      ? 1
                         : scenario.operation == ReaderOperation::LIMIT_1000 ? 1000
                                                                             : 0;
    while (!eof) {
        auto block = make_block(session->schema);
        size_t rows = 0;
        throw_if_error(session->reader->get_block(&block, &rows, &eof));
        output_rows += rows;
        benchmark::DoNotOptimize(block);
        if (scenario.operation == ReaderOperation::OPEN_TO_FIRST_BLOCK) {
            break;
        }
        if (limit != 0 && output_rows >= limit) {
            break;
        }
    }
    return output_rows;
}

inline size_t raw_rows_per_iteration(const ReaderScenario& scenario) {
    if (scenario.operation == ReaderOperation::LIMIT_1) {
        return 1;
    }
    if (scenario.operation == ReaderOperation::LIMIT_1000) {
        return 1000;
    }
    if (scenario.operation == ReaderOperation::OPEN_TO_FIRST_BLOCK) {
        return format::parquet::ParquetScanScheduler::DEFAULT_READ_BATCH_SIZE;
    }
    return READER_ROWS;
}

inline int projected_columns(const ReaderScenario& scenario) {
    if (scenario.operation == ReaderOperation::FULL_SCAN) {
        return scenario.schema_width;
    }
    if (scenario.operation == ReaderOperation::PREDICATE_SCAN &&
        scenario.projection == Projection::PREDICATE_ONLY) {
        return 1;
    }
    if (scenario.operation == ReaderOperation::COMPLEX_RESIDUAL_SCAN) {
        return 4;
    }
    return std::min(2, scenario.schema_width);
}

inline void run_reader(benchmark::State& state, ReaderScenario scenario) {
    std::filesystem::path fixture;
    try {
        fixture = ensure_fixture(scenario);
        size_t selected_rows = 0;
        for (auto _ : state) {
            if (scenario.operation != ReaderOperation::OPEN_TO_FIRST_BLOCK) {
                state.PauseTiming();
            }
            auto session = open_reader(fixture, scenario);
            if (scenario.operation != ReaderOperation::OPEN_TO_FIRST_BLOCK) {
                state.ResumeTiming();
            }
            selected_rows = scan_reader(session.get(), scenario);
            state.PauseTiming();
            throw_if_error(session->reader->close());
            state.ResumeTiming();
            benchmark::ClobberMemory();
        }

        const auto raw_rows = raw_rows_per_iteration(scenario);
        state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * selected_rows));
        state.SetBytesProcessed(static_cast<int64_t>(
                state.iterations() * raw_rows * projected_columns(scenario) * sizeof(int32_t)));
        state.counters["raw_rows"] = static_cast<double>(raw_rows);
        state.counters["selected_rows"] = static_cast<double>(selected_rows);
        state.counters["fixture_bytes"] = static_cast<double>(std::filesystem::file_size(fixture));
        state.counters["ns/raw_row"] = benchmark::Counter(
                static_cast<double>(raw_rows),
                benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
        if (selected_rows > 0) {
            state.counters["ns/selected_row"] = benchmark::Counter(
                    static_cast<double>(selected_rows),
                    benchmark::Counter::kIsIterationInvariantRate | benchmark::Counter::kInvert);
        }
    } catch (const std::exception& error) {
        state.SkipWithError(error.what());
    }
}

inline bool register_reader_benchmarks() {
    for (const auto& scenario : reader_scenarios()) {
        std::string name = "ParquetReader/" + reader_scenario_name(scenario);
        benchmark::RegisterBenchmark(name.c_str(), [=](benchmark::State& state) {
            run_reader(state, scenario);
        })->Unit(benchmark::kNanosecond);
    }
    return true;
}

inline const bool READER_BENCHMARKS_REGISTERED = register_reader_benchmarks();

} // namespace reader_detail
} // namespace doris::parquet_benchmark
