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

#include "exec/sink/writer/paimon/vpaimon_table_writer.h"

#include <gen_cpp/DataSinks_types.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/metrics/doris_metrics.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "exprs/vexpr.h"
#include "exprs/vexpr_context.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "util/defer_op.h"
#include "exec/sink/writer/paimon/paimon_writer_utils.h"
#include "exec/sink/writer/paimon/paimon_doris_hdfs_file_system.h"
#include "exec/sink/writer/paimon/vpaimon_partition_writer.h"

#ifdef WITH_PAIMON_CPP
#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/type.h>

#include <cstdint>

#include "format/arrow/arrow_block_convertor.h"
#include "format/arrow/arrow_row_batch.h"
#include "format/parquet/arrow_memory_pool.h"
#include "io/fs/hdfs_file_system.h"
#include "io/hdfs_builder.h"
#include "paimon/commit_message.h"
#include "paimon/factories/factory_creator.h"
#include "paimon/file_store_write.h"
#include "paimon/memory/memory_pool.h"
#include "paimon/metrics.h"
#include "paimon/utils/bucket_id_calculator.h"
#include "paimon/write_context.h"
#include "exec/sink/writer/paimon/paimon_doris_memory_pool.h"

// Force link paimon file format factories
namespace paimon {
namespace parquet {}
} // namespace paimon

#endif

namespace doris {

#ifdef WITH_PAIMON_CPP
namespace {
bool is_paimon_cpp_time_metric(std::string_view name) {
    return name.size() > 3 && name.substr(name.size() - 3) == "_ns";
}

void attach_paimon_cpp_metrics_to_profile(RuntimeProfile* profile,
                                          const std::shared_ptr<::paimon::Metrics>& metrics) {
    if (profile == nullptr || !metrics) {
        return;
    }
    auto all = metrics->GetAllCounters();
    if (all.empty()) {
        return;
    }
    for (const auto& kv : all) {
        std::string counter_name = "PaimonCpp_" + kv.first;
        std::replace(counter_name.begin(), counter_name.end(), '.', '_');
        RuntimeProfile::Counter* counter = nullptr;
        if (is_paimon_cpp_time_metric(kv.first)) {
            counter = ADD_COUNTER(profile, counter_name, TUnit::TIME_NS);
        } else {
            counter = ADD_COUNTER(profile, counter_name, TUnit::UNIT);
        }
        COUNTER_UPDATE(counter, kv.second);
    }
}
} // namespace
#endif

VPaimonTableWriter::~VPaimonTableWriter() = default;

VPaimonTableWriter::VPaimonTableWriter(const TDataSink& t_sink,
                                       const VExprContextSPtrs& output_exprs)
        : VPaimonTableWriter(t_sink, output_exprs, nullptr, nullptr) {}

VPaimonTableWriter::VPaimonTableWriter(const TDataSink& t_sink,
                                       const VExprContextSPtrs& output_exprs,
                                       std::shared_ptr<Dependency> dep,
                                       std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, std::move(dep), std::move(fin_dep)), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.paimon_table_sink);
}

Status VPaimonTableWriter::init_properties(ObjectPool* /*pool*/) {
    // Currently there is no extra property to initialize. Kept for symmetry
    // with VIcebergTableWriter and future paimon-cpp wiring.
    return Status::OK();
}

Status VPaimonTableWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _profile = profile;
#ifndef WITH_PAIMON_CPP
    return Status::NotSupported("paimon-cpp is not enabled");
#else
    _written_rows_counter = ADD_COUNTER(_profile, "WrittenRows", TUnit::UNIT);
    _written_bytes_counter = ADD_COUNTER(_profile, "WrittenBytes", TUnit::BYTES);
    _send_data_timer = ADD_TIMER(_profile, "SendDataTime");
    _project_timer = ADD_CHILD_TIMER(_profile, "ProjectTime", "SendDataTime");
    _bucket_calc_timer = ADD_CHILD_TIMER(_profile, "BucketCalcTime", "SendDataTime");
    _partition_writers_dispatch_timer =
            ADD_CHILD_TIMER(_profile, "PartitionsDispatchTime", "SendDataTime");
    _partition_writers_write_timer =
            ADD_CHILD_TIMER(_profile, "PartitionsWriteTime", "SendDataTime");
    _partition_writers_count = ADD_COUNTER(_profile, "PartitionsWriteCount", TUnit::UNIT);
    _partition_writer_created = ADD_COUNTER(_profile, "PartitionWriterCreated", TUnit::UNIT);
    _open_timer = ADD_TIMER(_profile, "OpenTime");
    _close_timer = ADD_TIMER(_profile, "CloseTime");
    _prepare_commit_timer = ADD_TIMER(_profile, "PrepareCommitTime");
    _serialize_commit_messages_timer = ADD_TIMER(_profile, "SerializeCommitMessagesTime");
    _commit_payload_bytes_counter = ADD_COUNTER(_profile, "CommitPayloadBytes", TUnit::BYTES);

    SCOPED_TIMER(_open_timer);

    ensure_paimon_doris_hdfs_file_system_registered();

    auto registered_types = paimon::FactoryCreator::GetInstance()->GetRegisteredType();
    std::string types_str;
    bool has_parquet = false;
    for (const auto& t : registered_types) {
        types_str += t + ", ";
        has_parquet |= (t == "parquet");
    }
    if (!has_parquet) {
        return Status::InternalError(
                "paimon-cpp parquet file format factory is not registered (missing 'parquet' in "
                "FactoryCreator). Please ensure BE is built with WITH_PAIMON_CPP=ON and linked "
                "with libpaimon_parquet_file_format.a (whole-archive). Registered factories: {}",
                types_str);
    }

    _pool = std::make_shared<PaimonDorisMemoryPool>(_state->query_mem_tracker());
    const auto& paimon_sink = _t_sink.paimon_table_sink;
    if (!paimon_sink.__isset.table_location || paimon_sink.table_location.empty()) {
        return Status::InvalidArgument("paimon table location is empty");
    }
    std::string commit_user;
    if (paimon_sink.__isset.options) {
        auto it = paimon_sink.options.find("doris.commit_user");
        if (it != paimon_sink.options.end()) {
            commit_user = it->second;
        }
    }
    if (commit_user.empty()) {
        commit_user = _state->user();
    }

    std::map<std::string, std::string> options;
    if (paimon_sink.__isset.options) {
        for (const auto& kv : paimon_sink.options) {
            if (kv.first.rfind("doris.", 0) == 0) {
                continue;
            }
            options.emplace(kv.first, kv.second);
        }
    }

    // Workaround for paimon-cpp issue where it defaults to LocalFileSystem if path has no scheme.
    // If table_location is missing scheme (common in HDFS setup without full URI),
    // and fs.defaultFS is provided in options, we prepend it.
    std::string table_location = paimon_sink.table_location;
    if (table_location.find("://") == std::string::npos) {
        auto it = options.find("fs.defaultFS");
        if (it != options.end()) {
            std::string default_fs = it->second;
            // Remove trailing slash from default_fs if present
            while (!default_fs.empty() && default_fs.back() == '/') {
                default_fs.pop_back();
            }
            // Remove leading slash from table_location if present
            if (!table_location.empty() && table_location.front() == '/') {
                table_location = default_fs + table_location;
            } else {
                table_location = default_fs + "/" + table_location;
            }
        }
    }

    int64_t buffer_size = 256 * 1024 * 1024L; // Default 256MB

    if (_state->query_options().__isset.paimon_write_buffer_size &&
        _state->query_options().paimon_write_buffer_size > 0) {
        buffer_size = _state->query_options().paimon_write_buffer_size;
    }

    bool enable_adaptive = true;
    if (_state->query_options().__isset.enable_paimon_adaptive_buffer_size) {
        enable_adaptive = _state->query_options().enable_paimon_adaptive_buffer_size;
    }

    if (enable_adaptive && paimon_sink.__isset.bucket_num && paimon_sink.bucket_num > 0) {
        int bucket_num = paimon_sink.bucket_num;
        buffer_size = get_paimon_write_buffer_size(buffer_size, true, bucket_num);
        LOG(INFO) << "Adaptive Paimon Buffer Size: bucket_num=" << bucket_num
                  << ", adjusted_buffer_size=" << buffer_size;
    }
    LOG(INFO) << "Paimon Native Writer Final Buffer Size: " << buffer_size
              << " (enable_adaptive=" << enable_adaptive << ")";
    options["write-buffer-size"] = std::to_string(buffer_size);

    if (_state->query_options().__isset.paimon_target_file_size &&
        _state->query_options().paimon_target_file_size > 0) {
        options["target-file-size"] =
                std::to_string(_state->query_options().paimon_target_file_size);
        LOG(INFO) << "Paimon Native Writer Target File Size: "
                  << _state->query_options().paimon_target_file_size;
    }

    auto file_format_it = options.find("file.format");
    if (file_format_it != options.end()) {
        std::string file_format = file_format_it->second;
        std::transform(file_format.begin(), file_format.end(), file_format.begin(),
                       [](unsigned char c) { return std::tolower(c); });
        if (file_format == "orc") {
            return Status::NotSupported(
                    "paimon-cpp native writer does not support ORC tables yet; "
                    "enable_paimon_jni_writer=true is required");
        }
    } else {
        options["file.format"] = "parquet";
    }
    if (options.find("manifest.format") == options.end()) {
        options["manifest.format"] = "parquet";
    }
    if ((!paimon_sink.__isset.bucket_num || paimon_sink.bucket_num <= 0) &&
        paimon_sink.__isset.bucket_keys && !paimon_sink.bucket_keys.empty()) {
        return Status::NotSupported(
                "paimon-cpp native writer does not support primary-key table with dynamic bucket "
                "(bucket=-1) yet; enable_paimon_jni_writer=true is required");
    }

    ::paimon::WriteContextBuilder builder(table_location, commit_user);
    builder.SetOptions(options);
    builder.WithIgnorePreviousFiles(true);
    builder.WithMemoryPool(_pool);
    builder.WithFileSystemSchemeToIdentifierMap(
            {{"hdfs", kPaimonDorisHdfsFsIdentifier}, {"dfs", kPaimonDorisHdfsFsIdentifier}});
    auto ctx_result = builder.Finish();
    if (!ctx_result.ok()) {
        return Status::InternalError("failed to build paimon write context: {}",
                                     ctx_result.status().ToString());
    }
    auto write_result = ::paimon::FileStoreWrite::Create(std::move(ctx_result).value());
    if (!write_result.ok()) {
        auto message = write_result.status().ToString();
        if (message.find("format 'orc'") != std::string::npos ||
            message.find("format \"orc\"") != std::string::npos) {
            return Status::InternalError(
                    "failed to create paimon file store write: {}. This Doris build does not "
                    "support ORC in paimon-cpp; create paimon table with options "
                    "file.format=parquet and manifest.format=parquet.",
                    message);
        }
        return Status::InternalError("failed to create paimon file store write: {}", message);
    }
    _file_store_write = std::move(write_result).value();
    return Status::OK();
#endif
}

Status VPaimonTableWriter::_init_partition_column_indices(const ::doris::Block& block) const {
    const TPaimonTableSink& paimon_sink = _t_sink.paimon_table_sink;
    if (!paimon_sink.__isset.partition_keys || paimon_sink.partition_keys.empty() ||
        _partition_indices_inited) {
        return Status::OK();
    }

    std::unordered_map<std::string, int> name_to_idx;
    for (int i = 0; i < block.columns(); ++i) {
        std::string col_name = block.get_by_position(i).name;
        if (col_name.empty()) {
            if (paimon_sink.__isset.column_names && i < paimon_sink.column_names.size()) {
                col_name = paimon_sink.column_names[i];
            }
        }
        name_to_idx.emplace(col_name, i);
    }
    _partition_column_indices.clear();
    _partition_column_indices.reserve(paimon_sink.partition_keys.size());
    for (const auto& key_name : paimon_sink.partition_keys) {
        auto it = name_to_idx.find(key_name);
        if (it == name_to_idx.end()) {
            return Status::InvalidArgument("paimon partition key {} not found in output block",
                                           key_name);
        }
        _partition_column_indices.push_back(it->second);
    }
    _partition_indices_inited = true;
    return Status::OK();
}

std::string VPaimonTableWriter::_default_partition_name() const {
    std::string default_part = "__DEFAULT_PARTITION__";
    const TPaimonTableSink& paimon_sink = _t_sink.paimon_table_sink;
    if (paimon_sink.__isset.options) {
        auto it = paimon_sink.options.find("partition.default-name");
        if (it != paimon_sink.options.end() && !it->second.empty()) {
            default_part = it->second;
        }
    }
    return default_part;
}

Status VPaimonTableWriter::_collect_partition_value_columns(
        const ::doris::Block& block,
        std::vector<std::vector<std::string>>* partition_value_columns) const {
    partition_value_columns->clear();
    const TPaimonTableSink& paimon_sink = _t_sink.paimon_table_sink;
    if (!paimon_sink.__isset.partition_keys || paimon_sink.partition_keys.empty()) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_init_partition_column_indices(block));
    const size_t rows = block.rows();
    partition_value_columns->resize(_partition_column_indices.size());
    const std::string default_part = _default_partition_name();
    DataTypeSerDe::FormatOptions options;
    for (size_t part_idx = 0; part_idx < _partition_column_indices.size(); ++part_idx) {
        auto& values = (*partition_value_columns)[part_idx];
        values.resize(rows);
        const auto& col_with_type = block.get_by_position(_partition_column_indices[part_idx]);
        const auto& type = col_with_type.type;
        const auto& col = col_with_type.column;
        for (size_t row = 0; row < rows; ++row) {
            if (col->is_null_at(row)) {
                values[row] = default_part;
            } else {
                values[row] = type->to_string(*col, row, options);
            }
        }
    }
    return Status::OK();
}

Status VPaimonTableWriter::_get_or_create_writer(const WriteKey& key,
                                                 std::shared_ptr<VPaimonPartitionWriter>* writer) {
    auto it = _writers.find(key);
    if (it != _writers.end()) {
        *writer = it->second;
        return Status::OK();
    }

    auto new_writer =
            std::make_shared<VPaimonPartitionWriter>(_t_sink, key.partition_values, key.bucket_id
#ifdef WITH_PAIMON_CPP
                                                     ,
                                                     _file_store_write.get(), _pool
#endif
            );
    RETURN_IF_ERROR(new_writer->init_properties(nullptr));
    RETURN_IF_ERROR(new_writer->open(_state, _profile));
    COUNTER_UPDATE(_partition_writer_created, 1);
    DorisMetrics::instance()->paimon_partition_writer_created->increment(1);
    _writers.emplace(key, new_writer);
    *writer = std::move(new_writer);
    return Status::OK();
}

Status VPaimonTableWriter::write(RuntimeState* state, ::doris::Block& block) {
#ifndef WITH_PAIMON_CPP
    return Status::NotSupported("paimon-cpp is not enabled");
#else
    if (block.rows() == 0) {
        return Status::OK();
    }
    SCOPED_TIMER(_send_data_timer);
    int64_t send_data_ns = 0;
    auto to_ms_ceil = [](int64_t ns) -> uint64_t {
        if (ns <= 0) {
            return 0;
        }
        return static_cast<uint64_t>((ns + 999999) / 1000000);
    };
    Defer record_send_data_latency {[&]() {
        DorisMetrics::instance()->paimon_write_send_data_latency_ms->add(to_ms_ceil(send_data_ns));
    }};
    SCOPED_RAW_TIMER(&send_data_ns);

    Block output_block;
    int64_t project_ns = 0;
    {
        SCOPED_TIMER(_project_timer);
        SCOPED_RAW_TIMER(&project_ns);
        RETURN_IF_ERROR(_projection_block(block, &output_block));
    }
    _row_count += output_block.rows();
    COUNTER_UPDATE(_written_rows_counter, output_block.rows());
    COUNTER_UPDATE(_written_bytes_counter, output_block.bytes());
    DorisMetrics::instance()->paimon_write_rows->increment(output_block.rows());
    DorisMetrics::instance()->paimon_write_bytes->increment(output_block.bytes());

    static_cast<void>(output_block.get_columns_and_convert());
    const size_t rows = output_block.rows();
    std::vector<int32_t> bucket_ids(rows, 0);
    const TPaimonTableSink& paimon_sink = _t_sink.paimon_table_sink;
    if (paimon_sink.__isset.bucket_num && paimon_sink.bucket_num > 0) {
        int64_t bucket_calc_ns = 0;
        SCOPED_TIMER(_bucket_calc_timer);
        SCOPED_RAW_TIMER(&bucket_calc_ns);

        // Optimization: Check if bucket_id is already calculated and attached as a hidden column
        int bucket_pos = output_block.get_position_by_name("__paimon_bucket_id");
        if (bucket_pos >= 0) {
            const auto& bucket_col = *output_block.get_by_position(bucket_pos).column;
            for (int i = 0; i < rows; ++i) {
                bucket_ids[i] = static_cast<int32_t>(static_cast<int32_t>(bucket_col.get_int(i)));
            }
        } else {
            // Fallback to calculation if not found
            if (!paimon_sink.__isset.bucket_keys || paimon_sink.bucket_keys.empty()) {
                return Status::InvalidArgument("paimon fixed bucket mode requires bucket_keys");
            }

            std::unordered_map<std::string, int> name_to_idx;
            name_to_idx.reserve(output_block.columns());
            for (int i = 0; i < output_block.columns(); ++i) {
                std::string col_name = output_block.get_by_position(i).name;
                if (col_name.empty()) {
                    if (paimon_sink.__isset.column_names && i < paimon_sink.column_names.size()) {
                        col_name = paimon_sink.column_names[i];
                    }
                }
                name_to_idx.emplace(col_name, i);
            }

            Block bucket_key_block;
            for (const auto& key_name : paimon_sink.bucket_keys) {
                auto it = name_to_idx.find(key_name);
                if (it == name_to_idx.end()) {
                    return Status::InvalidArgument("paimon bucket key {} not found in output block",
                                                   key_name);
                }
                bucket_key_block.insert(output_block.get_by_position(it->second));
            }

            ::doris::ArrowMemoryPool<> arrow_pool;
            std::shared_ptr<arrow::Schema> arrow_schema;
            RETURN_IF_ERROR(get_arrow_schema_from_block(bucket_key_block, &arrow_schema,
                                                        _state->timezone()));
            std::shared_ptr<arrow::RecordBatch> record_batch;
            RETURN_IF_ERROR(convert_to_arrow_batch(bucket_key_block, arrow_schema, &arrow_pool,
                                                   &record_batch, _state->timezone_obj()));

            std::vector<std::shared_ptr<arrow::Field>> bucket_fields = arrow_schema->fields();
            std::vector<std::shared_ptr<arrow::Array>> bucket_columns = record_batch->columns();

            auto bucket_struct_res = arrow::StructArray::Make(bucket_columns, bucket_fields);
            if (!bucket_struct_res.ok()) {
                return Status::InternalError("failed to build bucket struct array: {}",
                                             bucket_struct_res.status().ToString());
            }
            std::shared_ptr<arrow::Array> bucket_struct = bucket_struct_res.ValueOrDie();
            std::shared_ptr<arrow::Schema> bucket_schema = arrow::schema(bucket_fields);

            ArrowArray c_bucket_array;
            auto arrow_status = arrow::ExportArray(*bucket_struct, &c_bucket_array);
            if (!arrow_status.ok()) {
                return Status::InternalError("failed to export bucket arrow array: {}",
                                             arrow_status.ToString());
            }
            ArrowSchema c_bucket_schema;
            arrow_status = arrow::ExportSchema(*bucket_schema, &c_bucket_schema);
            if (!arrow_status.ok()) {
                if (c_bucket_array.release) {
                    c_bucket_array.release(&c_bucket_array);
                }
                return Status::InternalError("failed to export bucket arrow schema: {}",
                                             arrow_status.ToString());
            }

            auto calc_res =
                    ::paimon::BucketIdCalculator::Create(false, paimon_sink.bucket_num, _pool);
            if (!calc_res.ok()) {
                if (c_bucket_array.release) {
                    c_bucket_array.release(&c_bucket_array);
                }
                if (c_bucket_schema.release) {
                    c_bucket_schema.release(&c_bucket_schema);
                }
                return Status::InternalError("failed to create paimon bucket calculator: {}",
                                             calc_res.status().ToString());
            }
            auto paimon_st = calc_res.value()->CalculateBucketIds(&c_bucket_array, &c_bucket_schema,
                                                                  bucket_ids.data());
            if (c_bucket_array.release) {
                c_bucket_array.release(&c_bucket_array);
            }
            if (c_bucket_schema.release) {
                c_bucket_schema.release(&c_bucket_schema);
            }
            if (!paimon_st.ok()) {
                return Status::InternalError("failed to calculate paimon bucket ids: {}",
                                             paimon_st.ToString());
            }
        }
        DorisMetrics::instance()->paimon_write_bucket_calc_latency_ms->add(
                to_ms_ceil(bucket_calc_ns));
    }

    int64_t dispatch_ns = 0;
    int64_t partitions_write_ns = 0;
    struct PartitionDispatch {
        WriteKey key;
        std::vector<uint32_t> row_indices;
    };
    std::vector<PartitionDispatch> partitions;
    partitions.reserve(rows);
    {
        SCOPED_TIMER(_partition_writers_dispatch_timer);
        SCOPED_RAW_TIMER(&dispatch_ns);
        std::vector<std::vector<std::string>> partition_value_columns;
        RETURN_IF_ERROR(_collect_partition_value_columns(output_block, &partition_value_columns));
        std::unordered_map<size_t, std::vector<size_t>> partition_hash_to_indices;
        partition_hash_to_indices.reserve(rows);
        auto hash_row = [&](uint32_t row) {
            size_t hash = std::hash<int32_t> {}(bucket_ids[row]);
            for (const auto& values : partition_value_columns) {
                hash = hash * 31 + std::hash<std::string_view> {}(values[row]);
            }
            return hash;
        };
        auto match_partition = [&](const WriteKey& key, uint32_t row) {
            if (key.bucket_id != bucket_ids[row] ||
                key.partition_values.size() != partition_value_columns.size()) {
                return false;
            }
            for (size_t i = 0; i < partition_value_columns.size(); ++i) {
                if (key.partition_values[i] != partition_value_columns[i][row]) {
                    return false;
                }
            }
            return true;
        };
        auto materialize_key = [&](uint32_t row) {
            WriteKey key;
            key.bucket_id = bucket_ids[row];
            key.partition_values.reserve(partition_value_columns.size());
            for (const auto& values : partition_value_columns) {
                key.partition_values.emplace_back(values[row]);
            }
            return key;
        };
        for (uint32_t i = 0; i < static_cast<uint32_t>(rows); ++i) {
            const size_t hash = hash_row(i);
            size_t partition_idx = partitions.size();
            auto it = partition_hash_to_indices.find(hash);
            if (it != partition_hash_to_indices.end()) {
                for (size_t candidate_idx : it->second) {
                    if (match_partition(partitions[candidate_idx].key, i)) {
                        partition_idx = candidate_idx;
                        break;
                    }
                }
            }
            if (partition_idx == partitions.size()) {
                PartitionDispatch dispatch;
                dispatch.key = materialize_key(i);
                partitions.push_back(std::move(dispatch));
                partition_hash_to_indices[hash].push_back(partition_idx);
            }
            partitions[partition_idx].row_indices.push_back(i);
        }
    }

    COUNTER_UPDATE(_partition_writers_count, partitions.size());

    {
        SCOPED_TIMER(_partition_writers_write_timer);
        SCOPED_RAW_TIMER(&partitions_write_ns);
        const int cols = output_block.columns();
        for (auto& partition : partitions) {
            std::shared_ptr<VPaimonPartitionWriter> writer;
            RETURN_IF_ERROR(_get_or_create_writer(partition.key, &writer));
            auto columns = output_block.clone_empty_columns();
            const uint32_t* begin = partition.row_indices.data();
            const uint32_t* end = begin + partition.row_indices.size();
            for (int col = 0; col < cols; ++col) {
                columns[col]->insert_indices_from(*output_block.get_by_position(col).column, begin,
                                                  end);
            }
            Block gathered_block = output_block.clone_with_columns(std::move(columns));
            RETURN_IF_ERROR(writer->write(gathered_block));
        }
    }
    _state->update_num_rows_load_total(output_block.rows());
    _state->update_num_bytes_load_total(output_block.bytes());
    DorisMetrics::instance()->paimon_write_project_latency_ms->add(to_ms_ceil(project_ns));
    DorisMetrics::instance()->paimon_write_dispatch_latency_ms->add(to_ms_ceil(dispatch_ns));
    DorisMetrics::instance()->paimon_write_partitions_latency_ms->add(
            to_ms_ceil(partitions_write_ns));
    return Status::OK();
#endif
}

Status VPaimonTableWriter::_filter_block(::doris::Block& block, const IColumn::Filter* filter,
                                         ::doris::Block* output_block) {
    *output_block = block;
    std::vector<uint32_t> columns_to_filter;
    int column_to_keep = output_block->columns();
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }
    Block::filter_block_internal(output_block, columns_to_filter, *filter);
    return Status::OK();
}

Status VPaimonTableWriter::close(Status status) {
#ifndef WITH_PAIMON_CPP
    return status;
#else
    SCOPED_TIMER(_close_timer);
    Status result_status;
    for (auto& it : _writers) {
        Status st = it.second->close(status);
        if (!st.ok() && result_status.ok()) {
            result_status = st;
        }
    }

    if (_file_store_write) {
        std::vector<TPaimonCommitMessage> msgs;
        if (status.ok() && result_status.ok()) {
            int64_t prepare_commit_ns = 0;
            auto commit_result = [&]() {
                SCOPED_TIMER(_prepare_commit_timer);
                SCOPED_RAW_TIMER(&prepare_commit_ns);
                return _file_store_write->PrepareCommit();
            }();
            attach_paimon_cpp_metrics_to_profile(_profile, _file_store_write->GetMetrics());
            if (!commit_result.ok()) {
                return Status::InternalError("paimon prepare commit failed: {}",
                                             commit_result.status().ToString());
            }
            const auto& commit_messages = commit_result.value();
            DorisMetrics::instance()->paimon_prepare_commit_messages->increment(
                    commit_messages.size());
            constexpr size_t kMaxPayloadBytes = 8 * 1024 * 1024;
            size_t chunk_size = 512;
            size_t total_messages = commit_messages.size();
            size_t total_payload_bytes = 0;
            int32_t serializer_version = ::paimon::CommitMessage::CurrentVersion();
            size_t i = 0;
            {
                SCOPED_TIMER(_serialize_commit_messages_timer);
                int64_t serialize_ns = 0;
                Defer record_serialize_commit_messages_latency {[&]() {
                    DorisMetrics::instance()->paimon_serialize_commit_messages_latency_ms->add(
                            static_cast<uint64_t>(
                                    serialize_ns <= 0 ? 0 : (serialize_ns + 999999) / 1000000));
                }};
                SCOPED_RAW_TIMER(&serialize_ns);
                while (i < total_messages) {
                    size_t end = std::min(i + chunk_size, total_messages);
                    std::vector<std::shared_ptr<::paimon::CommitMessage>> chunk;
                    chunk.reserve(end - i);
                    for (size_t j = i; j < end; ++j) {
                        chunk.emplace_back(commit_messages[j]);
                    }
                    auto payload_result = ::paimon::CommitMessage::SerializeList(chunk, _pool);
                    if (!payload_result.ok()) {
                        return Status::InternalError("paimon serialize commit messages failed: {}",
                                                     payload_result.status().ToString());
                    }
                    std::string raw_payload = std::move(payload_result).value();
                    if (raw_payload.size() + 12 > kMaxPayloadBytes && chunk_size > 1) {
                        chunk_size = std::max<size_t>(1, chunk_size / 2);
                        continue;
                    }

                    std::string wrapped;
                    wrapped.reserve(12 + raw_payload.size());
                    wrapped.append("DPCM", 4);
                    auto append_i32_be = [&wrapped](int32_t v) {
                        uint32_t u = static_cast<uint32_t>(v);
                        wrapped.push_back(static_cast<char>((u >> 24) & 0xFF));
                        wrapped.push_back(static_cast<char>((u >> 16) & 0xFF));
                        wrapped.push_back(static_cast<char>((u >> 8) & 0xFF));
                        wrapped.push_back(static_cast<char>(u & 0xFF));
                    };
                    append_i32_be(serializer_version);
                    append_i32_be(static_cast<int32_t>(raw_payload.size()));
                    wrapped.append(raw_payload);

                    total_payload_bytes += wrapped.size();
                    COUNTER_UPDATE(_commit_payload_bytes_counter, wrapped.size());
                    DorisMetrics::instance()->paimon_commit_payload_bytes->increment(
                            wrapped.size());
                    DorisMetrics::instance()->paimon_commit_payload_chunks->increment(1);
                    TPaimonCommitMessage msg;
                    msg.__set_payload(wrapped);
                    msgs.emplace_back(std::move(msg));
                    i = end;
                }
            }
            DorisMetrics::instance()->paimon_prepare_commit_latency_ms->add(static_cast<uint64_t>(
                    prepare_commit_ns <= 0 ? 0 : (prepare_commit_ns + 999999) / 1000000));
            VLOG(1) << "Prepared " << total_messages << " commit messages, serialized as "
                    << msgs.size() << " payload chunks, total_payload_bytes=" << total_payload_bytes
                    << ", serializer_version=" << serializer_version;
        }
        auto close_status = _file_store_write->Close();
        if (!close_status.ok()) {
            return Status::InternalError("paimon file store write close failed: {}",
                                         close_status.ToString());
        }
        if (!msgs.empty()) {
            _state->add_paimon_commit_messages(msgs);
        }
    }
    _writers.clear();
    if (!result_status.ok()) {
        return result_status;
    }
    return status;
#endif
}

} // namespace doris
