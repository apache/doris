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

#include "exec/sink/viceberg_delete_sink.h"

#include <fmt/format.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <zlib.h>

#include "common/logging.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/data_type/data_type_string.h"
#include "core/data_type/data_type_struct.h"
#include "exec/common/endian.h"
#include "exprs/vexpr.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "format/transformer/vfile_format_transformer.h"
#include "io/file_factory.h"
#include "runtime/runtime_state.h"
#include "util/slice.h"
#include "util/string_util.h"
#include "util/uid_util.h"

namespace doris {

namespace {

class RewriteBitmapVisitor final : public IcebergPositionDeleteVisitor {
public:
    RewriteBitmapVisitor(const std::string& referenced_data_file_path,
                         roaring::Roaring64Map* rows_to_delete)
            : _referenced_data_file_path(referenced_data_file_path),
              _rows_to_delete(rows_to_delete) {}

    Status visit(const std::string& file_path, int64_t pos) override {
        if (_rows_to_delete == nullptr) {
            return Status::InvalidArgument("rows_to_delete is null");
        }
        if (file_path == _referenced_data_file_path) {
            _rows_to_delete->add(static_cast<uint64_t>(pos));
        }
        return Status::OK();
    }

private:
    const std::string& _referenced_data_file_path;
    roaring::Roaring64Map* _rows_to_delete;
};

Status load_rewritable_delete_rows(RuntimeState* state, RuntimeProfile* profile,
                                   const std::string& referenced_data_file_path,
                                   const std::vector<TIcebergDeleteFileDesc>& delete_files,
                                   const std::map<std::string, std::string>& hadoop_conf,
                                   TFileType::type file_type,
                                   const std::vector<TNetworkAddress>& broker_addresses,
                                   roaring::Roaring64Map* rows_to_delete) {
    if (rows_to_delete == nullptr) {
        return Status::InvalidArgument("rows_to_delete is null");
    }
    if (state == nullptr || profile == nullptr || delete_files.empty()) {
        return Status::OK();
    }

    TFileScanRangeParams params =
            build_iceberg_delete_scan_range_params(hadoop_conf, file_type, broker_addresses);
    IcebergDeleteFileIOContext delete_file_io_ctx(state);
    IcebergDeleteFileReaderOptions options;
    options.state = state;
    options.profile = profile;
    options.scan_params = &params;
    options.io_ctx = &delete_file_io_ctx.io_ctx;
    options.batch_size = 102400;

    for (const auto& delete_file : delete_files) {
        if (is_iceberg_deletion_vector(delete_file)) {
            RETURN_IF_ERROR(read_iceberg_deletion_vector(delete_file, options, rows_to_delete));
            continue;
        }
        RewriteBitmapVisitor visitor(referenced_data_file_path, rows_to_delete);
        RETURN_IF_ERROR(read_iceberg_position_delete_file(delete_file, options, &visitor));
    }
    return Status::OK();
}

} // namespace

VIcebergDeleteSink::VIcebergDeleteSink(const TDataSink& t_sink,
                                       const VExprContextSPtrs& output_exprs,
                                       std::shared_ptr<Dependency> dep,
                                       std::shared_ptr<Dependency> fin_dep)
        : AsyncResultWriter(output_exprs, dep, fin_dep), _t_sink(t_sink) {
    DCHECK(_t_sink.__isset.iceberg_delete_sink);
}

Status VIcebergDeleteSink::init_properties(ObjectPool* pool) {
    const auto& delete_sink = _t_sink.iceberg_delete_sink;

    _delete_type = delete_sink.delete_type;
    if (_delete_type != TFileContent::POSITION_DELETES) {
        return Status::NotSupported("Iceberg delete only supports position delete files");
    }

    // Get file format settings
    if (delete_sink.__isset.file_format) {
        _file_format_type = delete_sink.file_format;
    }

    if (delete_sink.__isset.compress_type) {
        _compress_type = delete_sink.compress_type;
    }

    // Get output path and table location
    if (delete_sink.__isset.output_path) {
        _output_path = delete_sink.output_path;
    }

    if (delete_sink.__isset.table_location) {
        _table_location = delete_sink.table_location;
    }

    // Get Hadoop configuration
    if (delete_sink.__isset.hadoop_config) {
        _hadoop_conf.insert(delete_sink.hadoop_config.begin(), delete_sink.hadoop_config.end());
    }

    if (delete_sink.__isset.file_type) {
        _file_type = delete_sink.file_type;
    }

    if (delete_sink.__isset.broker_addresses) {
        _broker_addresses.assign(delete_sink.broker_addresses.begin(),
                                 delete_sink.broker_addresses.end());
    }

    // Get partition information
    if (delete_sink.__isset.partition_spec_id) {
        _partition_spec_id = delete_sink.partition_spec_id;
    }

    if (delete_sink.__isset.partition_data_json) {
        _partition_data_json = delete_sink.partition_data_json;
    }

    if (delete_sink.__isset.format_version) {
        _format_version = delete_sink.format_version;
    }

    // for merge old deletion vector and old position delete to a new deletion vector.
    if (_format_version >= 3 && delete_sink.__isset.rewritable_delete_file_sets) {
        for (const auto& delete_file_set : delete_sink.rewritable_delete_file_sets) {
            if (!delete_file_set.__isset.referenced_data_file_path ||
                !delete_file_set.__isset.delete_files ||
                delete_file_set.referenced_data_file_path.empty() ||
                delete_file_set.delete_files.empty()) {
                continue;
            }
            _rewritable_delete_files.emplace(delete_file_set.referenced_data_file_path,
                                             delete_file_set.delete_files);
        }
    }

    return Status::OK();
}

Status VIcebergDeleteSink::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;

    // Initialize counters
    _written_rows_counter = ADD_COUNTER(profile, "RowsWritten", TUnit::UNIT);
    _send_data_timer = ADD_TIMER(profile, "SendDataTime");
    _write_delete_files_timer = ADD_TIMER(profile, "WriteDeleteFilesTime");
    _delete_file_count_counter = ADD_COUNTER(profile, "DeleteFileCount", TUnit::UNIT);
    _open_timer = ADD_TIMER(profile, "OpenTime");
    _close_timer = ADD_TIMER(profile, "CloseTime");

    SCOPED_TIMER(_open_timer);

    if (_format_version < 3) {
        RETURN_IF_ERROR(_init_position_delete_output_exprs());
    }

    LOG(INFO) << fmt::format(
            "VIcebergDeleteSink opened: delete_type={}, output_path={}, format_version={}",
            to_string(_delete_type), _output_path, _format_version);

    return Status::OK();
}

Status VIcebergDeleteSink::write(RuntimeState* state, Block& block) {
    SCOPED_TIMER(_send_data_timer);

    if (block.rows() == 0) {
        return Status::OK();
    }

    _row_count += block.rows();

    if (_delete_type != TFileContent::POSITION_DELETES) {
        return Status::NotSupported("Iceberg delete only supports position delete files");
    }

    // Extract $row_id column and group by file_path
    RETURN_IF_ERROR(_collect_position_deletes(block, _file_deletions));

    if (_written_rows_counter) {
        COUNTER_UPDATE(_written_rows_counter, block.rows());
    }

    return Status::OK();
}

Status VIcebergDeleteSink::close(Status close_status) {
    SCOPED_TIMER(_close_timer);

    if (!close_status.ok()) {
        LOG(WARNING) << fmt::format("VIcebergDeleteSink close with error: {}",
                                    close_status.to_string());
        return close_status;
    }

    if (_delete_type == TFileContent::POSITION_DELETES && !_file_deletions.empty()) {
        SCOPED_TIMER(_write_delete_files_timer);
        if (_format_version >= 3) {
            RETURN_IF_ERROR(_write_deletion_vector_files(_file_deletions));
        } else {
            RETURN_IF_ERROR(_write_position_delete_files(_file_deletions));
        }
    }

    // Update counters
    if (_delete_file_count_counter) {
        COUNTER_UPDATE(_delete_file_count_counter, _delete_file_count);
    }

    LOG(INFO) << fmt::format("VIcebergDeleteSink closed: rows={}, delete_files={}", _row_count,
                             _delete_file_count);

    if (_state != nullptr) {
        for (const auto& commit_data : _commit_data_list) {
            _state->add_iceberg_commit_datas(commit_data);
        }
    }

    return Status::OK();
}

int VIcebergDeleteSink::_get_row_id_column_index(const Block& block) {
    // Find __DORIS_ICEBERG_ROWID_COL__ column in block
    for (size_t i = 0; i < block.columns(); ++i) {
        const auto& col_name = block.get_by_position(i).name;
        if (col_name == doris::BeConsts::ICEBERG_ROWID_COL) {
            return static_cast<int>(i);
        }
    }
    return -1;
}

Status VIcebergDeleteSink::_collect_position_deletes(
        const Block& block, std::map<std::string, IcebergFileDeletion>& file_deletions) {
    // Find row id column
    int row_id_col_idx = _get_row_id_column_index(block);
    if (row_id_col_idx < 0) {
        return Status::InternalError(
                "__DORIS_ICEBERG_ROWID_COL__ column not found in block for position delete");
    }

    const auto& row_id_col = block.get_by_position(row_id_col_idx);
    const IColumn* row_id_data = row_id_col.column.get();
    const IDataType* row_id_type = row_id_col.type.get();
    const auto* nullable_col = check_and_get_column<ColumnNullable>(row_id_data);
    if (nullable_col != nullptr) {
        row_id_data = nullable_col->get_nested_column_ptr().get();
    }
    const auto* nullable_type = check_and_get_data_type<DataTypeNullable>(row_id_type);
    if (nullable_type != nullptr) {
        row_id_type = nullable_type->get_nested_type().get();
    }
    const auto* struct_col = check_and_get_column<ColumnStruct>(row_id_data);
    const auto* struct_type = check_and_get_data_type<DataTypeStruct>(row_id_type);
    if (!struct_col || !struct_type) {
        return Status::InternalError("__DORIS_ICEBERG_ROWID_COL__ column is not a struct column");
    }

    // __DORIS_ICEBERG_ROWID_COL__ struct:
    // (file_path: STRING, row_position: BIGINT, partition_spec_id: INT, partition_data: STRING)
    size_t field_count = struct_col->tuple_size();
    if (field_count < 2) {
        return Status::InternalError(
                "__DORIS_ICEBERG_ROWID_COL__ struct must have at least 2 fields "
                "(file_path, row_position)");
    }

    auto normalize = [](const std::string& name) { return doris::to_lower(name); };

    int file_path_idx = -1;
    int row_position_idx = -1;
    int spec_id_idx = -1;
    int partition_data_idx = -1;
    const auto& field_names = struct_type->get_element_names();
    for (size_t i = 0; i < field_names.size(); ++i) {
        std::string name = normalize(field_names[i]);
        if (file_path_idx < 0 && name == "file_path") {
            file_path_idx = static_cast<int>(i);
        } else if (row_position_idx < 0 && name == "row_position") {
            row_position_idx = static_cast<int>(i);
        } else if (spec_id_idx < 0 && name == "partition_spec_id") {
            spec_id_idx = static_cast<int>(i);
        } else if (partition_data_idx < 0 && name == "partition_data") {
            partition_data_idx = static_cast<int>(i);
        }
    }

    if (file_path_idx < 0 || row_position_idx < 0) {
        return Status::InternalError(
                "__DORIS_ICEBERG_ROWID_COL__ must contain standard fields file_path and "
                "row_position");
    }
    if (field_count >= 3 && spec_id_idx < 0) {
        return Status::InternalError(
                "__DORIS_ICEBERG_ROWID_COL__ must use standard field name partition_spec_id");
    }
    if (field_count >= 4 && partition_data_idx < 0) {
        return Status::InternalError(
                "__DORIS_ICEBERG_ROWID_COL__ must use standard field name partition_data");
    }

    const auto* file_path_col = check_and_get_column<ColumnString>(
            remove_nullable(struct_col->get_column_ptr(file_path_idx)).get());
    const auto* row_position_col = check_and_get_column<ColumnVector<TYPE_BIGINT>>(
            remove_nullable(struct_col->get_column_ptr(row_position_idx)).get());

    if (!file_path_col || !row_position_col) {
        return Status::InternalError(
                "__DORIS_ICEBERG_ROWID_COL__ struct fields have incorrect types");
    }

    const ColumnVector<TYPE_INT>* spec_id_col = nullptr;
    const ColumnString* partition_data_col = nullptr;
    if (spec_id_idx >= 0 && spec_id_idx < static_cast<int>(field_count)) {
        spec_id_col = check_and_get_column<ColumnVector<TYPE_INT>>(
                remove_nullable(struct_col->get_column_ptr(spec_id_idx)).get());
        if (!spec_id_col) {
            return Status::InternalError(
                    "__DORIS_ICEBERG_ROWID_COL__ partition_spec_id has incorrect type");
        }
    }
    if (partition_data_idx >= 0 && partition_data_idx < static_cast<int>(field_count)) {
        partition_data_col = check_and_get_column<ColumnString>(
                remove_nullable(struct_col->get_column_ptr(partition_data_idx)).get());
        if (!partition_data_col) {
            return Status::InternalError(
                    "__DORIS_ICEBERG_ROWID_COL__ partition_data has incorrect type");
        }
    }

    // Group by file_path using roaring bitmap
    for (size_t i = 0; i < block.rows(); ++i) {
        std::string file_path = file_path_col->get_data_at(i).to_string();
        int64_t row_position = row_position_col->get_element(i);
        if (row_position < 0) {
            return Status::InternalError("Invalid row_position {} in row_id column", row_position);
        }

        int32_t partition_spec_id = _partition_spec_id;
        std::string partition_data_json = _partition_data_json;
        if (spec_id_col != nullptr) {
            partition_spec_id = spec_id_col->get_element(i);
        }
        if (partition_data_col != nullptr) {
            partition_data_json = partition_data_col->get_data_at(i).to_string();
        }

        auto [iter, inserted] = file_deletions.emplace(
                file_path, IcebergFileDeletion(partition_spec_id, partition_data_json));
        if (!inserted) {
            if (iter->second.partition_spec_id != partition_spec_id ||
                iter->second.partition_data_json != partition_data_json) {
                LOG(WARNING) << fmt::format(
                        "Mismatched partition info for file {}, existing spec_id={}, data={}, "
                        "new spec_id={}, data={}",
                        file_path, iter->second.partition_spec_id, iter->second.partition_data_json,
                        partition_spec_id, partition_data_json);
            }
        }
        iter->second.rows_to_delete.add(static_cast<uint64_t>(row_position));
    }

    return Status::OK();
}

Status VIcebergDeleteSink::_write_position_delete_files(
        const std::map<std::string, IcebergFileDeletion>& file_deletions) {
    constexpr size_t kBatchSize = 4096;
    for (const auto& [data_file_path, deletion] : file_deletions) {
        if (deletion.rows_to_delete.isEmpty()) {
            continue;
        }
        // Generate unique delete file path
        std::string delete_file_path = _generate_delete_file_path(data_file_path);

        // Create delete file writer
        auto writer = VIcebergDeleteFileWriterFactory::create_writer(
                TFileContent::POSITION_DELETES, delete_file_path, _file_format_type,
                _compress_type);

        // Build column names for position delete
        std::vector<std::string> column_names = {"file_path", "pos"};

        if (_position_delete_output_expr_ctxs.empty()) {
            RETURN_IF_ERROR(_init_position_delete_output_exprs());
        }

        // Open writer
        RETURN_IF_ERROR(writer->open(_state, _state->runtime_profile(),
                                     _position_delete_output_expr_ctxs, column_names, _hadoop_conf,
                                     _file_type, _broker_addresses));

        // Build block with (file_path, pos) columns
        std::vector<int64_t> positions;
        positions.reserve(kBatchSize);
        for (auto it = deletion.rows_to_delete.begin(); it != deletion.rows_to_delete.end(); ++it) {
            positions.push_back(static_cast<int64_t>(*it));
            if (positions.size() >= kBatchSize) {
                Block delete_block;
                RETURN_IF_ERROR(
                        _build_position_delete_block(data_file_path, positions, delete_block));
                RETURN_IF_ERROR(writer->write(delete_block));
                positions.clear();
            }
        }
        if (!positions.empty()) {
            Block delete_block;
            RETURN_IF_ERROR(_build_position_delete_block(data_file_path, positions, delete_block));
            RETURN_IF_ERROR(writer->write(delete_block));
        }

        // Set partition info on writer before close
        writer->set_partition_info(deletion.partition_spec_id, deletion.partition_data_json);

        // Close writer and collect commit data
        TIcebergCommitData commit_data;
        RETURN_IF_ERROR(writer->close(commit_data));

        // Set referenced data file path
        commit_data.__set_referenced_data_file_path(data_file_path);

        _commit_data_list.push_back(commit_data);
        _delete_file_count++;

        VLOG(1) << fmt::format("Written position delete file: path={}, rows={}, referenced_file={}",
                               delete_file_path, commit_data.row_count, data_file_path);
    }

    return Status::OK();
}

Status VIcebergDeleteSink::_init_position_delete_output_exprs() {
    if (!_position_delete_output_expr_ctxs.empty()) {
        return Status::OK();
    }

    std::vector<TExpr> texprs;
    texprs.reserve(2);

    std::string empty_string;
    TExprNode file_path_node =
            create_texpr_node_from(&empty_string, PrimitiveType::TYPE_STRING, 0, 0);
    file_path_node.__set_num_children(0);
    file_path_node.__set_output_scale(0);
    file_path_node.__set_is_nullable(false);
    TExpr file_path_expr;
    file_path_expr.nodes.emplace_back(std::move(file_path_node));
    texprs.emplace_back(std::move(file_path_expr));

    int64_t zero = 0;
    TExprNode pos_node = create_texpr_node_from(&zero, PrimitiveType::TYPE_BIGINT, 0, 0);
    pos_node.__set_num_children(0);
    pos_node.__set_output_scale(0);
    pos_node.__set_is_nullable(false);
    TExpr pos_expr;
    pos_expr.nodes.emplace_back(std::move(pos_node));
    texprs.emplace_back(std::move(pos_expr));

    RETURN_IF_ERROR(VExpr::create_expr_trees(texprs, _position_delete_output_expr_ctxs));
    return Status::OK();
}

Status VIcebergDeleteSink::_build_position_delete_block(const std::string& file_path,
                                                        const std::vector<int64_t>& positions,
                                                        Block& output_block) {
    // Create file_path column (repeated for each position)
    auto file_path_col = ColumnString::create();
    for (size_t i = 0; i < positions.size(); ++i) {
        file_path_col->insert_data(file_path.data(), file_path.size());
    }

    // Create pos column
    auto pos_col = ColumnVector<TYPE_BIGINT>::create();
    pos_col->get_data().assign(positions.begin(), positions.end());

    // Build block
    output_block.insert(ColumnWithTypeAndName(std::move(file_path_col),
                                              std::make_shared<DataTypeString>(), "file_path"));
    output_block.insert(
            ColumnWithTypeAndName(std::move(pos_col), std::make_shared<DataTypeInt64>(), "pos"));

    return Status::OK();
}

std::string VIcebergDeleteSink::_get_file_extension() const {
    std::string compress_name;
    switch (_compress_type) {
    case TFileCompressType::SNAPPYBLOCK: {
        compress_name = ".snappy";
        break;
    }
    case TFileCompressType::ZLIB: {
        compress_name = ".zlib";
        break;
    }
    case TFileCompressType::ZSTD: {
        compress_name = ".zstd";
        break;
    }
    default: {
        compress_name = "";
        break;
    }
    }

    std::string file_format_name;
    switch (_file_format_type) {
    case TFileFormatType::FORMAT_PARQUET: {
        file_format_name = ".parquet";
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        file_format_name = ".orc";
        break;
    }
    default: {
        file_format_name = "";
        break;
    }
    }
    return fmt::format("{}{}", compress_name, file_format_name);
}

Status VIcebergDeleteSink::_write_deletion_vector_files(
        const std::map<std::string, IcebergFileDeletion>& file_deletions) {
    std::vector<DeletionVectorBlob> blobs;
    for (const auto& [data_file_path, deletion] : file_deletions) {
        if (deletion.rows_to_delete.isEmpty()) {
            continue;
        }
        roaring::Roaring64Map merged_rows = deletion.rows_to_delete;
        DeletionVectorBlob blob;
        blob.delete_count = static_cast<int64_t>(merged_rows.cardinality());
        auto previous_delete_it = _rewritable_delete_files.find(data_file_path);
        if (previous_delete_it != _rewritable_delete_files.end()) {
            roaring::Roaring64Map previous_rows;
            RETURN_IF_ERROR(load_rewritable_delete_rows(
                    _state, _state->runtime_profile(), data_file_path, previous_delete_it->second,
                    _hadoop_conf, _file_type, _broker_addresses, &previous_rows));
            merged_rows |= previous_rows;
        }

        size_t bitmap_size = merged_rows.getSizeInBytes();
        blob.referenced_data_file = data_file_path;
        blob.partition_spec_id = deletion.partition_spec_id;
        blob.partition_data_json = deletion.partition_data_json;
        blob.merged_count = static_cast<int64_t>(merged_rows.cardinality());
        blob.content_size_in_bytes = static_cast<int64_t>(4 + 4 + bitmap_size + 4);
        blob.blob_data.resize(static_cast<size_t>(blob.content_size_in_bytes));
        merged_rows.write(blob.blob_data.data() + 8);

        uint32_t total_length = static_cast<uint32_t>(4 + bitmap_size);
        BigEndian::Store32(blob.blob_data.data(), total_length);

        constexpr char DV_MAGIC[] = {'\xD1', '\xD3', '\x39', '\x64'};
        memcpy(blob.blob_data.data() + 4, DV_MAGIC, 4);

        uint32_t crc = static_cast<uint32_t>(
                ::crc32(0, reinterpret_cast<const Bytef*>(blob.blob_data.data() + 4),
                        4 + (uInt)bitmap_size));
        BigEndian::Store32(blob.blob_data.data() + 8 + bitmap_size, crc);
        blobs.emplace_back(std::move(blob));
    }

    if (blobs.empty()) {
        return Status::OK();
    }

    std::string puffin_path = _generate_puffin_file_path();
    int64_t puffin_file_size = 0;
    RETURN_IF_ERROR(_write_puffin_file(puffin_path, &blobs, &puffin_file_size));

    for (const auto& blob : blobs) {
        TIcebergCommitData commit_data;
        commit_data.__set_file_path(puffin_path);
        commit_data.__set_row_count(blob.merged_count);
        commit_data.__set_affected_rows(blob.delete_count);
        commit_data.__set_file_size(puffin_file_size);
        commit_data.__set_file_content(TFileContent::DELETION_VECTOR);
        commit_data.__set_content_offset(blob.content_offset);
        commit_data.__set_content_size_in_bytes(blob.content_size_in_bytes);
        commit_data.__set_referenced_data_file_path(blob.referenced_data_file);
        if (blob.partition_spec_id != 0 || !blob.partition_data_json.empty()) {
            commit_data.__set_partition_spec_id(blob.partition_spec_id);
            commit_data.__set_partition_data_json(blob.partition_data_json);
        }

        _commit_data_list.push_back(commit_data);
        _delete_file_count++;
    }
    return Status::OK();
}

Status VIcebergDeleteSink::_write_puffin_file(const std::string& puffin_path,
                                              std::vector<DeletionVectorBlob>* blobs,
                                              int64_t* out_file_size) {
    DCHECK(blobs != nullptr);
    DCHECK(!blobs->empty());

    io::FSPropertiesRef fs_properties(_file_type);
    fs_properties.properties = &_hadoop_conf;
    if (!_broker_addresses.empty()) {
        fs_properties.broker_addresses = &_broker_addresses;
    }
    io::FileDescription file_description = {.path = puffin_path, .fs_name {}};
    auto fs = DORIS_TRY(FileFactory::create_fs(fs_properties, file_description));
    io::FileWriterOptions file_writer_options = {.used_by_s3_committer = false};
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(fs->create_file(file_description.path, &file_writer, &file_writer_options));

    constexpr char PUFFIN_MAGIC[] = {'\x50', '\x46', '\x41', '\x31'};
    RETURN_IF_ERROR(file_writer->append(Slice(reinterpret_cast<const uint8_t*>(PUFFIN_MAGIC), 4)));
    int64_t current_offset = 4;
    for (auto& blob : *blobs) {
        blob.content_offset = current_offset;
        RETURN_IF_ERROR(file_writer->append(Slice(
                reinterpret_cast<const uint8_t*>(blob.blob_data.data()), blob.blob_data.size())));
        current_offset += static_cast<int64_t>(blob.blob_data.size());
    }
    RETURN_IF_ERROR(file_writer->append(Slice(reinterpret_cast<const uint8_t*>(PUFFIN_MAGIC), 4)));

    std::string footer_json = _build_puffin_footer_json(*blobs);
    RETURN_IF_ERROR(file_writer->append(
            Slice(reinterpret_cast<const uint8_t*>(footer_json.data()), footer_json.size())));

    char footer_size_buf[4];
    LittleEndian::Store32(footer_size_buf, static_cast<uint32_t>(footer_json.size()));
    RETURN_IF_ERROR(file_writer->append(
            Slice(reinterpret_cast<const uint8_t*>(footer_size_buf), sizeof(footer_size_buf))));

    char flags[4] = {0, 0, 0, 0};
    RETURN_IF_ERROR(
            file_writer->append(Slice(reinterpret_cast<const uint8_t*>(flags), sizeof(flags))));
    RETURN_IF_ERROR(file_writer->append(Slice(reinterpret_cast<const uint8_t*>(PUFFIN_MAGIC), 4)));
    RETURN_IF_ERROR(file_writer->close());

    *out_file_size = current_offset + 4 + static_cast<int64_t>(footer_json.size()) + 4 + 4 + 4;
    return Status::OK();
}

std::string VIcebergDeleteSink::_build_puffin_footer_json(
        const std::vector<DeletionVectorBlob>& blobs) {
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    writer.StartObject();
    writer.Key("blobs");
    writer.StartArray();
    for (const auto& blob : blobs) {
        writer.StartObject();
        writer.Key("type");
        writer.String("deletion-vector-v1");
        writer.Key("fields");
        writer.StartArray();
        writer.EndArray();
        writer.Key("snapshot-id");
        writer.Int64(-1);
        writer.Key("sequence-number");
        writer.Int64(-1);
        writer.Key("offset");
        writer.Int64(blob.content_offset);
        writer.Key("length");
        writer.Int64(blob.content_size_in_bytes);
        writer.Key("properties");
        writer.StartObject();
        writer.Key("referenced-data-file");
        writer.String(blob.referenced_data_file.c_str(),
                      static_cast<rapidjson::SizeType>(blob.referenced_data_file.size()));
        std::string cardinality = std::to_string(blob.merged_count);
        writer.Key("cardinality");
        writer.String(cardinality.c_str(), static_cast<rapidjson::SizeType>(cardinality.size()));
        writer.EndObject();
        writer.EndObject();
    }
    writer.EndArray();
    writer.Key("properties");
    writer.StartObject();
    writer.Key("created-by");
    writer.String("doris-puffin-v1");
    writer.EndObject();
    writer.EndObject();
    return {buffer.GetString(), buffer.GetSize()};
}

std::string VIcebergDeleteSink::_generate_delete_file_path(
        const std::string& referenced_data_file) {
    // Generate unique delete file name using UUID
    std::string uuid = generate_uuid_string();
    std::string file_name;

    std::string file_extension = _get_file_extension();
    file_name =
            fmt::format("delete_pos_{}_{}{}", uuid,
                        std::hash<std::string> {}(referenced_data_file) % 10000000, file_extension);

    // Combine with output path or table location
    std::string base_path = _output_path.empty() ? _table_location : _output_path;

    // Ensure base path ends with /
    if (!base_path.empty() && base_path.back() != '/') {
        base_path += '/';
    }

    // Delete files are data files in Iceberg, write under data location
    return fmt::format("{}{}", base_path, file_name);
}

std::string VIcebergDeleteSink::_generate_puffin_file_path() {
    std::string uuid = generate_uuid_string();
    std::string file_name = fmt::format("delete_dv_{}.puffin", uuid);
    std::string base_path = _output_path.empty() ? _table_location : _output_path;
    if (!base_path.empty() && base_path.back() != '/') {
        base_path += '/';
    }
    return fmt::format("{}{}", base_path, file_name);
}

} // namespace doris
