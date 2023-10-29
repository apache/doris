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

#include "vfile_result_writer.h"

#include <gen_cpp/Data_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <ostream>
#include <utility>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/consts.h"
#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/broker_file_system.h"
#include "io/fs/hdfs_file_system.h"
#include "io/fs/local_file_system.h"
#include "io/fs/s3_file_system.h"
#include "io/hdfs_builder.h"
#include "runtime/buffer_control_block.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/large_int_value.h"
#include "runtime/primitive_type.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/mysql_row_buffer.h"
#include "util/s3_uri.h"
#include "util/s3_util.h"
#include "util/uid_util.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vcsv_transformer.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"
#include "vec/sink/vresult_sink.h"

namespace doris::vectorized {

VFileResultWriter::VFileResultWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs)
        : AsyncResultWriter(output_exprs) {}

VFileResultWriter::VFileResultWriter(const ResultFileOptions* file_opts,
                                     const TStorageBackendType::type storage_type,
                                     const TUniqueId fragment_instance_id,
                                     const VExprContextSPtrs& output_vexpr_ctxs,
                                     BufferControlBlock* sinker, Block* output_block,
                                     bool output_object_data,
                                     const RowDescriptor& output_row_descriptor)
        : AsyncResultWriter(output_vexpr_ctxs),
          _file_opts(file_opts),
          _storage_type(storage_type),
          _fragment_instance_id(fragment_instance_id),
          _sinker(sinker),
          _output_block(output_block),
          _output_row_descriptor(output_row_descriptor) {
    _output_object_data = output_object_data;
}

Status VFileResultWriter::open(RuntimeState* state, RuntimeProfile* profile) {
    _state = state;
    _init_profile(profile);
    // Delete existing files
    if (_file_opts->delete_existing_files) {
        RETURN_IF_ERROR(_delete_dir());
    }
    return _create_next_file_writer();
}

void VFileResultWriter::_init_profile(RuntimeProfile* parent_profile) {
    RuntimeProfile* profile = parent_profile->create_child("VFileResultWriter", true, true);
    _append_row_batch_timer = ADD_TIMER(profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(profile, "TupleConvertTime", "AppendBatchTime");
    _file_write_timer = ADD_CHILD_TIMER(profile, "FileWriteTime", "AppendBatchTime");
    _writer_close_timer = ADD_TIMER(profile, "FileWriterCloseTime");
    _written_rows_counter = ADD_COUNTER(profile, "NumWrittenRows", TUnit::UNIT);
    _written_data_bytes = ADD_COUNTER(profile, "WrittenDataBytes", TUnit::BYTES);
}

Status VFileResultWriter::_create_success_file() {
    std::string file_name;
    RETURN_IF_ERROR(_get_success_file_name(&file_name));
    RETURN_IF_ERROR(FileFactory::create_file_writer(
            FileFactory::convert_storage_type(_storage_type), _state->exec_env(),
            _file_opts->broker_addresses, _file_opts->broker_properties, file_name, 0,
            _file_writer_impl));
    // must write somthing because s3 file writer can not writer empty file
    RETURN_IF_ERROR(_file_writer_impl->append({"success"}));
    return _file_writer_impl->close();
}

Status VFileResultWriter::_get_success_file_name(std::string* file_name) {
    std::stringstream ss;
    ss << _file_opts->file_path << _file_opts->success_file_name;
    *file_name = ss.str();
    if (_storage_type == TStorageBackendType::LOCAL) {
        // For local file writer, the file_path is a local dir.
        // Here we do a simple security verification by checking whether the file exists.
        // Because the file path is currently arbitrarily specified by the user,
        // Doris is not responsible for ensuring the correctness of the path.
        // This is just to prevent overwriting the existing file.
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(*file_name, &exists));
        if (exists) {
            return Status::InternalError("File already exists: {}", *file_name);
        }
    }

    return Status::OK();
}

Status VFileResultWriter::_create_next_file_writer() {
    std::string file_name;
    RETURN_IF_ERROR(_get_next_file_name(&file_name));
    return _create_file_writer(file_name);
}

Status VFileResultWriter::_create_file_writer(const std::string& file_name) {
    RETURN_IF_ERROR(FileFactory::create_file_writer(
            FileFactory::convert_storage_type(_storage_type), _state->exec_env(),
            _file_opts->broker_addresses, _file_opts->broker_properties, file_name, 0,
            _file_writer_impl));
    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        _vfile_writer.reset(new VCSVTransformer(
                _state, _file_writer_impl.get(), _vec_output_expr_ctxs, _output_object_data,
                _header_type, _header, _file_opts->column_separator, _file_opts->line_delimiter));
        break;
    case TFileFormatType::FORMAT_PARQUET:
        _vfile_writer.reset(new VParquetTransformer(
                _state, _file_writer_impl.get(), _vec_output_expr_ctxs, _file_opts->parquet_schemas,
                _file_opts->parquet_commpression_type, _file_opts->parquert_disable_dictionary,
                _file_opts->parquet_version, _output_object_data));
        break;
    case TFileFormatType::FORMAT_ORC:
        _vfile_writer.reset(new VOrcTransformer(_state, _file_writer_impl.get(),
                                                _vec_output_expr_ctxs, _file_opts->orc_schema,
                                                _output_object_data));
        break;
    default:
        return Status::InternalError("unsupported file format: {}", _file_opts->file_format);
    }
    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id())
              << " format:" << _file_opts->file_format;

    return _vfile_writer->open();
}

// file name format as: my_prefix_{fragment_instance_id}_0.csv
Status VFileResultWriter::_get_next_file_name(std::string* file_name) {
    std::string suffix =
            _file_opts->file_suffix.empty() ? _file_format_to_name() : _file_opts->file_suffix;
    std::stringstream ss;
    ss << _file_opts->file_path << print_id(_fragment_instance_id) << "_" << (_file_idx++) << "."
       << suffix;
    *file_name = ss.str();
    if (_storage_type == TStorageBackendType::LOCAL) {
        // For local file writer, the file_path is a local dir.
        // Here we do a simple security verification by checking whether the file exists.
        // Because the file path is currently arbitrarily specified by the user,
        // Doris is not responsible for ensuring the correctness of the path.
        // This is just to prevent overwriting the existing file.
        bool exists = true;
        RETURN_IF_ERROR(io::global_local_filesystem()->exists(*file_name, &exists));
        if (exists) {
            return Status::InternalError("File already exists: {}", *file_name);
        }
    }

    return Status::OK();
}

// file url format as:
// LOCAL: file:///localhost_address/{file_path}{fragment_instance_id}_
// S3: {file_path}{fragment_instance_id}_
// BROKER: {file_path}{fragment_instance_id}_

Status VFileResultWriter::_get_file_url(std::string* file_url) {
    std::stringstream ss;
    if (_storage_type == TStorageBackendType::LOCAL) {
        ss << "file:///" << BackendOptions::get_localhost();
    }
    ss << _file_opts->file_path;
    ss << print_id(_fragment_instance_id) << "_";
    *file_url = ss.str();
    return Status::OK();
}

std::string VFileResultWriter::_file_format_to_name() {
    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        return "csv";
    case TFileFormatType::FORMAT_PARQUET:
        return "parquet";
    case TFileFormatType::FORMAT_ORC:
        return "orc";
    default:
        return "unknown";
    }
}

Status VFileResultWriter::append_block(Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }
    SCOPED_TIMER(_append_row_batch_timer);
    Block output_block;
    RETURN_IF_ERROR(_projection_block(block, &output_block));
    RETURN_IF_ERROR(_write_file(output_block));

    _written_rows += block.rows();
    return Status::OK();
}

Status VFileResultWriter::_write_file(const Block& block) {
    {
        SCOPED_TIMER(_file_write_timer);
        RETURN_IF_ERROR(_vfile_writer->write(block));
    }
    // split file if exceed limit
    _current_written_bytes = _vfile_writer->written_len();
    return _create_new_file_if_exceed_size();
}

Status VFileResultWriter::_create_new_file_if_exceed_size() {
    if (_current_written_bytes < _file_opts->max_file_size_bytes) {
        return Status::OK();
    }
    // current file size exceed the max file size. close this file
    // and create new one
    {
        SCOPED_TIMER(_writer_close_timer);
        RETURN_IF_ERROR(_close_file_writer(false));
    }
    _current_written_bytes = 0;
    return Status::OK();
}

Status VFileResultWriter::_close_file_writer(bool done) {
    if (_vfile_writer) {
        // we can not use _current_written_bytes to COUNTER_UPDATE(_written_data_bytes, _current_written_bytes)
        // because it will call `write()` function of orc/parquet function in `_vfile_writer->close()`
        // and the real written_len will increase
        // and _current_written_bytes will less than _vfile_writer->written_len()
        COUNTER_UPDATE(_written_data_bytes, _vfile_writer->written_len());
        RETURN_IF_ERROR(_vfile_writer->close());
        _vfile_writer.reset(nullptr);
    } else if (_file_writer_impl) {
        RETURN_IF_ERROR(_file_writer_impl->close());
    }

    if (!done) {
        // not finished, create new file writer for next file
        RETURN_IF_ERROR(_create_next_file_writer());
    } else {
        // All data is written to file, send statistic result
        if (_file_opts->success_file_name != "") {
            // write success file, just need to touch an empty file
            RETURN_IF_ERROR(_create_success_file());
        }
        if (_output_block == nullptr) {
            RETURN_IF_ERROR(_send_result());
        } else {
            RETURN_IF_ERROR(_fill_result_block());
        }
    }
    return Status::OK();
}

Status VFileResultWriter::_send_result() {
    if (_is_result_sent) {
        return Status::OK();
    }
    _is_result_sent = true;

    // The final stat result include:
    // FileNumber, TotalRows, FileSize and URL
    // The type of these field should be consistent with types defined
    // in OutFileClause.java of FE.
    MysqlRowBuffer<> row_buffer;
    row_buffer.push_int(_file_idx);                         // file number
    row_buffer.push_bigint(_written_rows_counter->value()); // total rows
    row_buffer.push_bigint(_written_data_bytes->value());   // file size
    std::string file_url;
    static_cast<void>(_get_file_url(&file_url));
    row_buffer.push_string(file_url.c_str(), file_url.length()); // url

    std::unique_ptr<TFetchDataResult> result = std::make_unique<TFetchDataResult>();
    result->result_batch.rows.resize(1);
    result->result_batch.rows[0].assign(row_buffer.buf(), row_buffer.length());

    std::map<std::string, std::string> attach_infos;
    attach_infos.insert(std::make_pair("FileNumber", std::to_string(_file_idx)));
    attach_infos.insert(
            std::make_pair("TotalRows", std::to_string(_written_rows_counter->value())));
    attach_infos.insert(std::make_pair("FileSize", std::to_string(_written_data_bytes->value())));
    attach_infos.insert(std::make_pair("URL", file_url));

    result->result_batch.__set_attached_infos(attach_infos);
    RETURN_NOT_OK_STATUS_WITH_WARN(_sinker->add_batch(result), "failed to send outfile result");
    return Status::OK();
}

Status VFileResultWriter::_fill_result_block() {
    if (_is_result_sent) {
        return Status::OK();
    }
    _is_result_sent = true;

#ifndef INSERT_TO_COLUMN
#define INSERT_TO_COLUMN                                                            \
    if (i == 0) {                                                                   \
        column->insert_data(reinterpret_cast<const char*>(&_file_idx), 0);          \
    } else if (i == 1) {                                                            \
        int64_t written_rows = _written_rows_counter->value();                      \
        column->insert_data(reinterpret_cast<const char*>(&written_rows), 0);       \
    } else if (i == 2) {                                                            \
        int64_t written_data_bytes = _written_data_bytes->value();                  \
        column->insert_data(reinterpret_cast<const char*>(&written_data_bytes), 0); \
    } else if (i == 3) {                                                            \
        std::string file_url;                                                       \
        static_cast<void>(_get_file_url(&file_url));                                \
        column->insert_data(file_url.c_str(), file_url.size());                     \
    }                                                                               \
    _output_block->replace_by_position(i, std::move(column));
#endif

    for (int i = 0; i < _output_block->columns(); i++) {
        switch (_output_row_descriptor.tuple_descriptors()[0]->slots()[i]->type().type) {
        case TYPE_INT: {
            auto column = ColumnVector<int32_t>::create();
            INSERT_TO_COLUMN;
            break;
        }
        case TYPE_BIGINT: {
            auto column = ColumnVector<int64_t>::create();
            INSERT_TO_COLUMN;
            break;
        }
        case TYPE_VARCHAR:
        case TYPE_CHAR:
        case TYPE_STRING: {
            auto column = ColumnString::create();
            INSERT_TO_COLUMN;
            break;
        }
        default:
            return Status::InternalError(
                    "Invalid type to print: {}",
                    _output_row_descriptor.tuple_descriptors()[0]->slots()[i]->type().type);
        }
    }
    return Status::OK();
}

Status VFileResultWriter::_delete_dir() {
    // get dir of file_path
    std::string dir = _file_opts->file_path.substr(0, _file_opts->file_path.find_last_of('/') + 1);
    std::shared_ptr<io::FileSystem> file_system = nullptr;
    switch (_storage_type) {
    case TStorageBackendType::LOCAL:
        file_system = io::LocalFileSystem::create(dir, "");
        break;
    case TStorageBackendType::BROKER: {
        std::shared_ptr<io::BrokerFileSystem> broker_fs = nullptr;
        RETURN_IF_ERROR(io::BrokerFileSystem::create(_file_opts->broker_addresses[0],
                                                     _file_opts->broker_properties, &broker_fs));
        file_system = broker_fs;
        break;
    }
    case TStorageBackendType::HDFS: {
        THdfsParams hdfs_params = parse_properties(_file_opts->broker_properties);
        std::shared_ptr<io::HdfsFileSystem> hdfs_fs = nullptr;
        RETURN_IF_ERROR(
                io::HdfsFileSystem::create(hdfs_params, hdfs_params.fs_name, nullptr, &hdfs_fs));
        file_system = hdfs_fs;
        break;
    }
    case TStorageBackendType::S3: {
        S3URI s3_uri(dir);
        RETURN_IF_ERROR(s3_uri.parse());
        S3Conf s3_conf;
        std::shared_ptr<io::S3FileSystem> s3_fs = nullptr;
        RETURN_IF_ERROR(S3ClientFactory::convert_properties_to_s3_conf(
                _file_opts->broker_properties, s3_uri, &s3_conf));
        RETURN_IF_ERROR(io::S3FileSystem::create(s3_conf, "", &s3_fs));
        file_system = s3_fs;
        break;
    }
    default:
        return Status::NotSupported("Unsupported storage type: {}", std::to_string(_storage_type));
    }
    RETURN_IF_ERROR(file_system->delete_directory(dir));
    return Status::OK();
}

Status VFileResultWriter::close(Status) {
    // the following 2 profile "_written_rows_counter" and "_writer_close_timer"
    // must be outside the `_close_file_writer()`.
    // because `_close_file_writer()` may be called in deconstructor,
    // at that time, the RuntimeState may already been deconstructed,
    // so does the profile in RuntimeState.
    if (_written_rows_counter) {
        COUNTER_SET(_written_rows_counter, _written_rows);
        SCOPED_TIMER(_writer_close_timer);
    }
    return _close_file_writer(true);
}

} // namespace doris::vectorized
