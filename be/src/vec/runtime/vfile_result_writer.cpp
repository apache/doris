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

#include "vec/runtime/vfile_result_writer.h"

#include "common/consts.h"
#include "common/status.h"
#include "exprs/expr_context.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/substitute.h"
#include "io/file_factory.h"
#include "io/file_writer.h"
#include "runtime/buffer_control_block.h"
#include "runtime/descriptors.h"
#include "runtime/large_int_value.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/mysql_global.h"
#include "util/mysql_row_buffer.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vorc_writer.h"

namespace doris::vectorized {
const size_t VFileResultWriter::OUTSTREAM_BUFFER_SIZE_BYTES = 1024 * 1024;
using doris::operator<<;

VFileResultWriter::VFileResultWriter(
        const ResultFileOptions* file_opts, const TStorageBackendType::type storage_type,
        const TUniqueId fragment_instance_id,
        const std::vector<vectorized::VExprContext*>& output_vexpr_ctxs,
        RuntimeProfile* parent_profile, BufferControlBlock* sinker, Block* output_block,
        bool output_object_data, const RowDescriptor& output_row_descriptor)
        : _file_opts(file_opts),
          _storage_type(storage_type),
          _fragment_instance_id(fragment_instance_id),
          _output_vexpr_ctxs(output_vexpr_ctxs),
          _parent_profile(parent_profile),
          _sinker(sinker),
          _output_block(output_block),
          _output_row_descriptor(output_row_descriptor),
          _vfile_writer(nullptr) {
    _output_object_data = output_object_data;
}

Status VFileResultWriter::init(RuntimeState* state) {
    _state = state;
    _init_profile();
    return _create_next_file_writer();
}

void VFileResultWriter::_init_profile() {
    RuntimeProfile* profile = _parent_profile->create_child("VFileResultWriter", true, true);
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
    RETURN_IF_ERROR(_create_file_writer(file_name));
    // set only close to true to avoid dead loop
    return _close_file_writer(true, true);
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
        if (FileUtils::check_exist(*file_name)) {
            return Status::InternalError("File already exists: " + *file_name +
                                         ". Host: " + BackendOptions::get_localhost());
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
    RETURN_IF_ERROR(_file_writer_impl->open());
    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        // just use file writer is enough
        break;
    case TFileFormatType::FORMAT_PARQUET:
        _vfile_writer.reset(new VParquetWriterWrapper(
                _file_writer_impl.get(), _output_vexpr_ctxs, _file_opts->parquet_schemas,
                _file_opts->parquet_commpression_type, _file_opts->parquert_disable_dictionary,
                _file_opts->parquet_version, _output_object_data));
        RETURN_IF_ERROR(_vfile_writer->prepare());
        break;
    case TFileFormatType::FORMAT_ORC:
        _vfile_writer.reset(new VOrcWriterWrapper(_file_writer_impl.get(), _output_vexpr_ctxs,
                                                  _file_opts->orc_schema, _output_object_data));
        RETURN_IF_ERROR(_vfile_writer->prepare());
        break;
    default:
        return Status::InternalError("unsupported file format: {}", _file_opts->file_format);
    }
    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id())
              << " format:" << _file_opts->file_format;
    return Status::OK();
}

// file name format as: my_prefix_{fragment_instance_id}_0.csv
Status VFileResultWriter::_get_next_file_name(std::string* file_name) {
    std::stringstream ss;
    ss << _file_opts->file_path << print_id(_fragment_instance_id) << "_" << (_file_idx++) << "."
       << _file_format_to_name();
    *file_name = ss.str();
    _header_sent = false;
    if (_storage_type == TStorageBackendType::LOCAL) {
        // For local file writer, the file_path is a local dir.
        // Here we do a simple security verification by checking whether the file exists.
        // Because the file path is currently arbitrarily specified by the user,
        // Doris is not responsible for ensuring the correctness of the path.
        // This is just to prevent overwriting the existing file.
        if (FileUtils::check_exist(*file_name)) {
            return Status::InternalError("File already exists: " + *file_name +
                                         ". Host: " + BackendOptions::get_localhost());
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
    RETURN_IF_ERROR(write_csv_header());
    SCOPED_TIMER(_append_row_batch_timer);
    Status status = Status::OK();
    // Exec vectorized expr here to speed up, block.rows() == 0 means expr exec
    // failed, just return the error status
    auto output_block =
            VExprContext::get_output_block_after_execute_exprs(_output_vexpr_ctxs, block, status);
    auto num_rows = output_block.rows();
    if (UNLIKELY(num_rows == 0)) {
        return status;
    }
    if (_vfile_writer) {
        RETURN_IF_ERROR(_write_file(output_block));
    } else {
        RETURN_IF_ERROR(_write_csv_file(output_block));
    }

    _written_rows += block.rows();
    return Status::OK();
}

Status VFileResultWriter::_write_file(const Block& block) {
    RETURN_IF_ERROR(_vfile_writer->write(block));
    // split file if exceed limit
    _current_written_bytes = _vfile_writer->written_len();
    return _create_new_file_if_exceed_size();
}

Status VFileResultWriter::_write_csv_file(const Block& block) {
    for (size_t i = 0; i < block.rows(); i++) {
        for (size_t col_id = 0; col_id < block.columns(); col_id++) {
            auto col = block.get_by_position(col_id);
            if (col.column->is_null_at(i)) {
                _plain_text_outstream << NULL_IN_CSV;
            } else {
                switch (_output_vexpr_ctxs[col_id]->root()->type().type) {
                case TYPE_BOOLEAN:
                case TYPE_TINYINT:
                    _plain_text_outstream << (int)*reinterpret_cast<const int8_t*>(
                            col.column->get_data_at(i).data);
                    break;
                case TYPE_SMALLINT:
                    _plain_text_outstream
                            << *reinterpret_cast<const int16_t*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_INT:
                    _plain_text_outstream
                            << *reinterpret_cast<const int32_t*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_BIGINT:
                    _plain_text_outstream
                            << *reinterpret_cast<const int64_t*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_LARGEINT:
                    _plain_text_outstream
                            << *reinterpret_cast<const __int128*>(col.column->get_data_at(i).data);
                    break;
                case TYPE_FLOAT: {
                    char buffer[MAX_FLOAT_STR_LENGTH + 2];
                    float float_value =
                            *reinterpret_cast<const float*>(col.column->get_data_at(i).data);
                    buffer[0] = '\0';
                    int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
                    DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
                    _plain_text_outstream << buffer;
                    break;
                }
                case TYPE_DOUBLE: {
                    // To prevent loss of precision on float and double types,
                    // they are converted to strings before output.
                    // For example: For a double value 27361919854.929001,
                    // the direct output of using std::stringstream is 2.73619e+10,
                    // and after conversion to a string, it outputs 27361919854.929001
                    char buffer[MAX_DOUBLE_STR_LENGTH + 2];
                    double double_value =
                            *reinterpret_cast<const double*>(col.column->get_data_at(i).data);
                    buffer[0] = '\0';
                    int length = DoubleToBuffer(double_value, MAX_DOUBLE_STR_LENGTH, buffer);
                    DCHECK(length >= 0) << "gcvt double failed, double value=" << double_value;
                    _plain_text_outstream << buffer;
                    break;
                }
                case TYPE_DATEV2: {
                    char buf[64];
                    const DateV2Value<DateV2ValueType>* time_val =
                            (const DateV2Value<DateV2ValueType>*)(col.column->get_data_at(i).data);
                    time_val->to_string(buf);
                    _plain_text_outstream << buf;
                    break;
                }
                case TYPE_DATETIMEV2: {
                    char buf[64];
                    const DateV2Value<DateTimeV2ValueType>* time_val =
                            (const DateV2Value<DateTimeV2ValueType>*)(col.column->get_data_at(i)
                                                                              .data);
                    time_val->to_string(buf, _output_vexpr_ctxs[col_id]->root()->type().scale);
                    _plain_text_outstream << buf;
                    break;
                }
                case TYPE_DATE:
                case TYPE_DATETIME: {
                    char buf[64];
                    const VecDateTimeValue* time_val =
                            (const VecDateTimeValue*)(col.column->get_data_at(i).data);
                    time_val->to_string(buf);
                    _plain_text_outstream << buf;
                    break;
                }
                case TYPE_OBJECT:
                case TYPE_HLL: {
                    if (!_output_object_data) {
                        _plain_text_outstream << NULL_IN_CSV;
                        break;
                    }
                    [[fallthrough]];
                }
                case TYPE_VARCHAR:
                case TYPE_CHAR:
                case TYPE_STRING: {
                    auto value = col.column->get_data_at(i);
                    _plain_text_outstream << value;
                    break;
                }
                case TYPE_DECIMALV2: {
                    const DecimalV2Value decimal_val(
                            reinterpret_cast<const PackedInt128*>(col.column->get_data_at(i).data)
                                    ->value);
                    std::string decimal_str;
                    decimal_str = decimal_val.to_string();
                    _plain_text_outstream << decimal_str;
                    break;
                }
                case TYPE_DECIMAL32: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_DECIMAL64: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_DECIMAL128I: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                case TYPE_ARRAY: {
                    _plain_text_outstream << col.type->to_string(*col.column, i);
                    break;
                }
                default: {
                    // not supported type, like BITMAP, just export null
                    _plain_text_outstream << NULL_IN_CSV;
                }
                }
            }
            if (col_id < block.columns() - 1) {
                _plain_text_outstream << _file_opts->column_separator;
            }
        }
        _plain_text_outstream << _file_opts->line_delimiter;
    }

    return _flush_plain_text_outstream(true);
}

std::string VFileResultWriter::gen_types() {
    std::string types;
    int num_columns = _output_vexpr_ctxs.size();
    for (int i = 0; i < num_columns; ++i) {
        types += type_to_string(_output_vexpr_ctxs[i]->root()->type().type);
        if (i < num_columns - 1) {
            types += _file_opts->column_separator;
        }
    }
    types += _file_opts->line_delimiter;
    return types;
}

Status VFileResultWriter::write_csv_header() {
    if (!_header_sent && _header.size() > 0) {
        std::string tmp_header = _header;
        if (_header_type == BeConsts::CSV_WITH_NAMES_AND_TYPES) {
            tmp_header += gen_types();
        }
        size_t written_len = 0;
        RETURN_IF_ERROR(
                _file_writer_impl->write(reinterpret_cast<const uint8_t*>(tmp_header.c_str()),
                                         tmp_header.size(), &written_len));
        _header_sent = true;
    }
    return Status::OK();
}

Status VFileResultWriter::_flush_plain_text_outstream(bool eos) {
    SCOPED_TIMER(_file_write_timer);
    size_t pos = _plain_text_outstream.tellp();
    if (pos == 0 || (pos < OUTSTREAM_BUFFER_SIZE_BYTES && !eos)) {
        return Status::OK();
    }

    const std::string& buf = _plain_text_outstream.str();
    size_t written_len = 0;
    RETURN_IF_ERROR(_file_writer_impl->write(reinterpret_cast<const uint8_t*>(buf.c_str()),
                                             buf.size(), &written_len));
    COUNTER_UPDATE(_written_data_bytes, written_len);
    _current_written_bytes += written_len;

    // clear the stream
    _plain_text_outstream.str("");
    _plain_text_outstream.clear();

    // split file if exceed limit
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

Status VFileResultWriter::_close_file_writer(bool done, bool only_close) {
    if (_vfile_writer) {
        _vfile_writer->close();
        COUNTER_UPDATE(_written_data_bytes, _current_written_bytes);
        _vfile_writer.reset(nullptr);
    } else if (_file_writer_impl) {
        _file_writer_impl->close();
    }

    if (only_close) {
        return Status::OK();
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
    MysqlRowBuffer row_buffer;
    row_buffer.push_int(_file_idx);                         // file number
    row_buffer.push_bigint(_written_rows_counter->value()); // total rows
    row_buffer.push_bigint(_written_data_bytes->value());   // file size
    std::string file_url;
    _get_file_url(&file_url);
    row_buffer.push_string(file_url.c_str(), file_url.length()); // url

    std::unique_ptr<TFetchDataResult> result = std::make_unique<TFetchDataResult>();
    result->result_batch.rows.resize(1);
    result->result_batch.rows[0].assign(row_buffer.buf(), row_buffer.length());
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
        _get_file_url(&file_url);                                                   \
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

Status VFileResultWriter::close() {
    // the following 2 profile "_written_rows_counter" and "_writer_close_timer"
    // must be outside the `_close_file_writer()`.
    // because `_close_file_writer()` may be called in deconstructor,
    // at that time, the RuntimeState may already been deconstructed,
    // so does the profile in RuntimeState.
    COUNTER_SET(_written_rows_counter, _written_rows);
    SCOPED_TIMER(_writer_close_timer);
    return _close_file_writer(true);
}

} // namespace doris::vectorized
