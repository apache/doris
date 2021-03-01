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

#include "runtime/file_result_writer.h"

#include "exec/broker_writer.h"
#include "exec/local_file_writer.h"
#include "exec/parquet_writer.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gen_cpp/PaloInternalService_types.h"
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/date_func.h"
#include "util/types.h"
#include "util/uid_util.h"

namespace doris {

const size_t FileResultWriter::OUTSTREAM_BUFFER_SIZE_BYTES = 1024 * 1024;

FileResultWriter::FileResultWriter(const ResultFileOptions* file_opts,
                                   const std::vector<ExprContext*>& output_expr_ctxs,
                                   RuntimeProfile* parent_profile)
        : _file_opts(file_opts),
          _output_expr_ctxs(output_expr_ctxs),
          _parent_profile(parent_profile) {}

FileResultWriter::~FileResultWriter() {
    _close_file_writer(true);
}

Status FileResultWriter::init(RuntimeState* state) {
    _state = state;
    _init_profile();

    RETURN_IF_ERROR(_create_file_writer());
    return Status::OK();
}

void FileResultWriter::_init_profile() {
    RuntimeProfile* profile = _parent_profile->create_child("FileResultWriter", true, true);
    _append_row_batch_timer = ADD_TIMER(profile, "AppendBatchTime");
    _convert_tuple_timer = ADD_CHILD_TIMER(profile, "TupleConvertTime", "AppendBatchTime");
    _file_write_timer = ADD_CHILD_TIMER(profile, "FileWriteTime", "AppendBatchTime");
    _writer_close_timer = ADD_TIMER(profile, "FileWriterCloseTime");
    _written_rows_counter = ADD_COUNTER(profile, "NumWrittenRows", TUnit::UNIT);
    _written_data_bytes = ADD_COUNTER(profile, "WrittenDataBytes", TUnit::BYTES);
}

Status FileResultWriter::_create_file_writer() {
    std::string file_name = _get_next_file_name();
    if (_file_opts->is_local_file) {
        _file_writer = new LocalFileWriter(file_name, 0 /* start offset */);
    } else {
        _file_writer =
                new BrokerWriter(_state->exec_env(), _file_opts->broker_addresses,
                                 _file_opts->broker_properties, file_name, 0 /*start offset*/);
    }
    RETURN_IF_ERROR(_file_writer->open());

    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        // just use file writer is enough
        break;
    case TFileFormatType::FORMAT_PARQUET:
        _parquet_writer = new ParquetWriterWrapper(_file_writer, _output_expr_ctxs);
        break;
    default:
        return Status::InternalError(
                strings::Substitute("unsupported file format: $0", _file_opts->file_format));
    }
    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id());
    return Status::OK();
}

// file name format as: my_prefix_0.csv
std::string FileResultWriter::_get_next_file_name() {
    std::stringstream ss;
    ss << _file_opts->file_path << (_file_idx++) << "." << _file_format_to_name();
    return ss.str();
}

std::string FileResultWriter::_file_format_to_name() {
    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        return "csv";
    case TFileFormatType::FORMAT_PARQUET:
        return "parquet";
    default:
        return "unknown";
    }
}

Status FileResultWriter::append_row_batch(const RowBatch* batch) {
    if (nullptr == batch || 0 == batch->num_rows()) {
        return Status::OK();
    }

    SCOPED_TIMER(_append_row_batch_timer);
    if (_parquet_writer != nullptr) {
        RETURN_IF_ERROR(_parquet_writer->write(*batch));
    } else {
        RETURN_IF_ERROR(_write_csv_file(*batch));
    }

    _written_rows += batch->num_rows();
    return Status::OK();
}

Status FileResultWriter::_write_csv_file(const RowBatch& batch) {
    int num_rows = batch.num_rows();
    for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = batch.get_row(i);
        RETURN_IF_ERROR(_write_one_row_as_csv(row));
    }
    _flush_plain_text_outstream(true);
    return Status::OK();
}

// actually, this logic is same as `ExportSink::gen_row_buffer`
// TODO(cmy): find a way to unify them.
Status FileResultWriter::_write_one_row_as_csv(TupleRow* row) {
    {
        SCOPED_TIMER(_convert_tuple_timer);
        int num_columns = _output_expr_ctxs.size();
        for (int i = 0; i < num_columns; ++i) {
            void* item = _output_expr_ctxs[i]->get_value(row);

            if (item == nullptr) {
                _plain_text_outstream << NULL_IN_CSV;
                if (i < num_columns - 1) {
                    _plain_text_outstream << _file_opts->column_separator;
                }
                continue;
            }

            switch (_output_expr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                _plain_text_outstream << (int)*static_cast<int8_t*>(item);
                break;
            case TYPE_SMALLINT:
                _plain_text_outstream << *static_cast<int16_t*>(item);
                break;
            case TYPE_INT:
                _plain_text_outstream << *static_cast<int32_t*>(item);
                break;
            case TYPE_BIGINT:
                _plain_text_outstream << *static_cast<int64_t*>(item);
                break;
            case TYPE_LARGEINT:
                _plain_text_outstream << reinterpret_cast<PackedInt128*>(item)->value;
                break;
            case TYPE_FLOAT: {
                char buffer[MAX_FLOAT_STR_LENGTH + 2];
                float float_value = *static_cast<float*>(item);
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
                double double_value = *static_cast<double*>(item);
                buffer[0] = '\0';
                int length = DoubleToBuffer(double_value, MAX_DOUBLE_STR_LENGTH, buffer);
                DCHECK(length >= 0) << "gcvt double failed, double value=" << double_value;
                _plain_text_outstream << buffer;
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                char buf[64];
                const DateTimeValue* time_val = (const DateTimeValue*)(item);
                time_val->to_string(buf);
                _plain_text_outstream << buf;
                break;
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR: {
                const StringValue* string_val = (const StringValue*)(item);
                if (string_val->ptr == NULL) {
                    if (string_val->len != 0) {
                        _plain_text_outstream << NULL_IN_CSV;
                    }
                } else {
                    _plain_text_outstream << std::string(string_val->ptr, string_val->len);
                }
                break;
            }
            case TYPE_DECIMAL: {
                const DecimalValue* decimal_val = reinterpret_cast<const DecimalValue*>(item);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();
                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val->to_string(output_scale);
                } else {
                    decimal_str = decimal_val->to_string();
                }
                _plain_text_outstream << decimal_str;
                break;
            }
            case TYPE_DECIMALV2: {
                const DecimalV2Value decimal_val(
                        reinterpret_cast<const PackedInt128*>(item)->value);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();
                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val.to_string(output_scale);
                } else {
                    decimal_str = decimal_val.to_string();
                }
                _plain_text_outstream << decimal_str;
                break;
            }
            default: {
                // not supported type, like BITMAP, HLL, just export null
                _plain_text_outstream << NULL_IN_CSV;
            }
            }
            if (i < num_columns - 1) {
                _plain_text_outstream << _file_opts->column_separator;
            }
        } // end for columns
        _plain_text_outstream << _file_opts->line_delimiter;
    }

    // write one line to file
    return _flush_plain_text_outstream(false);
}

Status FileResultWriter::_flush_plain_text_outstream(bool eos) {
    SCOPED_TIMER(_file_write_timer);
    size_t pos = _plain_text_outstream.tellp();
    if (pos == 0 || (pos < OUTSTREAM_BUFFER_SIZE_BYTES && !eos)) {
        return Status::OK();
    }

    const std::string& buf = _plain_text_outstream.str();
    size_t written_len = 0;
    RETURN_IF_ERROR(_file_writer->write(reinterpret_cast<const uint8_t*>(buf.c_str()), buf.size(),
                                        &written_len));
    COUNTER_UPDATE(_written_data_bytes, written_len);
    _current_written_bytes += written_len;

    // clear the stream
    _plain_text_outstream.str("");
    _plain_text_outstream.clear();

    // split file if exceed limit
    RETURN_IF_ERROR(_create_new_file_if_exceed_size());

    return Status::OK();
}

Status FileResultWriter::_create_new_file_if_exceed_size() {
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

Status FileResultWriter::_close_file_writer(bool done) {
    if (_parquet_writer != nullptr) {
        _parquet_writer->close();
        delete _parquet_writer;
        _parquet_writer = nullptr;
        if (!done) {
            //TODO(cmy): implement parquet writer later
        }
    } else if (_file_writer != nullptr) {
        _file_writer->close();
        delete _file_writer;
        _file_writer = nullptr;
    }

    if (!done) {
        // not finished, create new file writer for next file
        RETURN_IF_ERROR(_create_file_writer());
    }
    return Status::OK();
}

Status FileResultWriter::close() {
    // the following 2 profile "_written_rows_counter" and "_writer_close_timer"
    // must be outside the `_close_file_writer()`.
    // because `_close_file_writer()` may be called in deconstructor,
    // at that time, the RuntimeState may already been deconstructed,
    // so does the profile in RuntimeState.
    COUNTER_SET(_written_rows_counter, _written_rows);
    SCOPED_TIMER(_writer_close_timer);
    RETURN_IF_ERROR(_close_file_writer(true));
    return Status::OK();
}

} // namespace doris
