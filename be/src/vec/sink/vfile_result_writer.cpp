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

#include "vec/sink/vfile_result_writer.h"

#include "exec/broker_writer.h"
#include "exec/hdfs_reader_writer.h"
#include "exec/local_file_writer.h"
#include "exec/parquet_writer.h"
#include "exec/s3_writer.h"
#include "gutil/strings/substitute.h"
#include "runtime/buffer_control_block.h"
#include "service/backend_options.h"
#include "util/file_utils.h"
#include "util/url_coding.h"
#include "vec/core/materialize_block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"

namespace doris {
namespace vectorized {

VFileResultWriter::VFileResultWriter(const ResultFileOptions* file_opts,
                                     const std::vector<VExprContext*>* output_vexpr_ctxs,
                                     RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                                     bool output_object_data)
        : FileResultWriter(file_opts, nullptr, parent_profile, sinker, output_object_data) {
    _output_vexpr_ctxs = output_vexpr_ctxs;
}

VFileResultWriter::VFileResultWriter(const ResultFileOptions* file_opts,
                                     const TStorageBackendType::type storage_type,
                                     const TUniqueId fragment_instance_id,
                                     const std::vector<VExprContext*>* output_vexpr_ctxs,
                                     RuntimeProfile* parent_profile, BufferControlBlock* sinker,
                                     MutableBlock* output_batch, bool output_object_data)
        : FileResultWriter(file_opts, storage_type, fragment_instance_id, nullptr, parent_profile,
                           sinker, nullptr, output_object_data) {
    _output_vexpr_ctxs = output_vexpr_ctxs;
    _mutable_block = output_batch;
}

VFileResultWriter::~VFileResultWriter() {
    _close_file_writer(true);
}

Status VFileResultWriter::init(RuntimeState* state) {
    _state = state;
    _init_profile();
    return _create_next_file_writer();
}

Status VFileResultWriter::_create_file_writer(const std::string& file_name) {
    if (_storage_type == TStorageBackendType::LOCAL) {
        _file_writer = new doris::LocalFileWriter(file_name, 0 /* start offset */);
    } else if (_storage_type == TStorageBackendType::BROKER) {
        _file_writer = new doris::BrokerWriter(_state->exec_env(), _file_opts->broker_addresses,
                                               _file_opts->broker_properties, file_name,
                                               0 /*start offset*/);
    } else if (_storage_type == TStorageBackendType::S3) {
        _file_writer =
                new doris::S3Writer(_file_opts->broker_properties, file_name, 0 /* offset */);
    } else if (_storage_type == TStorageBackendType::HDFS) {
        RETURN_IF_ERROR(HdfsReaderWriter::create_writer(
                const_cast<std::map<std::string, std::string>&>(_file_opts->broker_properties),
                file_name, &_file_writer));
    }

    RETURN_IF_ERROR(_file_writer->open());
    switch (_file_opts->file_format) {
    case TFileFormatType::FORMAT_CSV_PLAIN:
        // just use file writer is enough
        break;

    case TFileFormatType::FORMAT_PARQUET:
        _parquet_writer = new doris::ParquetWriterWrapper(
                _file_writer, _output_vexpr_ctxs, _file_opts->file_properties, _file_opts->schema,
                FileResultWriter::_output_object_data);
        break;

    default:
        return Status::InternalError(
                strings::Substitute("unsupported file format: $0", _file_opts->file_format));
    }
    LOG(INFO) << "create file for exporting query result. file name: " << file_name
              << ". query id: " << print_id(_state->query_id())
              << " format:" << _file_opts->file_format;
    return Status::OK();
}

Status VFileResultWriter::append_row_batch(const RowBatch* batch) {
    return Status::NotSupported(
            "Not Implemented VFileResultWriter::append_row_batch(RowBatch* batch)");
}

Status VFileResultWriter::append_block(Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    SCOPED_TIMER(_append_row_batch_timer);
    if (_parquet_writer != nullptr) {
        RETURN_IF_ERROR(_write_parquet_file(block));
    } else {
        RETURN_IF_ERROR(_write_csv_file(block));
    }

    FileResultWriter::_written_rows += block.rows();
    return Status::OK();
}

Status VFileResultWriter::_write_parquet_file(Block& block) {
    RETURN_IF_ERROR(_parquet_writer->write(block));
    // split file if exceed limit
    return _create_new_file_if_exceed_size();
}

Status VFileResultWriter::_write_csv_file(Block& block) {
    int num_columns = _output_vexpr_ctxs->size();
    _column_ids.resize(num_columns);
    for (int i = 0; i < num_columns; ++i) {
        int column_id = -1;
        (*_output_vexpr_ctxs)[i]->execute(&block, &column_id);
        _column_ids[i] = column_id;
    }

    int num_rows = block.rows();
    materialize_block_inplace(block, _column_ids.begin(), _column_ids.end());
    for (int i = 0; i < num_rows; ++i) {
        RETURN_IF_ERROR(_write_one_row_as_csv(block, i));
    }
    return _flush_plain_text_outstream(true);
}

Status VFileResultWriter::_write_one_row_as_csv(Block& block, size_t row) {
    SCOPED_TIMER(_convert_tuple_timer);
    int num_columns = _output_vexpr_ctxs->size();

    for (int i = 0; i < num_columns; ++i) {
        auto& column_ptr = block.get_by_position(_column_ids[i]).column;
        auto& type_ptr = block.get_by_position(_column_ids[i]).type;
        vectorized::ColumnPtr column;
        if (type_ptr->is_nullable()) {
            column = assert_cast<const vectorized::ColumnNullable&>(*column_ptr)
                             .get_nested_column_ptr();
            if (column_ptr->is_null_at(row)) {
                fmt::format_to(_insert_stmt_buffer, "{}", NULL_IN_CSV);

                if (i < num_columns - 1) {
                    fmt::format_to(_insert_stmt_buffer, "{}", _file_opts->column_separator);
                }
                continue;
            }
        } else {
            column = column_ptr;
        }
        switch ((*_output_vexpr_ctxs)[i]->root()->result_type()) {
        case TYPE_BOOLEAN: {
            auto& data = assert_cast<const vectorized::ColumnUInt8&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_TINYINT: {
            auto& data = assert_cast<const vectorized::ColumnInt8&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_SMALLINT: {
            auto& data = assert_cast<const vectorized::ColumnInt16&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_INT: {
            auto& data = assert_cast<const vectorized::ColumnInt32&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_BIGINT: {
            auto& data = assert_cast<const vectorized::ColumnInt64&>(*column).get_data();
            fmt::format_to(_insert_stmt_buffer, "{}", data[row]);
            break;
        }
        case TYPE_FLOAT: {
            auto& data = assert_cast<const vectorized::ColumnFloat32&>(*column).get_data();
            // To prevent loss of precision on float and double types,
            char buffer[MAX_FLOAT_STR_LENGTH + 2];
            float float_value = static_cast<float>(data[row]);
            buffer[0] = '\0';
            int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
            DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
            fmt::format_to(_insert_stmt_buffer, "{}", buffer);
            break;
        }
        case TYPE_DOUBLE: {
            auto& data = assert_cast<const vectorized::ColumnFloat64&>(*column).get_data();
            char buffer[MAX_DOUBLE_STR_LENGTH + 2];
            double double_value = static_cast<double>(data[row]);
            buffer[0] = '\0';
            int length = DoubleToBuffer(double_value, MAX_DOUBLE_STR_LENGTH, buffer);
            DCHECK(length >= 0) << "gcvt double failed, double value=" << double_value;
            fmt::format_to(_insert_stmt_buffer, "{}", buffer);
            break;
        }

        case TYPE_STRING:
        case TYPE_CHAR:
        case TYPE_VARCHAR: {
            const auto& string_val =
                    assert_cast<const vectorized::ColumnString&>(*column).get_data_at(row);
            DCHECK(string_val.data != nullptr);
            fmt::format_to(_insert_stmt_buffer, "{}",
                           std::string(string_val.data, string_val.size));
            break;
        }
        case TYPE_DECIMALV2: {
            DecimalV2Value value =
                    (DecimalV2Value)
                            assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>&>(
                                    *column)
                                    .get_data()[row];
            fmt::format_to(_insert_stmt_buffer, "{}", value.to_string());
            break;
        }
        case TYPE_DATE:
        case TYPE_DATETIME: {
            int64_t int_val = assert_cast<const vectorized::ColumnInt64&>(*column).get_data()[row];
            vectorized::VecDateTimeValue value =
                    binary_cast<int64_t, doris::vectorized::VecDateTimeValue>(int_val);

            char buf[64];
            char* pos = value.to_string(buf);
            std::string str(buf, pos - buf - 1);
            fmt::format_to(_insert_stmt_buffer, "{}", str);
            break;
        }
        case TYPE_OBJECT: {
            if (FileResultWriter::_output_object_data) {
                auto date_column = assert_cast<const vectorized::ColumnBitmap&>(*column);
                BitmapValue& data = date_column.get_element(row);
                std::string bitmap_str(data.getSizeInBytes(), '0');
                data.write(bitmap_str.data());

                std::string base64_str;
                base64_encode(bitmap_str, &base64_str);
                fmt::format_to(_insert_stmt_buffer, "{}", base64_str);
            } else {
                fmt::format_to(_insert_stmt_buffer, "{}", NULL_IN_CSV);
            }
            break;
        }
        case TYPE_HLL: {
            if (FileResultWriter::_output_object_data) {
                const auto& data =
                        assert_cast<const vectorized::ColumnHLL&>(*column).get_data()[row];
                std::string hll_str(data.max_serialized_size(), '0');
                size_t actual_size = data.serialize((uint8_t*)hll_str.data());
                hll_str.resize(actual_size);

                std::string base64_str;
                base64_encode(hll_str, &base64_str);
                fmt::format_to(_insert_stmt_buffer, "{}", base64_str);
            } else {
                fmt::format_to(_insert_stmt_buffer, "{}", NULL_IN_CSV);
            }
            break;
        }
        default: {
            fmt::format_to(_insert_stmt_buffer, "{}", NULL_IN_CSV);
        }
        }
        if (i < num_columns - 1) {
            fmt::format_to(_insert_stmt_buffer, "{}", _file_opts->column_separator);
        }
    }

    fmt::format_to(_insert_stmt_buffer, "{}", _file_opts->line_delimiter);
    // write one line to file
    return _flush_plain_text_outstream(false);
}

Status VFileResultWriter::_flush_plain_text_outstream(bool eos) {
    SCOPED_TIMER(_file_write_timer);

    size_t pos = _insert_stmt_buffer.size();
    if (pos == 0 || (pos < OUTSTREAM_BUFFER_SIZE_BYTES && !eos)) {
        return Status::OK();
    }

    const std::string& buf = to_string(_insert_stmt_buffer);
    size_t written_len = 0;
    RETURN_IF_ERROR(_file_writer->write(reinterpret_cast<const uint8_t*>(buf.c_str()), buf.size(),
                                        &written_len));
    COUNTER_UPDATE(_written_data_bytes, written_len);
    _current_written_bytes += written_len;

    _insert_stmt_buffer.clear();
    // split file if exceed limit
    return _create_new_file_if_exceed_size();
}

Status VFileResultWriter::_close_file_writer(bool done, bool only_close) {
    if (_parquet_writer != nullptr) {
        _parquet_writer->close();
        _current_written_bytes = _parquet_writer->written_len();
        COUNTER_UPDATE(_written_data_bytes, _current_written_bytes);
        delete _parquet_writer;
        _parquet_writer = nullptr;
        delete _file_writer;
        _file_writer = nullptr;
    } else if (_file_writer != nullptr) {
        _file_writer->close();
        delete _file_writer;
        _file_writer = nullptr;
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
        if (_mutable_block == nullptr) {
            RETURN_IF_ERROR(_send_result());
        } else {
            RETURN_IF_ERROR(_fill_result_batch());
        }
    }
    return Status::OK();
}

Status VFileResultWriter::_fill_result_batch() {
    if (_is_result_sent) {
        return Status::OK();
    }
    _is_result_sent = true;
    int64_t result_data[3] = {_file_idx, _written_rows_counter->value(),
                              _written_data_bytes->value()}; // file_num, total rows,file size
    std::string file_url;
    _get_file_url(&file_url); // url address

    auto& res_columns = _mutable_block->mutable_columns();
    for (int i = 0; i < 3; ++i) {
        res_columns[i]->insert_data((const char*)(&result_data[i]), 0);
    }
    res_columns[3]->insert_data(file_url.c_str(), file_url.length());

    return Status::OK();
}

Status VFileResultWriter::close() {
    // the following 2 profile "_written_rows_counter" and "_writer_close_timer"
    // must be outside the `_close_file_writer()`.
    // because `_close_file_writer()` may be called in deconstructor,
    // at that time, the RuntimeState may already been deconstructed,
    // so does the profile in RuntimeState.
    COUNTER_SET(_written_rows_counter, FileResultWriter::_written_rows);
    SCOPED_TIMER(_writer_close_timer);
    return _close_file_writer(true, false);
}

} // namespace vectorized
} // namespace doris