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
#include "runtime/primitive_type.h"
#include "runtime/row_batch.h"
#include "runtime/tuple_row.h"
#include "runtime/runtime_state.h"
#include "util/types.h"
#include "util/date_func.h"

#include "gen_cpp/PaloInternalService_types.h"

namespace doris {

FileResultWriter::FileResultWriter(
        const ResultFileOptions* file_opts,
        const std::vector<ExprContext*>& output_expr_ctxs) :
            _file_opts(file_opts),
            _output_expr_ctxs(output_expr_ctxs) {
}

FileResultWriter::~FileResultWriter() {
    close();
}

Status FileResultWriter::init(RuntimeState* state) {
    if (_file_opts->is_local_file) {
        _file_writer = new LocalFileWriter(_file_opts->file_path, 0 /* start offset */);
    } else {
        _file_writer = new BrokerWriter(state->exec_env(),
                _file_opts->broker_addresses,
                _file_opts->broker_properties,
                _file_opts->file_path,
                0 /*start offset*/);
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
            return Status::InternalError(strings::Substitute("unsupport file format: $0", _file_opts->file_format));
    }

    return Status::OK();
}

Status FileResultWriter::append_row_batch(RowBatch* batch) {
    if (NULL == batch || 0 == batch->num_rows()) {
        return Status::OK();
    }

    if (_parquet_writer != nullptr) {
        RETURN_IF_ERROR(_parquet_writer->write(*batch));
    } else {
        RETURN_IF_ERROR(_write_csv_file(*batch));
    }

    return Status::OK();
}

Status FileResultWriter::_write_csv_file(const RowBatch& batch) {
    int num_rows = batch.num_rows();
    for (int i = 0; i < num_rows; ++i) {
        TupleRow* row = batch.get_row(i);
        RETURN_IF_ERROR(_write_one_row_as_csv(row));
    }
    return Status::OK();
}

// actually, this logic is same as `ExportSink::gen_row_buffer`
// TODO(cmy): find a way to unify them.
Status FileResultWriter::_write_one_row_as_csv(TupleRow* row) {
    std::stringstream ss;
    int num_columns = _output_expr_ctxs.size();
    for (int i = 0; i < num_columns; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);

        if (item == nullptr) {
            ss << NULL_IN_CSV;
            continue;
        }

        switch (_output_expr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                ss << (int)*static_cast<int8_t*>(item);
                break;
            case TYPE_SMALLINT:
                ss << *static_cast<int16_t*>(item);
                break;
            case TYPE_INT:
                ss << *static_cast<int32_t*>(item);
                break;
            case TYPE_BIGINT:
                ss << *static_cast<int64_t*>(item);
                break;
            case TYPE_LARGEINT:
                ss << reinterpret_cast<PackedInt128*>(item)->value;
                break;
            case TYPE_FLOAT: {
                char buffer[MAX_FLOAT_STR_LENGTH + 2];
                float float_value = *static_cast<float*>(item);
                buffer[0] = '\0';
                int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
                DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
                ss << buffer;
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
                ss << buffer;
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                char buf[64];
                const DateTimeValue* time_val = (const DateTimeValue*)(item);
                time_val->to_string(buf);
                ss << buf;
                break;
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR: {
                const StringValue* string_val = (const StringValue*)(item);
                if (string_val->ptr == NULL) {
                    if (string_val->len != 0) {
                        ss << NULL_IN_CSV;
                    }
                } else {
                    ss << std::string(string_val->ptr, string_val->len);
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
                ss << decimal_str;
                break;
            }
            case TYPE_DECIMALV2: {
                const DecimalV2Value decimal_val(reinterpret_cast<const PackedInt128*>(item)->value);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();
                if (output_scale > 0 && output_scale <= 30) {
                    decimal_str = decimal_val.to_string(output_scale);
                } else {
                    decimal_str = decimal_val.to_string();
                }
                ss << decimal_str;
                break;
            }
            default: {
               std::stringstream err_ss;
               err_ss << "can't export this type. type = " << _output_expr_ctxs[i]->root()->type();
               return Status::InternalError(err_ss.str());
            }
        }
        if (i < num_columns - 1) {
            ss << _file_opts->column_separator;
        }
    } // end for columns
    ss << _file_opts->line_delimiter;

    // write one line to file
    const std::string& buf = ss.str();
    size_t written_len = 0;
    RETURN_IF_ERROR(_file_writer->write(reinterpret_cast<const uint8_t*>(buf.c_str()),
            buf.size(), &written_len));

    return Status::OK();
}

Status FileResultWriter::close() {
    if (_parquet_writer != nullptr) {
        _parquet_writer->close();
        delete _parquet_writer;
    } else if (_file_writer != nullptr) {
        _file_writer->close();
        delete _file_writer;
    }
    return Status::OK();
}

}

/* vim: set ts=4 sw=4 sts=4 tw=100 expandtab : */
