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

#include "runtime/export_sink.h"

#include <thrift/protocol/TDebugProtocol.h>

#include <sstream>

#include "exec/broker_writer.h"
#include "exec/hdfs_reader_writer.h"
#include "exec/local_file_writer.h"
#include "exec/s3_writer.h"
#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "gutil/strings/numbers.h"
#include "runtime/mem_tracker.h"
#include "runtime/mysql_table_sink.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/tuple_row.h"
#include "util/runtime_profile.h"
#include "util/types.h"
#include "util/uid_util.h"

namespace doris {

ExportSink::ExportSink(ObjectPool* pool, const RowDescriptor& row_desc,
                       const std::vector<TExpr>& t_exprs)
        : _pool(pool),
          _row_desc(row_desc),
          _t_output_expr(t_exprs),
          _bytes_written_counter(nullptr),
          _rows_written_counter(nullptr),
          _write_timer(nullptr) {
    _name = "ExportSink";
}

ExportSink::~ExportSink() {}

Status ExportSink::init(const TDataSink& t_sink) {
    RETURN_IF_ERROR(DataSink::init(t_sink));
    _t_export_sink = t_sink.export_sink;

    // From the thrift expressions create the real exprs.
    RETURN_IF_ERROR(Expr::create_expr_trees(_pool, _t_output_expr, &_output_expr_ctxs));
    return Status::OK();
}

Status ExportSink::prepare(RuntimeState* state) {
    RETURN_IF_ERROR(DataSink::prepare(state));

    _state = state;

    std::stringstream title;
    title << "ExportSink (frag_id=" << state->fragment_instance_id() << ")";
    // create profile
    _profile = state->obj_pool()->add(new RuntimeProfile(title.str()));
    SCOPED_TIMER(_profile->total_time_counter());

    _mem_tracker = MemTracker::CreateTracker(-1, "ExportSink", state->instance_mem_tracker());

    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::prepare(_output_expr_ctxs, state, _row_desc, _mem_tracker));

    // TODO(lingbin): add some Counter
    _bytes_written_counter = ADD_COUNTER(profile(), "BytesExported", TUnit::BYTES);
    _rows_written_counter = ADD_COUNTER(profile(), "RowsExported", TUnit::UNIT);
    _write_timer = ADD_TIMER(profile(), "WriteTime");

    return Status::OK();
}

Status ExportSink::open(RuntimeState* state) {
    // Prepare the exprs to run.
    RETURN_IF_ERROR(Expr::open(_output_expr_ctxs, state));
    // open broker
    RETURN_IF_ERROR(open_file_writer());
    return Status::OK();
}

Status ExportSink::send(RuntimeState* state, RowBatch* batch) {
    VLOG_ROW << "debug: export_sink send batch: " << batch->to_string();
    SCOPED_TIMER(_profile->total_time_counter());
    int num_rows = batch->num_rows();
    // we send at most 1024 rows at a time
    int batch_send_rows = num_rows > 1024 ? 1024 : num_rows;
    std::stringstream ss;
    for (int i = 0; i < num_rows;) {
        ss.str("");
        for (int j = 0; j < batch_send_rows && i < num_rows; ++j, ++i) {
            RETURN_IF_ERROR(gen_row_buffer(batch->get_row(i), &ss));
        }

        VLOG_ROW << "debug: export_sink send row: " << ss.str();
        const std::string& buf = ss.str();
        size_t written_len = 0;

        SCOPED_TIMER(_write_timer);
        // TODO(lingbin): for broker writer, we should not send rpc each row.
        RETURN_IF_ERROR(_file_writer->write(reinterpret_cast<const uint8_t*>(buf.c_str()),
                                            buf.size(), &written_len));
        COUNTER_UPDATE(_bytes_written_counter, buf.size());
    }
    COUNTER_UPDATE(_rows_written_counter, num_rows);
    return Status::OK();
}

Status ExportSink::gen_row_buffer(TupleRow* row, std::stringstream* ss) {
    int num_columns = _output_expr_ctxs.size();
    // const TupleDescriptor& desc = row_desc().TupleDescriptor;
    for (int i = 0; i < num_columns; ++i) {
        void* item = _output_expr_ctxs[i]->get_value(row);
        if (item == nullptr) {
            (*ss) << "\\N";
        } else {
            switch (_output_expr_ctxs[i]->root()->type().type) {
            case TYPE_BOOLEAN:
            case TYPE_TINYINT:
                (*ss) << (int)*static_cast<int8_t*>(item);
                break;
            case TYPE_SMALLINT:
                (*ss) << *static_cast<int16_t*>(item);
                break;
            case TYPE_INT:
                (*ss) << *static_cast<int32_t*>(item);
                break;
            case TYPE_BIGINT:
                (*ss) << *static_cast<int64_t*>(item);
                break;
            case TYPE_LARGEINT:
                (*ss) << reinterpret_cast<PackedInt128*>(item)->value;
                break;
            case TYPE_FLOAT: {
                char buffer[MAX_FLOAT_STR_LENGTH + 2];
                float float_value = *static_cast<float*>(item);
                buffer[0] = '\0';
                int length = FloatToBuffer(float_value, MAX_FLOAT_STR_LENGTH, buffer);
                DCHECK(length >= 0) << "gcvt float failed, float value=" << float_value;
                (*ss) << buffer;
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
                (*ss) << buffer;
                break;
            }
            case TYPE_DATE:
            case TYPE_DATETIME: {
                char buf[64];
                const DateTimeValue* time_val = (const DateTimeValue*)(item);
                time_val->to_string(buf);
                (*ss) << buf;
                break;
            }
            case TYPE_VARCHAR:
            case TYPE_CHAR:
            case TYPE_STRING: {
                const StringValue* string_val = (const StringValue*)(item);

                if (string_val->ptr == nullptr) {
                    if (string_val->len == 0) {
                    } else {
                        (*ss) << "\\N";
                    }
                } else {
                    (*ss) << std::string(string_val->ptr, string_val->len);
                }
                break;
            }

            case TYPE_DECIMALV2: {
                const DecimalV2Value decimal_val(
                        reinterpret_cast<const PackedInt128*>(item)->value);
                std::string decimal_str;
                int output_scale = _output_expr_ctxs[i]->root()->output_scale();
                decimal_str = decimal_val.to_string(output_scale);
                (*ss) << decimal_str;
                break;
            }
            default: {
                std::stringstream err_ss;
                err_ss << "can't export this type. type = " << _output_expr_ctxs[i]->root()->type();
                return Status::InternalError(err_ss.str());
            }
            }
        }

        if (i < num_columns - 1) {
            (*ss) << _t_export_sink.column_separator;
        }
    }
    (*ss) << _t_export_sink.line_delimiter;

    return Status::OK();
}

Status ExportSink::close(RuntimeState* state, Status exec_status) {
    if (_closed) {
        return Status::OK();
    }
    Expr::close(_output_expr_ctxs, state);
    if (_file_writer != nullptr) {
        _file_writer->close();
        _file_writer = nullptr;
    }
    return DataSink::close(state, exec_status);
}

Status ExportSink::open_file_writer() {
    if (_file_writer != nullptr) {
        return Status::OK();
    }

    std::string file_name = gen_file_name();

    // TODO(lingbin): gen file path
    switch (_t_export_sink.file_type) {
    case TFileType::FILE_LOCAL: {
        LocalFileWriter* file_writer =
                new LocalFileWriter(_t_export_sink.export_path + "/" + file_name, 0);
        RETURN_IF_ERROR(file_writer->open());
        _file_writer.reset(file_writer);
        break;
    }
    case TFileType::FILE_BROKER: {
        BrokerWriter* broker_writer = new BrokerWriter(
                _state->exec_env(), _t_export_sink.broker_addresses, _t_export_sink.properties,
                _t_export_sink.export_path + "/" + file_name, 0 /* offset */);
        RETURN_IF_ERROR(broker_writer->open());
        _file_writer.reset(broker_writer);
        break;
    }
    case TFileType::FILE_S3: {
        S3Writer* s3_writer =
                new S3Writer(_t_export_sink.properties,
                             _t_export_sink.export_path + "/" + file_name, 0 /* offset */);
        RETURN_IF_ERROR(s3_writer->open());
        _file_writer.reset(s3_writer);
        break;
    }
    case TFileType::FILE_HDFS: {
        FileWriter* hdfs_writer;
        RETURN_IF_ERROR(HdfsReaderWriter::create_writer(
                const_cast<std::map<std::string, std::string>&>(_t_export_sink.properties),
                _t_export_sink.export_path + "/" + file_name, &hdfs_writer));
        RETURN_IF_ERROR(hdfs_writer->open());
        _file_writer.reset(hdfs_writer);
        break;
    }
    default: {
        std::stringstream ss;
        ss << "Unknown file type, type=" << _t_export_sink.file_type;
        return Status::InternalError(ss.str());
    }
    }

    _state->add_export_output_file(_t_export_sink.export_path + "/" + file_name);
    return Status::OK();
}

// TODO(lingbin): add some other info to file name, like partition
std::string ExportSink::gen_file_name() {
    const TUniqueId& id = _state->fragment_instance_id();

    struct timeval tv;
    gettimeofday(&tv, nullptr);

    std::stringstream file_name;
    file_name << "export-data-" << print_id(id) << "-" << (tv.tv_sec * 1000 + tv.tv_usec / 1000);
    return file_name.str();
}

} // namespace doris
