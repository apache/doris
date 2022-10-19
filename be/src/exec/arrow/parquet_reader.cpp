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
#include "exec/arrow/parquet_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <arrow/type_fwd.h>
#include <time.h>

#include <algorithm>
#include <cinttypes>
#include <mutex>
#include <thread>

#include "common/logging.h"
#include "common/status.h"
#include "io/file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"
#include "runtime/string_value.h"
#include "runtime/tuple.h"
#include "util/string_util.h"

namespace doris {

// Broker
ParquetReaderWrap::ParquetReaderWrap(RuntimeState* state,
                                     const std::vector<SlotDescriptor*>& file_slot_descs,
                                     FileReader* file_reader, int32_t num_of_columns_from_file,
                                     int64_t range_start_offset, int64_t range_size,
                                     bool case_sensitive)
        : ArrowReaderWrap(state, file_slot_descs, file_reader, num_of_columns_from_file,
                          case_sensitive),
          _rows_of_group(0),
          _current_line_of_group(0),
          _current_line_of_batch(0),
          _range_start_offset(range_start_offset),
          _range_size(range_size) {}

Status ParquetReaderWrap::init_reader(const TupleDescriptor* tuple_desc,
                                      const std::vector<ExprContext*>& conjunct_ctxs,
                                      const std::string& timezone) {
    try {
        parquet::ArrowReaderProperties arrow_reader_properties =
                parquet::default_arrow_reader_properties();
        arrow_reader_properties.set_pre_buffer(true);
        arrow_reader_properties.set_use_threads(true);
        // Open Parquet file reader
        auto reader_builder = parquet::arrow::FileReaderBuilder();
        reader_builder.properties(arrow_reader_properties);

        auto st = reader_builder.Open(_arrow_file);

        if (!st.ok()) {
            LOG(WARNING) << "failed to create parquet file reader, errmsg=" << st.ToString();
            return Status::InternalError("Failed to create file reader");
        }

        st = reader_builder.Build(&_reader);

        if (!st.ok()) {
            LOG(WARNING) << "failed to create parquet file reader, errmsg=" << st.ToString();
            return Status::InternalError("Failed to create file reader");
        }

        _file_metadata = _reader->parquet_reader()->metadata();
        // initial members
        _total_groups = _file_metadata->num_row_groups();
        if (_total_groups == 0) {
            return Status::EndOfFile("Empty Parquet File");
        }
        _rows_of_group = _file_metadata->RowGroup(0)->num_rows();

        // map
        auto* schemaDescriptor = _file_metadata->schema();
        for (int i = 0; i < _file_metadata->num_columns(); ++i) {
            std::string schemaName;
            // Get the Column Reader for the boolean column
            if (schemaDescriptor->Column(i)->max_definition_level() > 1) {
                schemaName = schemaDescriptor->Column(i)->path()->ToDotVector()[0];
            } else {
                schemaName = schemaDescriptor->Column(i)->name();
            }
            _map_column.emplace(_case_sensitive ? schemaName : to_lower(schemaName), i);
        }

        _timezone = timezone;

        RETURN_IF_ERROR(column_indices());
        _need_filter_row_group = (tuple_desc != nullptr);
        if (_need_filter_row_group) {
            int64_t file_size = 0;
            size(&file_size);
            _row_group_reader.reset(new RowGroupReader(_range_start_offset, _range_size,
                                                       conjunct_ctxs, _file_metadata, this));
            _row_group_reader->init_filter_groups(tuple_desc, _map_column, _include_column_ids,
                                                  file_size);
        }
        _thread = std::thread(&ArrowReaderWrap::prefetch_batch, this);
        return Status::OK();
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << "Init parquet reader fail. " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
}

Status ParquetReaderWrap::size(int64_t* size) {
    arrow::Result<int64_t> result = _arrow_file->GetSize();
    if (result.ok()) {
        *size = result.ValueOrDie();
        return Status::OK();
    } else {
        return Status::InternalError(result.status().ToString());
    }
}

inline void ParquetReaderWrap::fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool,
                                         const uint8_t* value, int32_t len) {
    tuple->set_not_null(slot_desc->null_indicator_offset());
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
    memcpy(str_slot->ptr, value, len);
    str_slot->len = len;
    return;
}

inline Status ParquetReaderWrap::set_field_null(Tuple* tuple, const SlotDescriptor* slot_desc) {
    if (!slot_desc->is_nullable()) {
        std::stringstream str_error;
        str_error << "The field name(" << slot_desc->col_name()
                  << ") is not allowed null, but Parquet field is null.";
        LOG(WARNING) << str_error.str();
        return Status::RuntimeError(str_error.str());
    }
    tuple->set_null(slot_desc->null_indicator_offset());
    return Status::OK();
}

Status ParquetReaderWrap::read_record_batch(bool* eof) {
    if (_current_line_of_group >= _rows_of_group) { // read next row group
        VLOG_DEBUG << "read_record_batch, current group id:" << _current_group
                   << " current line of group:" << _current_line_of_group
                   << " is larger than rows group size:" << _rows_of_group
                   << ". start to read next row group";
        _current_group++;
        if (_current_group >= _total_groups) { // read completed.
            _include_column_ids.clear();
            *eof = true;
            return Status::OK();
        }
        _current_line_of_group = 0;
        _rows_of_group = _file_metadata->RowGroup(_current_group)
                                 ->num_rows(); //get rows of the current row group
        // read batch
        RETURN_IF_ERROR(read_next_batch());
        _current_line_of_batch = 0;
    } else if (_current_line_of_batch >= _batch->num_rows()) {
        VLOG_DEBUG << "read_record_batch, current group id:" << _current_group
                   << " current line of batch:" << _current_line_of_batch
                   << " is larger than batch size:" << _batch->num_rows()
                   << ". start to read next batch";
        // read batch
        RETURN_IF_ERROR(read_next_batch());
        _current_line_of_batch = 0;
    }
    return Status::OK();
}

Status ParquetReaderWrap::handle_timestamp(const std::shared_ptr<arrow::TimestampArray>& ts_array,
                                           uint8_t* buf, int32_t* wbytes) {
    const auto type = std::static_pointer_cast<arrow::TimestampType>(ts_array->type());
    // Doris only supports seconds
    int64_t timestamp = 0;
    switch (type->unit()) {
    case arrow::TimeUnit::type::NANO: {                                    // INT96
        timestamp = ts_array->Value(_current_line_of_batch) / 1000000000L; // convert to Second
        break;
    }
    case arrow::TimeUnit::type::SECOND: {
        timestamp = ts_array->Value(_current_line_of_batch);
        break;
    }
    case arrow::TimeUnit::type::MILLI: {
        timestamp = ts_array->Value(_current_line_of_batch) / 1000; // convert to Second
        break;
    }
    case arrow::TimeUnit::type::MICRO: {
        timestamp = ts_array->Value(_current_line_of_batch) / 1000000; // convert to Second
        break;
    }
    default:
        return Status::InternalError("Invalid Time Type.");
    }

    DateTimeValue dtv;
    if (!dtv.from_unixtime(timestamp, _timezone)) {
        std::stringstream str_error;
        str_error << "Parse timestamp (" + std::to_string(timestamp) + ") error";
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
    char* buf_end = (char*)buf;
    buf_end = dtv.to_string((char*)buf_end);
    *wbytes = buf_end - (char*)buf - 1;
    return Status::OK();
}

Status ParquetReaderWrap::init_parquet_type() {
    // read batch
    RETURN_IF_ERROR(read_next_batch());
    _current_line_of_batch = 0;
    if (_batch == nullptr) {
        return Status::OK();
    }
    //save column type
    std::shared_ptr<arrow::Schema> field_schema = _batch->schema();
    for (int i = 0; i < _include_column_ids.size(); i++) {
        std::shared_ptr<arrow::Field> field = field_schema->field(i);
        if (!field) {
            LOG(WARNING) << "Get field schema failed. Column order:" << i;
            return Status::InternalError(_status.ToString());
        }
        _parquet_column_type.emplace_back(field->type()->id());
    }
    return Status::OK();
}

Status ParquetReaderWrap::read(Tuple* tuple, MemPool* mem_pool, bool* eof) {
    if (_batch == nullptr) {
        _current_line_of_group += _rows_of_group;
        return read_record_batch(eof);
    }
    uint8_t tmp_buf[128] = {0};
    int32_t wbytes = 0;
    const uint8_t* value = nullptr;
    int column_index = 0;
    try {
        size_t slots = _include_column_ids.size();
        for (size_t i = 0; i < slots; ++i) {
            auto slot_desc = _file_slot_descs[i];
            column_index = i; // column index in batch record
            switch (_parquet_column_type[i]) {
            case arrow::Type::type::STRING: {
                auto str_array =
                        std::static_pointer_cast<arrow::StringArray>(_batch->column(column_index));
                if (str_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    value = str_array->GetValue(_current_line_of_batch, &wbytes);
                    fill_slot(tuple, slot_desc, mem_pool, value, wbytes);
                }
                break;
            }
            case arrow::Type::type::INT32: {
                auto int32_array =
                        std::static_pointer_cast<arrow::Int32Array>(_batch->column(column_index));
                if (int32_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    int32_t value = int32_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%d", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::INT64: {
                auto int64_array =
                        std::static_pointer_cast<arrow::Int64Array>(_batch->column(column_index));
                if (int64_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    int64_t value = int64_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%" PRId64, value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::UINT32: {
                auto uint32_array =
                        std::static_pointer_cast<arrow::UInt32Array>(_batch->column(column_index));
                if (uint32_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    uint32_t value = uint32_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%u", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::UINT64: {
                auto uint64_array =
                        std::static_pointer_cast<arrow::UInt64Array>(_batch->column(column_index));
                if (uint64_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    uint64_t value = uint64_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%" PRIu64, value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::BINARY: {
                auto str_array =
                        std::static_pointer_cast<arrow::BinaryArray>(_batch->column(column_index));
                if (str_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    value = str_array->GetValue(_current_line_of_batch, &wbytes);
                    fill_slot(tuple, slot_desc, mem_pool, value, wbytes);
                }
                break;
            }
            case arrow::Type::type::FIXED_SIZE_BINARY: {
                auto fixed_array = std::static_pointer_cast<arrow::FixedSizeBinaryArray>(
                        _batch->column(column_index));
                if (fixed_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    std::string value = fixed_array->GetString(_current_line_of_batch);
                    fill_slot(tuple, slot_desc, mem_pool, (uint8_t*)value.c_str(), value.length());
                }
                break;
            }
            case arrow::Type::type::BOOL: {
                auto boolean_array =
                        std::static_pointer_cast<arrow::BooleanArray>(_batch->column(column_index));
                if (boolean_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    bool value = boolean_array->Value(_current_line_of_batch);
                    if (value) {
                        fill_slot(tuple, slot_desc, mem_pool, (uint8_t*)"true", 4);
                    } else {
                        fill_slot(tuple, slot_desc, mem_pool, (uint8_t*)"false", 5);
                    }
                }
                break;
            }
            case arrow::Type::type::UINT8: {
                auto uint8_array =
                        std::static_pointer_cast<arrow::UInt8Array>(_batch->column(column_index));
                if (uint8_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    uint8_t value = uint8_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%d", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::INT8: {
                auto int8_array =
                        std::static_pointer_cast<arrow::Int8Array>(_batch->column(column_index));
                if (int8_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    int8_t value = int8_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%d", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::UINT16: {
                auto uint16_array =
                        std::static_pointer_cast<arrow::UInt16Array>(_batch->column(column_index));
                if (uint16_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    uint16_t value = uint16_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%d", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::INT16: {
                auto int16_array =
                        std::static_pointer_cast<arrow::Int16Array>(_batch->column(column_index));
                if (int16_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    int16_t value = int16_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%d", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::HALF_FLOAT: {
                auto half_float_array = std::static_pointer_cast<arrow::HalfFloatArray>(
                        _batch->column(column_index));
                if (half_float_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    float value = half_float_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%f", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::FLOAT: {
                auto float_array =
                        std::static_pointer_cast<arrow::FloatArray>(_batch->column(column_index));
                if (float_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    float value = float_array->Value(_current_line_of_batch);
                    // Because the decimal type currently only supports (27, 9).
                    // Therefore, we use %.9f to give priority to the progress of the decimal type.
                    // Cannot use %f directly, this will cause 4000.9 to be converted to 4000.8999
                    wbytes = sprintf((char*)tmp_buf, "%.9f", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::DOUBLE: {
                auto double_array =
                        std::static_pointer_cast<arrow::DoubleArray>(_batch->column(column_index));
                if (double_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    double value = double_array->Value(_current_line_of_batch);
                    wbytes = sprintf((char*)tmp_buf, "%.9f", value);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::TIMESTAMP: {
                auto ts_array = std::static_pointer_cast<arrow::TimestampArray>(
                        _batch->column(column_index));
                if (ts_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    RETURN_IF_ERROR(handle_timestamp(ts_array, tmp_buf,
                                                     &wbytes)); // convert timestamp to string time
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::DECIMAL: {
                auto decimal_array =
                        std::static_pointer_cast<arrow::DecimalArray>(_batch->column(column_index));
                if (decimal_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    std::string value = decimal_array->FormatValue(_current_line_of_batch);
                    fill_slot(tuple, slot_desc, mem_pool, (const uint8_t*)value.c_str(),
                              value.length());
                }
                break;
            }
            case arrow::Type::type::DATE32: {
                auto ts_array =
                        std::static_pointer_cast<arrow::Date32Array>(_batch->column(column_index));
                if (ts_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    time_t timestamp = (time_t)((int64_t)ts_array->Value(_current_line_of_batch) *
                                                24 * 60 * 60);
                    struct tm local;
                    localtime_r(&timestamp, &local);
                    char* to = reinterpret_cast<char*>(&tmp_buf);
                    wbytes = (uint32_t)strftime(to, 64, "%Y-%m-%d", &local);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            case arrow::Type::type::DATE64: {
                auto ts_array =
                        std::static_pointer_cast<arrow::Date64Array>(_batch->column(column_index));
                if (ts_array->IsNull(_current_line_of_batch)) {
                    RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                } else {
                    // convert milliseconds to seconds
                    time_t timestamp =
                            (time_t)((int64_t)ts_array->Value(_current_line_of_batch) / 1000);
                    struct tm local;
                    localtime_r(&timestamp, &local);
                    char* to = reinterpret_cast<char*>(&tmp_buf);
                    wbytes = (uint32_t)strftime(to, 64, "%Y-%m-%d %H:%M:%S", &local);
                    fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbytes);
                }
                break;
            }
            default: {
                // other type not support.
                std::stringstream str_error;
                str_error << "The field name(" << slot_desc->col_name() << "), type("
                          << _parquet_column_type[i]
                          << ") not support. RowGroup: " << _current_group
                          << ", Row: " << _current_line_of_group
                          << ", ColumnIndex:" << column_index;
                LOG(WARNING) << str_error.str();
                return Status::InternalError(str_error.str());
            }
            }
        }
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << e.what() << " RowGroup:" << _current_group
                  << ", Row:" << _current_line_of_group << ", ColumnIndex " << column_index;
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }

    // update data value
    ++_current_line_of_group;
    ++_current_line_of_batch;
    return read_record_batch(eof);
}

Status ParquetReaderWrap::read_next_batch() {
    std::unique_lock<std::mutex> lock(_mtx);
    while (!_closed && _queue.empty()) {
        if (_batch_eof) {
            return Status::OK();
        }
        _queue_reader_cond.wait_for(lock, std::chrono::seconds(1));
    }

    if (UNLIKELY(_closed)) {
        return Status::InternalError(_status.message());
    }

    _batch = _queue.front();
    _queue.pop_front();
    _queue_writer_cond.notify_one();
    return Status::OK();
}

void ParquetReaderWrap::read_batches(arrow::RecordBatchVector& batches, int current_group) {
    _status = _reader->GetRecordBatchReader({current_group}, _include_column_ids, &_rb_reader);
    if (!_status.ok()) {
        _closed = true;
        return;
    }
    _status = _rb_reader->ReadAll(&batches);
}

bool ParquetReaderWrap::filter_row_group(int current_group) {
    if (_need_filter_row_group) {
        auto filter_group_set = _row_group_reader->filter_groups();
        if (filter_group_set.end() != filter_group_set.find(current_group)) {
            // find filter group, skip
            return true;
        }
    }
    return false;
}

} // namespace doris
