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
#include "exec/parquet_reader.h"
#include <time.h>
#include <arrow/status.h>
#include <arrow/array.h>
#include "exec/file_reader.h"
#include "common/logging.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "util/thrift_util.h"
#include "runtime/tuple.h"
#include "runtime/descriptors.h"
#include "runtime/mem_pool.h"

namespace doris {

// Broker

ParquetReaderWrap::ParquetReaderWrap(FileReader *file_reader) :
           _total_groups(0), _current_group(0), _rows_of_group(0), _current_line_of_group(0) {
    _parquet = std::shared_ptr<ParquetFile>(new ParquetFile(file_reader));
    _properties = parquet::ReaderProperties();
    _properties.enable_buffered_stream();
    _properties.set_buffer_size(65535);
}

ParquetReaderWrap::~ParquetReaderWrap() {
    close();
}

Status ParquetReaderWrap::init_parquet_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs) {
    try {
        // new file reader for parquet file
        _reader.reset(new parquet::arrow::FileReader(arrow::default_memory_pool(),
                std::move(parquet::ParquetFileReader::Open(_parquet, _properties))));

        _file_metadata = _reader->parquet_reader()->metadata();
        // initial members
        _total_groups = _file_metadata->num_row_groups();
        _rows_of_group = _file_metadata->RowGroup(0)->num_rows();

        // map
        auto *schemaDescriptor = _file_metadata->schema();
        for (int i = 0; i < _file_metadata->num_columns(); ++i) {
            // Get the Column Reader for the boolean column
            _map_column.emplace(schemaDescriptor->Column(i)->name(), i);
        }

        if (_current_line_of_group == 0) {// the first read
            RETURN_IF_ERROR(column_indices(tuple_slot_descs));
            // read batch
            arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);
            if (!status.ok()) {
                LOG(WARNING) << "Get RecordBatch Failed. " << status.ToString();
                return Status::InternalError(status.ToString());
            }
            status = _rb_batch->ReadNext(&_batch);
            if (!status.ok()) {
                LOG(WARNING) << "The first read record. " << status.ToString();
                return Status::InternalError(status.ToString());
            }
            //save column type
            std::shared_ptr<arrow::Schema> field_schema = _batch->schema();
            for (int i = 0; i < _parquet_column_ids.size(); i++) {
                std::shared_ptr<arrow::Field>  field = field_schema->field(i);
                if (!field) {
                    LOG(WARNING) << "Get filed schema failed. Column order:" << i;
                    return Status::InternalError(status.ToString());
                }
                _parquet_column_type.emplace_back(field->type()->id());
            }
        }
        return Status::OK();
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << "Init parquet reader fail. " << e.what();
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }
}

void ParquetReaderWrap::close() {
    _parquet->Close();
}

Status ParquetReaderWrap::size(int64_t* size) {
    _parquet->GetSize(size);
    return Status::OK();
}

inline void ParquetReaderWrap::fill_slot(Tuple* tuple, SlotDescriptor* slot_desc, MemPool* mem_pool, const uint8_t* value, int32_t len) {
    tuple->set_not_null(slot_desc->null_indicator_offset());
    void* slot = tuple->get_slot(slot_desc->tuple_offset());
    StringValue* str_slot = reinterpret_cast<StringValue*>(slot);
    str_slot->ptr = reinterpret_cast<char*>(mem_pool->allocate(len));
    memcpy(str_slot->ptr, value, len);
    str_slot->len = len;
    return;
}

Status ParquetReaderWrap::column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs)
{
    _parquet_column_ids.clear();
    for (auto slot_desc : tuple_slot_descs) {
        // Get the Column Reader for the boolean column
        auto iter = _map_column.find(slot_desc->col_name());
        if (iter == _map_column.end()) {
            std::stringstream str_error;
            str_error << "Invalid Column Name:" << slot_desc->col_name();
            LOG(WARNING) << str_error.str();
            return Status::InvalidArgument(str_error.str());
        }
        _parquet_column_ids.emplace_back(iter->second);
    }
    return Status::OK();
}

inline Status ParquetReaderWrap::set_field_null(Tuple* tuple, const SlotDescriptor* slot_desc) {
    if (!slot_desc->is_nullable()) {
        std::stringstream str_error;
        str_error << "The field name("<< slot_desc->col_name() <<") is not allowed null, but Parquet field is NULL.";
        LOG(WARNING) << str_error.str();
        return Status::RuntimeError(str_error.str());
    }
    tuple->set_null(slot_desc->null_indicator_offset());
    return Status::OK();
}

Status ParquetReaderWrap::read_record_batch(const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof) {
    if (_current_line_of_group >= _rows_of_group) {// read next row group
        _current_group++;
        if (_current_group >= _total_groups) {// read completed.
            _parquet_column_ids.clear();
            *eof = true;
            return Status::OK();
        }
        _current_line_of_group = 0;
        _rows_of_group = _file_metadata->RowGroup(_current_group)->num_rows(); //get rows of the current row group
        // read batch
        arrow::Status status = _reader->GetRecordBatchReader({_current_group}, _parquet_column_ids, &_rb_batch);
        if (!status.ok()) {
            return Status::InternalError("Get RecordBatchReader Failed.");
        }
        status = _rb_batch->ReadNext(&_batch);
        if (!status.ok()) {
            return Status::InternalError("Read Batch Error With Libarrow.");
        }
    }
    return Status::OK();
}

Status ParquetReaderWrap::handle_timestamp(const std::shared_ptr<arrow::TimestampArray>& ts_array, uint8_t *buf, int32_t *wbtyes) {
    const auto type = std::dynamic_pointer_cast<arrow::TimestampType>(ts_array->type());
    // Doris only supports seconds
    time_t timestamp = 0;
    switch (type->unit()) {
        case arrow::TimeUnit::type::NANO: {// INT96
            timestamp = (time_t)((int64_t)ts_array->Value(_current_line_of_group) / 1000000000); // convert to Second
            break;
        }
        case arrow::TimeUnit::type::SECOND: {
            timestamp = (time_t)ts_array->Value(_current_line_of_group);
            break;
        }
        case arrow::TimeUnit::type::MILLI: {
            timestamp = (time_t)((int64_t)ts_array->Value(_current_line_of_group) / 1000); // convert to Second
            break;
        }
        case arrow::TimeUnit::type::MICRO: {
            timestamp = (time_t)((int64_t)ts_array->Value(_current_line_of_group) / 1000000); // convert to Second
            break;
        }
        default:
            return Status::InternalError("Invalid Time Type.");
    }
    tm* local;
    local = localtime(&timestamp);
    *wbtyes = (uint32_t)strftime((char*)buf, 64, "%Y-%m-%d %H:%M:%S", local);
    return Status::OK();
}

Status ParquetReaderWrap::read(Tuple* tuple, const std::vector<SlotDescriptor*>& tuple_slot_descs, MemPool* mem_pool, bool* eof) {
    uint8_t tmp_buf[128] = {0};
    int32_t wbtyes = 0;
    const uint8_t *value = nullptr;
    int column_index = 0;
    try {
        size_t slots = tuple_slot_descs.size();
        for (size_t i = 0; i < slots; ++i) {
            auto slot_desc = tuple_slot_descs[i];
            column_index = i;// column index in batch record
            switch (_parquet_column_type[i]) {
                case arrow::Type::type::STRING: {
                    auto str_array = std::dynamic_pointer_cast<arrow::StringArray>(_batch->column(column_index));
                    if (str_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        value = str_array->GetValue(_current_line_of_group, &wbtyes);
                        fill_slot(tuple, slot_desc, mem_pool, value, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT32: {
                    auto int32_array = std::dynamic_pointer_cast<arrow::Int32Array>(_batch->column(column_index));
                    if (int32_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        int32_t value = int32_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT64: {
                    auto int64_array = std::dynamic_pointer_cast<arrow::Int64Array>(_batch->column(column_index));
                    if (int64_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        int64_t value = int64_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%ld", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::UINT32: {
                    auto uint32_array = std::dynamic_pointer_cast<arrow::UInt32Array>(_batch->column(column_index));
                    if (uint32_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        uint32_t value = uint32_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%u", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::UINT64: {
                    auto uint64_array = std::dynamic_pointer_cast<arrow::UInt64Array>(_batch->column(column_index));
                    if (uint64_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        uint64_t value = uint64_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%lu", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::BINARY: {
                    auto str_array = std::dynamic_pointer_cast<arrow::BinaryArray>(_batch->column(column_index));
                    if (str_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                      value = str_array->GetValue(_current_line_of_group, &wbtyes);
                      fill_slot(tuple, slot_desc, mem_pool, value, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::FIXED_SIZE_BINARY: {
                    auto fixed_array = std::dynamic_pointer_cast<arrow::FixedSizeBinaryArray>(_batch->column(column_index));
                    if (fixed_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        string value = fixed_array->GetString(_current_line_of_group);
                        fill_slot(tuple, slot_desc, mem_pool, (uint8_t*)value.c_str(), value.length());
                    }
                    break;
                }
                case arrow::Type::type::BOOL: {
                    auto boolean_array = std::dynamic_pointer_cast<arrow::BooleanArray>(_batch->column(column_index));
                    if (boolean_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        bool value = boolean_array->Value(_current_line_of_group);
                        if (value) {
                            fill_slot(tuple, slot_desc, mem_pool, (uint8_t*)"true", 4);
                        } else {
                            fill_slot(tuple, slot_desc, mem_pool, (uint8_t*)"false", 5);
                        }
                    }
                    break;
                }
                case arrow::Type::type::UINT8: {
                    auto uint8_array = std::dynamic_pointer_cast<arrow::UInt8Array>(_batch->column(column_index));
                    if (uint8_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        uint8_t value = uint8_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT8: {
                    auto int8_array = std::dynamic_pointer_cast<arrow::Int8Array>(_batch->column(column_index));
                    if (int8_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        int8_t value = int8_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::UINT16: {
                    auto uint16_array = std::dynamic_pointer_cast<arrow::UInt16Array>(_batch->column(column_index));
                    if (uint16_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        uint16_t value = uint16_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::INT16: {
                    auto int16_array = std::dynamic_pointer_cast<arrow::Int16Array>(_batch->column(column_index));
                    if (int16_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        int16_t value = int16_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%d", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::HALF_FLOAT: {
                    auto half_float_array = std::dynamic_pointer_cast<arrow::HalfFloatArray>(_batch->column(column_index));
                    if (half_float_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        float value = half_float_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%f", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::FLOAT: {
                    auto float_array = std::dynamic_pointer_cast<arrow::FloatArray>(_batch->column(column_index));
                    if (float_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        float value = float_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%f", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::DOUBLE: {
                    auto double_array = std::dynamic_pointer_cast<arrow::DoubleArray>(_batch->column(column_index));
                    if (double_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        float value = double_array->Value(_current_line_of_group);
                        wbtyes = sprintf((char*)tmp_buf, "%f", value);
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                case arrow::Type::type::TIMESTAMP: {
                    auto ts_array = std::dynamic_pointer_cast<arrow::TimestampArray>(_batch->column(column_index));
                    if (ts_array->IsNull(_current_line_of_group)) {
                        RETURN_IF_ERROR(set_field_null(tuple, slot_desc));
                    } else {
                        RETURN_IF_ERROR(handle_timestamp(ts_array, tmp_buf, &wbtyes));// convert timestamp to string time
                        fill_slot(tuple, slot_desc, mem_pool, tmp_buf, wbtyes);
                    }
                    break;
                }
                default: {
                    // other type not support.
                    std::stringstream str_error;
                    str_error << "The field name("<< slot_desc->col_name() <<"), type("<< _parquet_column_type[i] <<
                            ") not support. RowGroup: " << _current_group
                            << ", Row: " << _current_line_of_group << ", ColumnIndex:" << column_index;
                    LOG(WARNING) << str_error.str();
                    return Status::InternalError(str_error.str());
                }
            }
        }
    } catch (parquet::ParquetException& e) {
        std::stringstream str_error;
        str_error << e.what() << " RowGroup:" << _current_group << ", Row:" << _current_line_of_group
                            << ", ColumnIndex " << column_index;
        LOG(WARNING) << str_error.str();
        return Status::InternalError(str_error.str());
    }

    // update data value
    ++_current_line_of_group;
    return read_record_batch(tuple_slot_descs, eof);
}

ParquetFile::ParquetFile(FileReader *file): _file(file) {

}

ParquetFile::~ParquetFile() {
    Close();
}

arrow::Status ParquetFile::Close() {
    if (_file) {
        _file->close();
        delete _file;
        _file = nullptr;
    }
    return arrow::Status::OK();
}

bool ParquetFile::closed() const {
    if (_file) {
        return _file->closed();
    } else {
        return true;
    }
}

arrow::Status ParquetFile::Read(int64_t nbytes, int64_t* bytes_read, void* buffer) {
    bool eof = false;
    size_t data_size = 0;
    do {
        data_size = nbytes;
        Status result = _file->read((uint8_t*)buffer, &data_size, &eof);
        if (!result.ok()) {
            return arrow::Status::IOError("Read failed.");
        }
        if (eof) {
            break;
        }
        *bytes_read += data_size; // total read bytes
        nbytes -= data_size; // remained bytes
        buffer = (uint8_t*)buffer + data_size;
    } while (nbytes != 0);
    return arrow::Status::OK();
}

arrow::Status ParquetFile::ReadAt(int64_t position, int64_t nbytes, int64_t* bytes_read, void* out) {
    int64_t reads = 0;
    while(nbytes != 0) {
        Status result = _file->readat(position, nbytes, &reads, out);
        if (!result.ok()) {
            *bytes_read = 0;
            return arrow::Status::IOError("Readat failed.");
        }
        if (reads == 0) {
            break;
        }
        *bytes_read += reads;// total read bytes
        nbytes -= reads; // remained bytes
        position += reads;
        out = (char*)out + reads;
    }
    return arrow::Status::OK();
}

arrow::Status ParquetFile::GetSize(int64_t* size) {
    *size = _file->size();
    return arrow::Status::OK();
}

arrow::Status ParquetFile::Seek(int64_t position) {
    _file->seek(position);
    return arrow::Status::OK();
}


arrow::Status ParquetFile::Tell(int64_t* position) const {
    _file->tell(position);
    return arrow::Status::OK();
}

arrow::Status ParquetFile::Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) {
    return arrow::Status::NotImplemented("Not Supported.");
}

}
