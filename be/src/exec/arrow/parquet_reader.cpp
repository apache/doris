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

#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <opentelemetry/common/threadlocal.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/metadata.h>
#include <parquet/properties.h>
#include <parquet/schema.h>

#include <algorithm>
#include <atomic>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <condition_variable>
#include <list>
#include <map>
#include <mutex>
#include <ostream>
#include <thread>

#include "common/logging.h"
#include "common/status.h"
#include "util/string_util.h"

namespace doris {
class TupleDescriptor;

// Broker
ParquetReaderWrap::ParquetReaderWrap(RuntimeState* state,
                                     const std::vector<SlotDescriptor*>& file_slot_descs,
                                     io::FileReaderSPtr file_reader,
                                     int32_t num_of_columns_from_file, int64_t range_start_offset,
                                     int64_t range_size, bool case_sensitive)
        : ArrowReaderWrap(state, file_slot_descs, file_reader, num_of_columns_from_file,
                          case_sensitive),
          _rows_of_group(0),
          _current_line_of_group(0),
          _current_line_of_batch(0) {}

Status ParquetReaderWrap::init_reader(const TupleDescriptor* tuple_desc,
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
    return false;
}

} // namespace doris
