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
#include "exec/arrow/orc_reader.h"

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "common/logging.h"
#include "io/file_reader.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"

namespace doris {

ORCReaderWrap::ORCReaderWrap(FileReader* file_reader, int64_t batch_size,
                             int32_t num_of_columns_from_file, int64_t range_start_offset,
                             int64_t range_size)
        : ArrowReaderWrap(file_reader, batch_size, num_of_columns_from_file),
          _range_start_offset(range_start_offset),
          _range_size(range_size) {
    _reader = nullptr;
    _cur_file_eof = false;
}

ORCReaderWrap::~ORCReaderWrap() {
    _closed = true;
    _queue_writer_cond.notify_one();
    if (_thread.joinable()) {
        _thread.join();
    }
}

Status ORCReaderWrap::init_reader(const TupleDescriptor* tuple_desc,
                                  const std::vector<SlotDescriptor*>& tuple_slot_descs,
                                  const std::vector<ExprContext*>& conjunct_ctxs,
                                  const std::string& timezone) {
    // Open ORC file reader
    auto maybe_reader =
            arrow::adapters::orc::ORCFileReader::Open(_arrow_file, arrow::default_memory_pool());
    if (!maybe_reader.ok()) {
        // Handle error instantiating file reader...
        LOG(WARNING) << "failed to create orc file reader, errmsg=" << maybe_reader.status();
        return Status::InternalError("Failed to create orc file reader");
    }
    _reader = std::move(maybe_reader.ValueOrDie());
    _total_groups = _reader->NumberOfStripes();
    if (_total_groups == 0) {
        return Status::EndOfFile("Empty Orc File");
    }

    int64_t row_number = 0;
    int end_group = _total_groups;
    for (int i = 0; i < _total_groups; i++) {
        int64_t _offset = _reader->GetRawORCReader()->getStripe(i)->getOffset();
        int64_t row = _reader->GetRawORCReader()->getStripe(i)->getNumberOfRows();
        if (_offset < _range_start_offset) {
            row_number += row;
        } else if (_offset == _range_start_offset) {
            _current_group = i;
        }
        if (_range_start_offset + _range_size <= _offset) {
            end_group = i;
            break;
        }
    }
    LOG(INFO) << "This reader read orc file from offset: " << _range_start_offset
              << " with size: " << _range_size << ". Also mean that read from strip id from "
              << _current_group << " to " << end_group;
    _total_groups = end_group;

    if (!_reader->Seek(row_number).ok()) {
        LOG(WARNING) << "Failed to seek to the line number: " << row_number;
        return Status::InternalError("Failed to seek to the line number");
    }

    // map
    arrow::Result<std::shared_ptr<arrow::Schema>> maybe_schema = _reader->ReadSchema();
    if (!maybe_schema.ok()) {
        // Handle error instantiating file reader...
        LOG(WARNING) << "failed to read schema, errmsg=" << maybe_schema.status();
        return Status::InternalError("Failed to create orc file reader");
    }
    std::shared_ptr<arrow::Schema> schema = maybe_schema.ValueOrDie();
    for (size_t i = 0; i < schema->num_fields(); ++i) {
        // orc index started from 1.
        _map_column.emplace(schema->field(i)->name(), i + 1);
    }

    RETURN_IF_ERROR(column_indices(tuple_slot_descs));
    if (config::orc_predicate_push_down) {
        _strip_reader.reset(new StripeReader(conjunct_ctxs, this));
        _strip_reader->init_filter_groups(tuple_desc, _map_column, _include_column_ids,
                                          _current_group, _total_groups);
    }

    _thread = std::thread(&ORCReaderWrap::prefetch_batch, this);

    return Status::OK();
}

Status ORCReaderWrap::_next_stripe_reader(bool* eof) {
    bool skip_current_group = false;
    do {
        if (_current_group >= _total_groups) {
            *eof = true;
            return Status::OK();
        }
        if (config::orc_predicate_push_down) {
            auto filter_group_set = _strip_reader->filter_groups();
            if (filter_group_set.end() != filter_group_set.find(_current_group)) {
                // find filter group, skip
                skip_current_group = true;
            } else {
                skip_current_group = false;
            }
        }

        // Get a stripe level record batch iterator.
        // record batch will have up to batch_size rows.
        // NextStripeReader serves as a fine grained alternative to ReadStripe
        // which may cause OOM issues by loading the whole stripe into memory.
        // Note this will only read rows for the current stripe, not the entire file.
        arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> maybe_rb_reader =
                _reader->NextStripeReader(_batch_size, _include_column_ids);
        if (!maybe_rb_reader.ok()) {
            LOG(WARNING) << "Get RecordBatch Failed. " << maybe_rb_reader.status();
            return Status::InternalError(maybe_rb_reader.status().ToString());
        }
        _rb_reader = maybe_rb_reader.ValueOrDie();
        _current_group++;
    } while (skip_current_group);

    return Status::OK();
}

Status ORCReaderWrap::next_batch(std::shared_ptr<arrow::RecordBatch>* batch, bool* eof) {
    std::unique_lock<std::mutex> lock(_mtx);
    while (!_closed && _queue.empty()) {
        if (_batch_eof) {
            _include_column_ids.clear();
            *eof = true;
            _batch_eof = false;
            return Status::OK();
        }
        _queue_reader_cond.wait_for(lock, std::chrono::seconds(1));
    }
    if (UNLIKELY(_closed)) {
        return Status::InternalError(_status.message());
    }
    *batch = _queue.front();
    _queue.pop_front();
    _queue_writer_cond.notify_one();
    return Status::OK();
}

void ORCReaderWrap::prefetch_batch() {
    auto insert_batch = [this](const auto& batch) {
        std::unique_lock<std::mutex> lock(_mtx);
        while (!_closed && _queue.size() == _max_queue_size) {
            _queue_writer_cond.wait_for(lock, std::chrono::seconds(1));
        }
        if (UNLIKELY(_closed)) {
            return;
        }
        _queue.push_back(batch);
        _queue_reader_cond.notify_one();
    };
    int current_group = _current_group;
    int total_groups = _total_groups;
    while (true) {
        if (_closed || current_group >= total_groups) {
            _batch_eof = true;
            _queue_reader_cond.notify_one();

            return;
        }
        if (config::orc_predicate_push_down) {
            auto filter_group_set = _strip_reader->filter_groups();
            if (filter_group_set.end() != filter_group_set.find(current_group)) {
                // find filter group, skip
                current_group++;
                continue;
            }
        }

        arrow::Result<std::shared_ptr<arrow::RecordBatch>> maybe_batch =
                _reader->ReadStripe(current_group, _include_column_ids);

        insert_batch(maybe_batch.ValueOrDie());
        current_group++;
    }
}

} // namespace doris
