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
#include "runtime/runtime_state.h"
#include "runtime/tuple.h"
#include "util/string_util.h"
#include "vec/utils/arrow_column_to_doris_column.h"

namespace doris {

ORCReaderWrap::ORCReaderWrap(RuntimeState* state,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             FileReader* file_reader, int32_t num_of_columns_from_file,
                             int64_t range_start_offset, int64_t range_size, bool case_sensitive)
        : ArrowReaderWrap(state, file_slot_descs, file_reader, num_of_columns_from_file,
                          case_sensitive),
          _range_start_offset(range_start_offset),
          _range_size(range_size) {
    _reader = nullptr;
    _cur_file_eof = false;
}

Status ORCReaderWrap::init_reader(const TupleDescriptor* tuple_desc,
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
    // seek file position after _reader created.
    RETURN_IF_ERROR(_seek_start_stripe());

    // map
    arrow::Result<std::shared_ptr<arrow::Schema>> maybe_schema = _reader->ReadSchema();
    if (!maybe_schema.ok()) {
        // Handle error instantiating file reader...
        LOG(WARNING) << "failed to read schema, errmsg=" << maybe_schema.status();
        return Status::InternalError("Failed to create orc file reader");
    }
    _schema = maybe_schema.ValueOrDie();
    for (size_t i = 0; i < _schema->num_fields(); ++i) {
        std::string schemaName =
                _case_sensitive ? _schema->field(i)->name() : to_lower(_schema->field(i)->name());
        // orc index started from 1.
        _map_column.emplace(schemaName, i + 1);
    }
    RETURN_IF_ERROR(column_indices());

    _thread = std::thread(&ArrowReaderWrap::prefetch_batch, this);

    return Status::OK();
}

Status ORCReaderWrap::get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (size_t i = 0; i < _schema->num_fields(); ++i) {
        std::string schema_name =
                _case_sensitive ? _schema->field(i)->name() : to_lower(_schema->field(i)->name());
        TypeDescriptor type;
        RETURN_IF_ERROR(
                vectorized::arrow_type_to_doris_type(_schema->field(i)->type()->id(), &type));
        name_to_type->emplace(schema_name, type);
    }

    for (auto& col : _missing_cols) {
        missing_cols->insert(col);
    }
    return Status::OK();
}

Status ORCReaderWrap::_seek_start_stripe() {
    // If file was from Hms table, _range_start_offset is started from 3(magic word).
    // And if file was from load, _range_start_offset is always set to zero.
    // So now we only support file split for hms table.
    // TODO: support file split for loading.
    if (_range_size <= 0 || _range_start_offset == 0) {
        return Status::OK();
    }
    int64_t row_number = 0;
    int start_group = _current_group;
    int end_group = _total_groups;
    for (int i = 0; i < _total_groups; i++) {
        int64_t _offset = _reader->GetRawORCReader()->getStripe(i)->getOffset();
        int64_t row = _reader->GetRawORCReader()->getStripe(i)->getNumberOfRows();
        if (_offset < _range_start_offset) {
            row_number += row;
        } else if (_offset == _range_start_offset) {
            // If using the external file scan, _range_start_offset is always in the offset lists.
            // If using broker load, _range_start_offset is always set to be 0.
            start_group = i;
        }
        if (_range_start_offset + _range_size <= _offset) {
            end_group = i;
            break;
        }
    }

    LOG(INFO) << "This reader read orc file from offset: " << _range_start_offset
              << " with size: " << _range_size << ". Also mean that read from strip id from "
              << start_group << " to " << end_group;

    if (!_reader->Seek(row_number).ok()) {
        LOG(WARNING) << "Failed to seek to the line number: " << row_number;
        return Status::InternalError("Failed to seek to the line number");
    }

    _current_group = start_group;
    _total_groups = end_group;

    return Status::OK();
}

Status ORCReaderWrap::_next_stripe_reader(bool* eof) {
    if (_current_group >= _total_groups) {
        *eof = true;
        return Status::OK();
    }
    // Get a stripe level record batch iterator.
    // record batch will have up to batch_size rows.
    // NextStripeReader serves as a fine grained alternative to ReadStripe
    // which may cause OOM issues by loading the whole stripe into memory.
    // Note this will only read rows for the current stripe, not the entire file.
    arrow::Result<std::shared_ptr<arrow::RecordBatchReader>> maybe_rb_reader =
            _reader->NextStripeReader(_state->batch_size(), _include_column_ids);
    if (!maybe_rb_reader.ok()) {
        LOG(WARNING) << "Get RecordBatch Failed. " << maybe_rb_reader.status();
        return Status::InternalError(maybe_rb_reader.status().ToString());
    }
    _rb_reader = maybe_rb_reader.ValueOrDie();
    _current_group++;
    return Status::OK();
}

void ORCReaderWrap::read_batches(arrow::RecordBatchVector& batches, int current_group) {
    bool eof = false;
    Status status = _next_stripe_reader(&eof);
    if (!status.ok()) {
        _closed = true;
        return;
    }
    if (eof) {
        _closed = true;
        return;
    }

    _status = _rb_reader->ReadAll(&batches);
}

bool ORCReaderWrap::filter_row_group(int current_group) {
    return false;
}

} // namespace doris
