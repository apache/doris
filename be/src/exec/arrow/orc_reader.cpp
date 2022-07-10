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
                             int32_t num_of_columns_from_file)
        : ArrowReaderWrap(file_reader, batch_size, num_of_columns_from_file) {
    _reader = nullptr;
    _cur_file_eof = false;
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

    // map
    arrow::Result<std::shared_ptr<arrow::Schema>> maybe_schema = _reader->ReadSchema();
    if (!maybe_schema.ok()) {
        // Handle error instantiating file reader...
        LOG(WARNING) << "failed to read schema, errmsg=" << maybe_schema.status();
        return Status::InternalError("Failed to create orc file reader");
    }
    std::shared_ptr<arrow::Schema> schema = maybe_schema.ValueOrDie();
    for (size_t i = 0; i < schema->num_fields(); ++i) {
        _map_column.emplace(schema->field(i)->name(), i);
    }

    bool eof = false;
    RETURN_IF_ERROR(_next_stripe_reader(&eof));
    if (eof) {
        return Status::EndOfFile("end of file");
    }

    RETURN_IF_ERROR(column_indices(tuple_slot_descs));
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
            _reader->NextStripeReader(_batch_size, _include_column_ids);
    if (!maybe_rb_reader.ok()) {
        LOG(WARNING) << "Get RecordBatch Failed. " << maybe_rb_reader.status();
        return Status::InternalError(maybe_rb_reader.status().ToString());
    }
    _rb_reader = maybe_rb_reader.ValueOrDie();
    _current_group++;
    return Status::OK();
}

Status ORCReaderWrap::next_batch(std::shared_ptr<arrow::RecordBatch>* batch, bool* eof) {
    *eof = false;
    do {
        auto st = _rb_reader->ReadNext(batch);
        if (!st.ok()) {
            LOG(WARNING) << "failed to get next batch, errmsg=" << st;
            return Status::InternalError(st.ToString());
        }
        if (*batch == nullptr) {
            // try next stripe
            RETURN_IF_ERROR(_next_stripe_reader(eof));
            if (*eof) {
                break;
            }
        }
    } while (*batch == nullptr);
    return Status::OK();
}

} // namespace doris