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

#pragma once

#include <arrow/adapters/orc/adapter.h>
#include <arrow/api.h>
#include <arrow/buffer.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"
#include "exec/arrow/arrow_reader.h"
#include "exec/arrow/orc_stripe_reader.h"

namespace doris {
class StripeReader;

// Reader of ORC file
class ORCReaderWrap final : public ArrowReaderWrap {
public:
    ORCReaderWrap(FileReader* file_reader, int64_t batch_size, int32_t num_of_columns_from_file,
                  int64_t range_start_offset, int64_t range_size);
    ~ORCReaderWrap() override;

    Status init_reader(const TupleDescriptor* tuple_desc,
                       const std::vector<SlotDescriptor*>& tuple_slot_descs,
                       const std::vector<ExprContext*>& conjunct_ctxs,
                       const std::string& timezone) override;
    Status next_batch(std::shared_ptr<arrow::RecordBatch>* batch, bool* eof) override;

    std::shared_ptr<arrow::adapters::orc::ORCFileReader> getReader() { return _reader; }

private:
    Status _next_stripe_reader(bool* eof);
    void prefetch_batch();

private:
    // orc file reader object
    std::shared_ptr<arrow::adapters::orc::ORCFileReader> _reader;

    bool _cur_file_eof; // is read over?
    std::unique_ptr<doris::StripeReader> _strip_reader;
    int64_t _range_start_offset;
    int64_t _range_size;

    std::thread _thread;
    std::shared_ptr<arrow::RecordBatch> _batch;
    std::atomic<bool> _closed = false;
    std::atomic<bool> _batch_eof = false;
    arrow::Status _status;
    std::mutex _mtx;
    std::condition_variable _queue_reader_cond;
    std::condition_variable _queue_writer_cond;
    std::list<std::shared_ptr<arrow::RecordBatch>> _queue;
    const size_t _max_queue_size = config::parquet_reader_max_buffer_size;
};

} // namespace doris
