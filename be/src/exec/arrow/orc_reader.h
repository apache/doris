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
namespace doris {

// Reader of ORC file
class ORCReaderWrap final : public ArrowReaderWrap {
public:
    ORCReaderWrap(FileReader* file_reader, int64_t batch_size, int32_t num_of_columns_from_file,
                  int64_t range_start_offset, int64_t range_size, bool case_sensitive = true);
    ~ORCReaderWrap() override = default;

    Status init_reader(const TupleDescriptor* tuple_desc,
                       const std::vector<SlotDescriptor*>& tuple_slot_descs,
                       const std::vector<ExprContext*>& conjunct_ctxs,
                       const std::string& timezone) override;

private:
    Status _next_stripe_reader(bool* eof);
    Status _seek_start_stripe();
    void read_batches(arrow::RecordBatchVector& batches, int current_group) override;
    bool filter_row_group(int current_group) override;

private:
    // orc file reader object
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> _reader;
    bool _cur_file_eof; // is read over?
    int64_t _range_start_offset;
    int64_t _range_size;
};

} // namespace doris
