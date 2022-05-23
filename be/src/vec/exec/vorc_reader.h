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
#include "exec/arrow_reader.h"
namespace doris::vectorized {

// Reader of orc file
class VORCReaderWrap : public ArrowReaderWrap {
public:
    VORCReaderWrap(FileReader* file_reader, int64_t batch_size, int32_t num_of_columns_from_file);
    virtual ~VORCReaderWrap();

    Status init_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                       const std::string& timezone) override;
    Status next_batch(std::shared_ptr<arrow::RecordBatch>* batch,
                      const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof) override;

private:
    Status _column_indices(const std::vector<SlotDescriptor*>& tuple_slot_descs);
    Status _next_stripe_reader(bool* eof);

private:
    // orc file reader object
    std::shared_ptr<::arrow::RecordBatchReader> _rb_reader;
    std::unique_ptr<arrow::adapters::orc::ORCFileReader> _reader;
    std::map<std::string, int> _map_column; // column-name <---> column-index
    std::vector<int> _orc_column_ids;
    bool _cur_file_eof; // is read over?
    int64_t _num_of_stripes;
    int64_t _current_stripe;
};

} // namespace doris::vectorized