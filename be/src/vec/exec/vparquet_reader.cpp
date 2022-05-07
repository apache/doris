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

#include <arrow/array.h>
#include <arrow/status.h>
#include <time.h>

#include "common/logging.h"
#include "exec/file_reader.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/TPaloBrokerService.h"
#include "runtime/broker_mgr.h"
#include "runtime/client_cache.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/mem_pool.h"
#include "runtime/tuple.h"
#include "util/thrift_util.h"

namespace doris {
namespace vectorized {

// Broker
VParquetReaderWrap::VParquetReaderWrap(ParquetReaderWrap* parquet_reader,
                                       const std::vector<SlotDescriptor*>& src_slot_desc, std::string time_zone)
        : _src_slot_descs(src_slot_desc),
          _time_zone(std::move(time_zone)),
          _inited(false) {
    _reader = std::shared_ptr<ParquetReaderWrap>(parquet_reader);
}

VParquetReaderWrap::~VParquetReaderWrap() {
    _reader->close();
}

Status VParquetReaderWrap::next_batch(std::shared_ptr<arrow::RecordBatch>* batch) {
    if (!inited) {
        RETURN_IF_ERROR(_parquet_reader->init_parquet_reader(_src_slot_descs, _time_zone));
        _inited = true;
    } else {
        bool eof = false;
        auto status = _parquet_reader->read_record_batch(_src_slot_descs, &eof);
        if (status.is_end_of_file() || eof) {
            *batch = nullptr;
            return Status::EndOfFile("End Of Parquet File");
        } else if (!status.ok()) {
            return status;
        }
    }
    *batch = _parquet_reader->get_batch();
    return Status::OK();
}

} // namespace vectorized
} // namespace doris
