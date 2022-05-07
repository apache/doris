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

#include <arrow/api.h>
#include <arrow/buffer.h>
#include <arrow/io/api.h>
#include <arrow/io/file.h>
#include <arrow/io/interfaces.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"
#include "exec/parquet_reader.h"
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"

namespace doris {

// Reader of broker parquet file
class VParquetReaderWrap {
public:
    VParquetReaderWrap(ParquetReaderWrap* reader, std::vector<SlotDescriptor*>& src_slot_descs,
                       std::string time_zone);
    virtual ~VParquetReaderWrap();

    // Read
    Status next_batch(std::shared_ptr<arrow::RecordBatch>* batch);

private:
    std::shared_ptr<ParquetReaderWrap> _reader;
    const std::vector<SlotDescriptor*>& _src_slot_descs;
    bool _inited;
    std::string _timezone;
};

} // namespace doris
