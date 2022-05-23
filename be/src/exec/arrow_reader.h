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
#include "gen_cpp/PaloBrokerService_types.h"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"

namespace doris {

class ExecEnv;
class TBrokerRangeDesc;
class TNetworkAddress;
class RuntimeState;
class Tuple;
class SlotDescriptor;
class MemPool;
class FileReader;

class ArrowFile : public arrow::io::RandomAccessFile {
public:
    ArrowFile(FileReader* file);
    virtual ~ArrowFile();
    arrow::Result<int64_t> Read(int64_t nbytes, void* buffer) override;
    arrow::Result<int64_t> ReadAt(int64_t position, int64_t nbytes, void* out) override;
    arrow::Result<int64_t> GetSize() override;
    arrow::Status Seek(int64_t position) override;
    arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;
    bool closed() const override;

private:
    FileReader* _file;
    int64_t _pos = 0;
};

// base of arrow reader
class ArrowReaderWrap {
public:
    ArrowReaderWrap(FileReader* file_reader, int64_t batch_size, int32_t num_of_columns_from_file);
    virtual ~ArrowReaderWrap();

    virtual Status init_reader(const std::vector<SlotDescriptor*>& tuple_slot_descs,
                               const std::string& timezone) = 0;
    // for row
    virtual Status read(Tuple* tuple, const std::vector<SlotDescriptor*>& tuple_slot_descs,
                        MemPool* mem_pool, bool* eof) {
        return Status::NotSupported("Not Implemented read");
    }
    // for vec
    virtual Status next_batch(std::shared_ptr<arrow::RecordBatch>* batch,
                              const std::vector<SlotDescriptor*>& tuple_slot_descs, bool* eof) = 0;
    virtual void close();
    virtual Status size(int64_t* size) { return Status::NotSupported("Not Implemented size"); }

protected:
    const int64_t _batch_size;
    const int32_t _num_of_columns_from_file;
    std::shared_ptr<ArrowFile> _arrow_file;
};

} // namespace doris
