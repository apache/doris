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

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/internal_service.pb.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/format/generic_reader.h"
#include "arrow/buffer.h"
#include "arrow/io/interfaces.h"
#include "arrow/status.h"

namespace doris {

namespace io {
class FileSystem;
struct IOContext;
} // namespace io

namespace vectorized {

struct ScannerCounter;
class Block;

class BatchWithLengthReader {
    ENABLE_FACTORY_CREATOR(BatchWithLengthReader);

public:
    BatchWithLengthReader(io::FileReaderSPtr file_reader);

    ~BatchWithLengthReader();

    Status get_one_batch(uint8_t** data, int* length);

private:
    Status _get_batch_size();
    static uint32_t _convert_to_length(const uint8_t* data);
    Status _get_batch_value();
    void _ensure_cap(int req_size);
    Status _get_data_from_reader(int req_size);

    io::FileReaderSPtr _file_reader;
    uint8_t* _read_buf;
    int _read_buf_cap;
    int _read_buf_pos;
    int _read_buf_len;
    int _batch_size;
};

class ARROW_EXPORT PipStream : public arrow::io::InputStream {
ENABLE_FACTORY_CREATOR(PipStream);

 public:
  PipStream(io::FileReaderSPtr file_reader);
  ~PipStream() override {}

  arrow::Status Close() override;
  bool closed() const override;

  arrow::Result<std::string_view> Peek(int64_t nbytes) override;

  arrow::Result<int64_t> Tell() const override;

  arrow::Result<int64_t> Read(int64_t nbytes, void* out) override;

  arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t nbytes) override;

  Status HasNext(bool *get);

 private:
  io::FileReaderSPtr _file_reader;
  int64_t pos_;
  bool _begin;
  uint8_t* _read_buf;
};

} // namespace vectorized
} // namespace doris
