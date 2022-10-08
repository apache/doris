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

#include <stdint.h>

#include <map>
#include <orc/OrcFile.hh>
#include <string>

#include "common/status.h"
#include "io/file_writer.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vfile_result_writer.h"

namespace doris::vectorized {
class FileWriter;

class VOrcOutputStream : public orc::OutputStream {
public:
    VOrcOutputStream(doris::FileWriter* file_writer);

    ~VOrcOutputStream() override;

    uint64_t getLength() const override { return _written_len; }

    uint64_t getNaturalWriteSize() const override { return 128 * 1024; }

    void write(const void* buf, size_t length) override;

    const std::string& getName() const override { return _name; }

    void close() override;

    void set_written_len(int64_t written_len);

private:
    doris::FileWriter* _file_writer; // not owned
    int64_t _cur_pos = 0;            // current write position
    bool _is_closed = false;
    int64_t _written_len = 0;
    const std::string _name;
};

// a wrapper of parquet output stream
class VOrcWriterWrapper final : public VFileWriterWrapper {
public:
    VOrcWriterWrapper(doris::FileWriter* file_writer,
                      const std::vector<VExprContext*>& output_vexpr_ctxs,
                      const std::string& schema, bool output_object_data);

    ~VOrcWriterWrapper() = default;

    Status prepare() override;

    Status write(const Block& block) override;

    void close() override;

    int64_t written_len() override;

private:
    std::unique_ptr<orc::ColumnVectorBatch> _create_row_batch(size_t sz);

    doris::FileWriter* _file_writer;
    std::unique_ptr<orc::OutputStream> _output_stream;
    std::unique_ptr<orc::WriterOptions> _write_options;
    const std::string& _schema_str;
    std::unique_ptr<orc::Type> _schema;
    std::unique_ptr<orc::Writer> _writer;

    static constexpr size_t BUFFER_UNIT_SIZE = 4096;
};

} // namespace doris::vectorized
