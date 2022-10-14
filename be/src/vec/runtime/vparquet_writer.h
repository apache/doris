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
#include <gen_cpp/DataSinks_types.h>
#include <parquet/api/reader.h>
#include <parquet/api/writer.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <stdint.h>

#include <map>
#include <string>

#include "common/status.h"
#include "exec/parquet_writer.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vfile_result_writer.h"

namespace doris::vectorized {

class VFileWriterWrapper {
public:
    VFileWriterWrapper(const std::vector<VExprContext*>& output_vexpr_ctxs, bool output_object_data)
            : _output_vexpr_ctxs(output_vexpr_ctxs),
              _cur_written_rows(0),
              _output_object_data(output_object_data) {}

    virtual ~VFileWriterWrapper() = default;

    virtual Status prepare() = 0;

    virtual Status write(const Block& block) = 0;

    virtual void close() = 0;

    virtual int64_t written_len() = 0;

protected:
    const std::vector<VExprContext*>& _output_vexpr_ctxs;
    int64_t _cur_written_rows;
    bool _output_object_data;
};

// a wrapper of parquet output stream
class VParquetWriterWrapper final : public VFileWriterWrapper {
public:
    VParquetWriterWrapper(doris::FileWriter* file_writer,
                          const std::vector<VExprContext*>& output_vexpr_ctxs,
                          const std::vector<TParquetSchema>& parquet_schemas,
                          const TParquetCompressionType::type& compression_type,
                          const bool& parquet_disable_dictionary,
                          const TParquetVersion::type& parquet_version, bool output_object_data);

    ~VParquetWriterWrapper() = default;

    Status prepare() override;

    Status write(const Block& block) override;

    void close() override;

    int64_t written_len() override;

private:
    parquet::RowGroupWriter* get_rg_writer();

    void parse_schema(const std::vector<TParquetSchema>& parquet_schemas);

    void parse_properties(const TParquetCompressionType::type& compression_type,
                          const bool& parquet_disable_dictionary,
                          const TParquetVersion::type& parquet_version);

private:
    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<parquet::WriterProperties> _properties;
    std::shared_ptr<parquet::schema::GroupNode> _schema;
    std::unique_ptr<parquet::ParquetFileWriter> _writer;
    parquet::RowGroupWriter* _rg_writer;
    const int64_t _max_row_per_group = 10;
};

} // namespace doris::vectorized
