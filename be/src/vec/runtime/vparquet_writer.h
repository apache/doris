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

#include <arrow/io/interfaces.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/types.h>
#include <stdint.h>

#include <memory>
#include <vector>

#include "common/status.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"

namespace doris {
namespace io {
class FileWriter;
} // namespace io
} // namespace doris
namespace parquet {
namespace schema {
class GroupNode;
} // namespace schema
} // namespace parquet

namespace doris::vectorized {

class ParquetOutputStream : public arrow::io::OutputStream {
public:
    ParquetOutputStream(doris::io::FileWriter* file_writer);
    ParquetOutputStream(doris::io::FileWriter* file_writer, const int64_t& written_len);
    ~ParquetOutputStream() override;

    arrow::Status Write(const void* data, int64_t nbytes) override;
    // return the current write position of the stream
    arrow::Result<int64_t> Tell() const override;
    arrow::Status Close() override;

    bool closed() const override { return _is_closed; }

    int64_t get_written_len() const;

    void set_written_len(int64_t written_len);

private:
    doris::io::FileWriter* _file_writer; // not owned
    int64_t _cur_pos = 0;                // current write position
    bool _is_closed = false;
    int64_t _written_len = 0;
};

class ParquetBuildHelper {
public:
    static void build_schema_repetition_type(
            parquet::Repetition::type& parquet_repetition_type,
            const TParquetRepetitionType::type& column_repetition_type);

    static void build_schema_data_type(parquet::Type::type& parquet_data_type,
                                       const TParquetDataType::type& column_data_type);

    static void build_compression_type(parquet::WriterProperties::Builder& builder,
                                       const TParquetCompressionType::type& compression_type);

    static void build_version(parquet::WriterProperties::Builder& builder,
                              const TParquetVersion::type& parquet_version);
    static void build_schema_data_logical_type(
            std::shared_ptr<const parquet::LogicalType>& parquet_data_logical_type_ptr,
            const TParquetDataLogicalType::type& column_data_logical_type, int* primitive_length,
            const TypeDescriptor& type_desc);
};

class VFileWriterWrapper {
public:
    VFileWriterWrapper(const VExprContextSPtrs& output_vexpr_ctxs, bool output_object_data)
            : _output_vexpr_ctxs(output_vexpr_ctxs),
              _cur_written_rows(0),
              _output_object_data(output_object_data) {}

    virtual ~VFileWriterWrapper() = default;

    virtual Status prepare() = 0;

    virtual Status write(const Block& block) = 0;

    virtual void close() = 0;

    virtual int64_t written_len() = 0;

protected:
    const VExprContextSPtrs& _output_vexpr_ctxs;
    int64_t _cur_written_rows;
    bool _output_object_data;
};

// a wrapper of parquet output stream
class VParquetWriterWrapper final : public VFileWriterWrapper {
public:
    VParquetWriterWrapper(doris::io::FileWriter* file_writer,
                          const VExprContextSPtrs& output_vexpr_ctxs,
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

    Status parse_schema();

    Status parse_properties();

private:
    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<parquet::WriterProperties> _properties;
    std::shared_ptr<parquet::schema::GroupNode> _schema;
    std::unique_ptr<parquet::ParquetFileWriter> _writer;
    parquet::RowGroupWriter* _rg_writer;
    const int64_t _max_row_per_group = 10;

    const std::vector<TParquetSchema>& _parquet_schemas;
    const TParquetCompressionType::type& _compression_type;
    const bool& _parquet_disable_dictionary;
    const TParquetVersion::type& _parquet_version;
};

} // namespace doris::vectorized
