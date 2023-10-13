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
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/types.h>
#include <stdint.h>

#include "vfile_format_transformer.h"

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

// a wrapper of parquet output stream
class VParquetTransformer final : public VFileFormatTransformer {
public:
    VParquetTransformer(doris::io::FileWriter* file_writer,
                        const VExprContextSPtrs& output_vexpr_ctxs,
                        const std::vector<TParquetSchema>& parquet_schemas,
                        const TParquetCompressionType::type& compression_type,
                        const bool& parquet_disable_dictionary,
                        const TParquetVersion::type& parquet_version, bool output_object_data);

    ~VParquetTransformer() override = default;

    Status open() override;

    Status write(const Block& block) override;

    Status close() override;

    int64_t written_len() override;

private:
    Status _parse_properties();
    Status _parse_schema();
    arrow::Status _open_file_writer();

    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<parquet::WriterProperties> _parquet_writer_properties;
    std::shared_ptr<parquet::ArrowWriterProperties> _arrow_properties;
    std::unique_ptr<parquet::arrow::FileWriter> _writer;
    std::shared_ptr<arrow::Schema> _arrow_schema;

    const std::vector<TParquetSchema>& _parquet_schemas;
    const TParquetCompressionType::type& _compression_type;
    const bool& _parquet_disable_dictionary;
    const TParquetVersion::type& _parquet_version;
};

} // namespace doris::vectorized
