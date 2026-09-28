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
#include <cctz/time_zone.h>
#include <gen_cpp/DataSinks_types.h>
#include <parquet/arrow/writer.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/types.h>

#include <cstdint>
#include <optional>

#include "format/arrow/arrow_block_convertor.h"
#include "format/transformer/vfile_format_transformer.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace io {
class FileWriter;
} // namespace io
} // namespace doris
namespace parquet {
namespace schema {
class GroupNode;
} // namespace schema
} // namespace parquet

namespace doris {

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
    doris::io::FileWriter* _file_writer = nullptr; // not owned
    int64_t _cur_pos = 0;                          // current write position
    bool _is_closed = false;
    int64_t _written_len = 0;
};

class ParquetBuildHelper {
public:
    static void build_compression_type(::parquet::WriterProperties::Builder& builder,
                                       const TParquetCompressionType::type& compression_type);

    static void build_version(::parquet::WriterProperties::Builder& builder,
                              const TParquetVersion::type& parquet_version);
};

struct ParquetFileOptions {
    TParquetCompressionType::type compression_type;
    TParquetVersion::type parquet_version;
    bool parquet_disable_dictionary = false;
    bool enable_int96_timestamps = false;
    // Overrides only INT96 normalization; UTC preserves a wall-clock carrier.
    std::optional<std::string> int96_timezone = std::nullopt;
};

// Writes Doris blocks as Parquet files, including schema and Arrow conversion.
class VParquetWriter : public VFileFormatTransformer {
public:
    VParquetWriter(RuntimeState* state, doris::io::FileWriter* file_writer,
                   const VExprContextSPtrs& output_vexpr_ctxs,
                   std::vector<std::string> column_names, bool output_object_data,
                   const ParquetFileOptions& parquet_options);

    VParquetWriter(RuntimeState* state, doris::io::FileWriter* file_writer,
                   const VExprContextSPtrs& output_vexpr_ctxs,
                   std::vector<TParquetSchema> parquet_schemas, bool output_object_data,
                   const ParquetFileOptions& parquet_options);

    ~VParquetWriter() override = default;

    Status open() override;

    Status write(const Block& block) override;

    Status close() override;

    int64_t written_len() override;

protected:
    // Construct the schema and column bindings together for each writer instance.
    virtual std::unique_ptr<ArrowBlockConvertor> _create_arrow_block_convertor(
            DataTypes types, std::vector<std::string> names, const std::string& timezone_name,
            const cctz::time_zone& timezone, bool enable_int96_timestamps) const;
    std::shared_ptr<::parquet::FileMetaData> _file_metadata() const { return _writer->metadata(); }

private:
    std::unique_ptr<ArrowBlockConvertor> _arrow_block_convertor;
    Status _parse_properties();
    arrow::Status _open_file_writer();

    std::shared_ptr<ParquetOutputStream> _outstream;
    std::shared_ptr<::parquet::WriterProperties> _parquet_writer_properties;
    std::shared_ptr<::parquet::ArrowWriterProperties> _arrow_properties;
    std::unique_ptr<::parquet::arrow::FileWriter> _writer;

    std::vector<std::string> _column_names;
    std::vector<TParquetSchema> _parquet_schemas;
    const ParquetFileOptions _parquet_options;
    std::string _timezone;
    cctz::time_zone _timezone_obj;
    uint64_t _write_size = 0;
};

} // namespace doris

#include "common/compile_check_end.h"
