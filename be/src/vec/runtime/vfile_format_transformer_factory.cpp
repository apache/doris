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

#include "vec/runtime/vfile_format_transformer_factory.h"

#include <gen_cpp/PlanNodes_types.h>

#include <string>
#include <vector>

#include "vec/runtime/vcsv_transformer.h"
#include "vec/runtime/vjni_format_transformer.h"
#include "vec/runtime/vorc_transformer.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris::vectorized {

Status create_tvf_format_transformer(const TTVFTableSink& tvf_sink, RuntimeState* state,
                                     io::FileWriter* file_writer,
                                     const VExprContextSPtrs& output_vexpr_ctxs,
                                     std::unique_ptr<VFileFormatTransformer>* result) {
    // JNI writer path: delegate to Java-side writer
    if (tvf_sink.__isset.writer_type && tvf_sink.writer_type == TTVFWriterType::JNI) {
        if (!tvf_sink.__isset.writer_class) {
            return Status::InternalError("writer_class is required when writer_type is JNI");
        }
        std::map<std::string, std::string> writer_params;
        if (tvf_sink.__isset.properties) {
            writer_params = tvf_sink.properties;
        }
        writer_params["file_path"] = tvf_sink.file_path;
        if (tvf_sink.__isset.column_separator) {
            writer_params["column_separator"] = tvf_sink.column_separator;
        }
        if (tvf_sink.__isset.line_delimiter) {
            writer_params["line_delimiter"] = tvf_sink.line_delimiter;
        }
        result->reset(new VJniFormatTransformer(state, output_vexpr_ctxs, tvf_sink.writer_class,
                                                std::move(writer_params)));
        return Status::OK();
    }

    // Native writer path
    TFileFormatType::type format = tvf_sink.file_format;
    switch (format) {
    case TFileFormatType::FORMAT_CSV_PLAIN: {
        std::string column_separator =
                tvf_sink.__isset.column_separator ? tvf_sink.column_separator : ",";
        std::string line_delimiter =
                tvf_sink.__isset.line_delimiter ? tvf_sink.line_delimiter : "\n";
        TFileCompressType::type compress_type = TFileCompressType::PLAIN;
        if (tvf_sink.__isset.compression_type) {
            compress_type = tvf_sink.compression_type;
        }
        result->reset(new VCSVTransformer(state, file_writer, output_vexpr_ctxs, false, {}, {},
                                          column_separator, line_delimiter, false, compress_type));
        break;
    }
    case TFileFormatType::FORMAT_PARQUET: {
        std::vector<TParquetSchema> parquet_schemas;
        if (tvf_sink.__isset.columns) {
            for (const auto& col : tvf_sink.columns) {
                TParquetSchema schema;
                schema.__set_schema_column_name(col.column_name);
                parquet_schemas.push_back(schema);
            }
        }
        result->reset(new VParquetTransformer(
                state, file_writer, output_vexpr_ctxs, parquet_schemas, false,
                {TParquetCompressionType::SNAPPY, TParquetVersion::PARQUET_1_0, false, false}));
        break;
    }
    case TFileFormatType::FORMAT_ORC: {
        TFileCompressType::type compress_type = TFileCompressType::PLAIN;
        if (tvf_sink.__isset.compression_type) {
            compress_type = tvf_sink.compression_type;
        }
        std::vector<std::string> orc_column_names;
        if (tvf_sink.__isset.columns) {
            for (const auto& col : tvf_sink.columns) {
                orc_column_names.push_back(col.column_name);
            }
        }
        result->reset(new VOrcTransformer(state, file_writer, output_vexpr_ctxs, "",
                                          orc_column_names, false, compress_type));
        break;
    }
    default:
        return Status::InternalError("Unsupported TVF sink file format: {}", format);
    }
    return Status::OK();
}

} // namespace doris::vectorized
