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
#include <gen_cpp/PlanNodes_types.h>
#include <parquet/file_writer.h>
#include <parquet/properties.h>
#include <parquet/types.h>

#include <cstdint>
#include <string_view>

#include "util/block_compression.h"
#include "vfile_format_transformer.h"

namespace doris {
namespace io {
class FileWriter;
} // namespace io
} // namespace doris

namespace doris::vectorized {

class VCSVTransformer final : public VFileFormatTransformer {
public:
    VCSVTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                    const VExprContextSPtrs& output_vexpr_ctxs, bool output_object_data,
                    std::string_view header_type, std::string_view header,
                    std::string_view column_separator, std::string_view line_delimiter,
                    bool with_bom, TFileCompressType::type compress_type = TFileCompressType::PLAIN,
                    const THiveSerDeProperties* hive_serde_properties = nullptr);

    ~VCSVTransformer() = default;

    Status open() override;

    Status write(const Block& block) override;

    Status close() override;

    int64_t written_len() override;

private:
    Status _flush_plain_text_outstream(ColumnString& ser_col);
    std::string _gen_csv_header_types();

    std::string _csv_header;
    std::string_view _column_separator;
    std::string_view _line_delimiter;

    doris::io::FileWriter* _file_writer = nullptr;
    // Used to buffer the export data of plain text
    // TODO(cmy): I simply use a fmt::memmory_buffer to buffer the data, to avoid calling
    // file writer's write() for every single row.
    // But this cannot solve the problem of a row of data that is too large.
    // For example: bitmap_to_string() may return large volume of data.
    // And the speed is relative low, in my test, is about 6.5MB/s.
    fmt::memory_buffer _outstream_buffer;

    bool _with_bom = false;
    const TFileCompressType::type _compress_type;
    BlockCompressionCodec* _compress_codec = nullptr;
    const bool _is_text_format; // true: text format, false: csv format
};

} // namespace doris::vectorized
