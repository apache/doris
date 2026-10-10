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

#include <string>
#include <vector>

#include "format/transformer/json_cell_writer.h"
#include "format/transformer/vfile_format_transformer.h"

namespace doris {
namespace io {
class FileWriter;
} // namespace io
class BlockCompressionCodec;

class VJSONTransformer final : public VFileFormatTransformer {
public:
    VJSONTransformer(RuntimeState* state, io::FileWriter* file_writer,
                     const VExprContextSPtrs& output_vexpr_ctxs, bool output_object_data,
                     std::vector<std::string> column_names, std::string line_delimiter,
                     TFileCompressType::type compression_type);

    Status open() override;
    Status write(const Block& block) override;
    Status close() override;
    int64_t written_len() override;

private:
    io::FileWriter* _file_writer;
    const std::vector<std::string> _column_names;
    const std::string _line_delimiter;
    const TFileCompressType::type _compression_type;
    BlockCompressionCodec* _compression_codec = nullptr;
    json_format::JsonCellWriterState _cell_writer_state;
};
} // namespace doris
