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

#include "format/transformer/vjson_transformer.h"

#include <utility>

#include "core/column/column_string.h"
#include "core/string_buffer.hpp"
#include "io/fs/file_writer.h"
#include "runtime/runtime_state.h"
#include "util/block_compression.h"
#include "util/faststring.h"

namespace doris {

VJSONTransformer::VJSONTransformer(RuntimeState* state, io::FileWriter* file_writer,
                                   const VExprContextSPtrs& output_vexpr_ctxs,
                                   bool output_object_data, std::vector<std::string> column_names,
                                   std::string line_delimiter,
                                   TFileCompressType::type compression_type)
        : VFileFormatTransformer(state, output_vexpr_ctxs, output_object_data),
          _file_writer(file_writer),
          _column_names(std::move(column_names)),
          _line_delimiter(std::move(line_delimiter)),
          _compression_type(compression_type) {
    _options.timezone = &state->timezone_obj();
}

Status VJSONTransformer::open() {
    // Older FEs can send FORMAT_JSON without the schema added for its writer.
    if (_column_names.size() != _output_vexpr_ctxs.size()) {
        return Status::InvalidArgument("JSON OUTFILE requires a name for every output column");
    }
    for (const auto& expr : _output_vexpr_ctxs) {
        RETURN_IF_ERROR(json_format::validate_json_output_type(expr->root()->data_type()));
    }
    return get_block_compression_codec(_compression_type, &_compression_codec);
}

Status VJSONTransformer::write(const Block& block) {
    RETURN_IF_CANCELLED(_state);
    if (block.rows() == 0) {
        return Status::OK();
    }
    DCHECK_EQ(block.columns(), _column_names.size());
    auto serialized = ColumnString::create();
    BufferWritable buffer(*serialized);
    for (size_t row = 0; row < block.rows(); ++row) {
        RETURN_IF_CANCELLED(_state);
        buffer.write('{');
        for (size_t col = 0; col < block.columns(); ++col) {
            if (col != 0) buffer.write(',');
            buffer.write_json_string(_column_names[col]);
            buffer.write(':');
            const auto& value = block.get_by_position(col);
            RETURN_IF_ERROR(json_format::write_json_cell(*value.column, value.type, row, buffer,
                                                         _options, _cell_writer_state));
        }
        buffer.write('}');
        buffer.write(_line_delimiter.data(), _line_delimiter.size());
        buffer.commit();
    }

    // Append only after the complete block serialized successfully. A failed
    // cell or cancellation cannot publish the partially buffered JSON row.
    RETURN_IF_CANCELLED(_state);
    const Slice data(serialized->get_chars().data(), serialized->get_chars().size());
    if (_compression_codec) {
        faststring compressed;
        RETURN_IF_ERROR(_compression_codec->compress(data, &compressed));
        RETURN_IF_ERROR(_file_writer->append(Slice(compressed.data(), compressed.size())));
    } else {
        RETURN_IF_ERROR(_file_writer->append(data));
    }
    _cur_written_rows += block.rows();
    return Status::OK();
}

Status VJSONTransformer::close() {
    return _file_writer->close();
}

int64_t VJSONTransformer::written_len() {
    return _file_writer->bytes_appended();
}

} // namespace doris
