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

#include "vec/runtime/vorc_transformer.h"

#include <glog/logging.h>
#include <stdlib.h>
#include <string.h>

#include <exception>
#include <ostream>

#include "io/fs/file_writer.h"
#include "orc/Int128.hh"
#include "orc/MemoryPool.hh"
#include "orc/OrcFile.hh"
#include "orc/Vector.hh"
#include "runtime/define_primitive_type.h"
#include "runtime/types.h"
#include "util/binary_cast.hpp"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_number.h"
#include "vec/common/assert_cast.h"
#include "vec/common/pod_array.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris::vectorized {
VOrcOutputStream::VOrcOutputStream(doris::io::FileWriter* file_writer)
        : _file_writer(file_writer), _cur_pos(0), _written_len(0), _name("VOrcOutputStream") {}

VOrcOutputStream::~VOrcOutputStream() {
    if (!_is_closed) {
        close();
    }
}

void VOrcOutputStream::close() {
    if (!_is_closed) {
        Status st = _file_writer->close();
        _is_closed = true;
        if (!st.ok()) {
            LOG(WARNING) << "close orc output stream failed: " << st;
            throw std::runtime_error(st.to_string());
        }
    }
}

void VOrcOutputStream::write(const void* data, size_t length) {
    if (!_is_closed) {
        Status st = _file_writer->append({static_cast<const uint8_t*>(data), length});
        if (!st.ok()) {
            LOG(WARNING) << "Write to ORC file failed: " << st;
            return;
        }
        _cur_pos += length;
        _written_len += length;
    }
}

void VOrcOutputStream::set_written_len(int64_t written_len) {
    _written_len = written_len;
}

VOrcTransformer::VOrcTransformer(doris::io::FileWriter* file_writer,
                                 const VExprContextSPtrs& output_vexpr_ctxs,
                                 const std::string& schema, bool output_object_data)
        : VFileFormatTransformer(output_vexpr_ctxs, output_object_data),
          _file_writer(file_writer),
          _write_options(new orc::WriterOptions()),
          _schema_str(schema) {
        _write_options->setTimezoneName("Asia/Shanghai");
}

Status VOrcTransformer::open() {
    try {
        _schema = orc::Type::buildTypeFromString(_schema_str);
    } catch (const std::exception& e) {
        return Status::InternalError("Orc build schema from \"{}\" failed: {}", _schema_str,
                                     e.what());
    }
    _output_stream = std::unique_ptr<VOrcOutputStream>(new VOrcOutputStream(_file_writer));
    _writer = orc::createWriter(*_schema, _output_stream.get(), *_write_options);
    if (_writer == nullptr) {
        return Status::InternalError("Failed to create file writer");
    }
    return Status::OK();
}

std::unique_ptr<orc::ColumnVectorBatch> VOrcTransformer::_create_row_batch(size_t sz) {
    return _writer->createRowBatch(sz);
}

int64_t VOrcTransformer::written_len() {
    // written_len() will be called in VFileResultWriter::_close_file_writer
    // but _output_stream may be nullptr
    // because the failure built by _schema in open()
    if (_output_stream) {
        return _output_stream->getLength();
    }
    return 0;
}

Status VOrcTransformer::close() {
    if (_writer != nullptr) {
        try {
            _writer->close();
        } catch (const std::exception& e) {
            return Status::IOError(e.what());
        }
    }
    if (_output_stream) {
        _output_stream->close();
    }
    return Status::OK();
}

Status VOrcTransformer::write(const Block& block) {
    if (block.rows() == 0) {
        return Status::OK();
    }

    // Buffer used by date/datetime/datev2/datetimev2/largeint type
    std::vector<StringRef> buffer_list;
    Defer defer {[&]() {
        for (auto& bufferRef : buffer_list) {
            if (bufferRef.data) {
                free(const_cast<char*>(bufferRef.data));
            }
        }
    }};

    size_t sz = block.rows();
    auto row_batch = _create_row_batch(sz);
    orc::StructVectorBatch* root = dynamic_cast<orc::StructVectorBatch*>(row_batch.get());
    try {
        for (size_t i = 0; i < block.columns(); i++) {
            auto& raw_column = block.get_by_position(i).column;
            RETURN_IF_ERROR(_serdes[i]->write_column_to_orc(*raw_column, nullptr, root->fields[i],
                                                            0, sz, buffer_list));
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "Orc write error: " << e.what();
        return Status::InternalError(e.what());
    }
    root->numElements = sz;
    _writer->add(*row_batch);
    _cur_written_rows += sz;

    return Status::OK();
}

} // namespace doris::vectorized
