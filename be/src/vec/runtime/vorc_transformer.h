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

#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <orc/OrcFile.hh>
#include <string>
#include <vector>

#include "common/status.h"
#include "orc/Type.hh"
#include "orc/Writer.hh"
#include "vec/core/block.h"
#include "vec/exec/format/table/iceberg/schema.h"
#include "vec/runtime/vparquet_transformer.h"

namespace doris {
namespace io {
class FileWriter;
} // namespace io
namespace vectorized {
class VExprContext;
} // namespace vectorized
} // namespace doris
namespace orc {
struct ColumnVectorBatch;
} // namespace orc
namespace iceberg {
class NestedField;
} // namespace iceberg

namespace doris::vectorized {

class VOrcOutputStream : public orc::OutputStream {
public:
    VOrcOutputStream(doris::io::FileWriter* file_writer);

    ~VOrcOutputStream() override;

    uint64_t getLength() const override { return _written_len; }

    uint64_t getNaturalWriteSize() const override { return 128 * 1024; }

    void write(const void* buf, size_t length) override;

    const std::string& getName() const override { return _name; }

    void close() override;

    void set_written_len(int64_t written_len);

private:
    doris::io::FileWriter* _file_writer = nullptr; // not owned
    int64_t _cur_pos = 0;                          // current write position
    bool _is_closed = false;
    int64_t _written_len = 0;
    const std::string _name;
};

// a wrapper of parquet output stream
class VOrcTransformer final : public VFileFormatTransformer {
public:
    VOrcTransformer(RuntimeState* state, doris::io::FileWriter* file_writer,
                    const VExprContextSPtrs& output_vexpr_ctxs, std::string schema,
                    std::vector<std::string> column_names, bool output_object_data,
                    TFileCompressType::type compression,
                    const iceberg::Schema* iceberg_schema = nullptr);

    ~VOrcTransformer() = default;

    Status open() override;

    Status write(const Block& block) override;

    Status close() override;

    int64_t written_len() override;

private:
    void set_compression_type(const TFileCompressType::type& compress_type);
    std::unique_ptr<orc::Type> _build_orc_type(const TypeDescriptor& type_descriptor,
                                               const iceberg::NestedField* nested_field);

    std::unique_ptr<orc::ColumnVectorBatch> _create_row_batch(size_t sz);
    // The size of subtypes of a complex type may be different from
    // the size of the complex type itself,
    // so we need to resize the subtype of a complex type
    Status _resize_row_batch(const DataTypePtr& type, const IColumn& column,
                             orc::ColumnVectorBatch* orc_col_batch);

    doris::io::FileWriter* _file_writer = nullptr;
    std::vector<std::string> _column_names;
    std::unique_ptr<orc::OutputStream> _output_stream;
    std::unique_ptr<orc::WriterOptions> _write_options;
    std::string _schema_str;
    std::unique_ptr<orc::Type> _schema;
    std::unique_ptr<orc::Writer> _writer;

    const iceberg::Schema* _iceberg_schema;

    // Buffer used by date/datetime/datev2/datetimev2/largeint type
    // date/datetime/datev2/datetimev2/largeint type will be converted to string bytes to store in Buffer
    // The minimum value of largeint has 40 bytes after being converted to string(a negative number occupies a byte)
    // The bytes of date/datetime/datev2/datetimev2 after converted to string are smaller than largeint
    // Because a block is 4064 rows by default, here is 4064*40 bytes to BUFFER,
    static constexpr size_t BUFFER_UNIT_SIZE = 4064 * 40;
    // buffer reserves 40 bytes. The reserved space is just to prevent Headp-Buffer-Overflow
    static constexpr size_t BUFFER_RESERVED_SIZE = 40;

    static constexpr const char* ORC_ICEBERG_ID_KEY = "iceberg.id";
    static constexpr const char* ORC_ICEBERG_REQUIRED_KEY = "iceberg.required";
    static constexpr const char* ICEBERG_LONG_TYPE = "iceberg.long-type";
};

} // namespace doris::vectorized
