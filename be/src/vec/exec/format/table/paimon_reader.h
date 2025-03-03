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

#include <memory>
#include <vector>

#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/table/table_format_reader.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class PaimonReader : public TableFormatReader {
public:
    PaimonReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                 RuntimeState* state, const TFileScanRangeParams& params,
                 const TFileRangeDesc& range, io::IOContext* io_ctx);
    ~PaimonReader() override = default;

    Status init_row_filters() final;

    Status get_next_block_inner(Block* block, size_t* read_rows, bool* eof) final;

protected:
    struct PaimonProfile {
        RuntimeProfile::Counter* num_delete_rows;
        RuntimeProfile::Counter* delete_files_read_time;
    };
    std::vector<int64_t> _delete_rows;
    PaimonProfile _paimon_profile;

    virtual void set_delete_rows() = 0;
};

class PaimonOrcReader final : public PaimonReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonOrcReader);
    PaimonOrcReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                    RuntimeState* state, const TFileScanRangeParams& params,
                    const TFileRangeDesc& range, io::IOContext* io_ctx)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~PaimonOrcReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<OrcReader*>(_file_format_reader.get()))
                ->set_position_delete_rowids(&_delete_rows);
    }
};

class PaimonParquetReader final : public PaimonReader {
public:
    ENABLE_FACTORY_CREATOR(PaimonParquetReader);
    PaimonParquetReader(std::unique_ptr<GenericReader> file_format_reader, RuntimeProfile* profile,
                        RuntimeState* state, const TFileScanRangeParams& params,
                        const TFileRangeDesc& range, io::IOContext* io_ctx)
            : PaimonReader(std::move(file_format_reader), profile, state, params, range, io_ctx) {};
    ~PaimonParquetReader() final = default;

    void set_delete_rows() final {
        (reinterpret_cast<ParquetReader*>(_file_format_reader.get()))
                ->set_delete_rows(&_delete_rows);
    }
};
#include "common/compile_check_end.h"
} // namespace doris::vectorized
