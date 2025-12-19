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

#include <cstddef>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "common/status.h"
#include "gen_cpp/PlanNodes_types.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {
class RuntimeProfile;
class RuntimeState;

namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {
class Block;

#include "common/compile_check_begin.h"

// Doris Native format reader.
// it will read a sequence of Blocks encoded in Doris Native binary format.
//
// NOTE: current implementation is just a skeleton and will be filled step by step.
class NativeReader : public GenericReader {
public:
    ENABLE_FACTORY_CREATOR(NativeReader);

    NativeReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                 const TFileRangeDesc& range, io::IOContext* io_ctx, RuntimeState* state);

    ~NativeReader() override;

    // Initialize underlying file reader and any format specific state.
    Status init_reader();

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

    Status init_schema_reader() override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override;

    Status close() override;

    bool count_read_rows() override { return true; }

protected:
    void _collect_profile_before_close() override {}

private:
    RuntimeProfile* _profile = nullptr;
    const TFileScanRangeParams& _scan_params;
    const TFileRangeDesc& _scan_range;

    io::FileReaderSPtr _file_reader;
    io::IOContext* _io_ctx = nullptr;
    RuntimeState* _state = nullptr;

    bool _eof = false;

    // Current read offset in the underlying file.
    int64_t _current_offset = 0;
    int64_t _file_size = 0;

    // Cached schema information from the first PBlock.
    bool _schema_inited = false;
    std::vector<std::string> _schema_col_names;
    std::vector<DataTypePtr> _schema_col_types;

    // Cached first block (serialized) to allow schema probing before data scan.
    std::string _first_block_buf;
    bool _first_block_loaded = false;
    bool _first_block_consumed = false;

    Status _read_next_pblock(std::string* buff, bool* eof);
    Status _init_schema_from_pblock(const PBlock& pblock);
};

#include "common/compile_check_end.h"
} // namespace doris::vectorized
