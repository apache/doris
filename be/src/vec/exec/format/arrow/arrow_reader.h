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
#include <gen_cpp/internal_service.pb.h>
#include <stddef.h>
#include <stdint.h>

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "arrow_batch_reader.h"
#include "common/status.h"
#include "io/file_factory.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "util/slice.h"
#include "vec/data_types/data_type.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/format/generic_reader.h"

namespace doris {

namespace io {
class FileSystem;
struct IOContext;
} // namespace io

namespace vectorized {

struct ScannerCounter;
class Block;

class ArrowReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(ArrowReader);

public:
    ArrowReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                const TFileScanRangeParams& params, const TFileRangeDesc& range,
                const std::vector<SlotDescriptor*>& file_slot_descs, io::IOContext* io_ctx);

    ~ArrowReader() override;

    Status init_reader();

    Status get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status get_columns(std::unordered_map<std::string, TypeDescriptor>* name_to_type,
                       std::unordered_set<std::string>* missing_cols) override;

private:
    RuntimeState* _state;
    // RuntimeProfile* _profile;
    // ScannerCounter* _counter;
    // const TFileScanRangeParams& _params;
    const TFileRangeDesc& _range;
    const std::vector<SlotDescriptor*>& _file_slot_descs;
    io::FileReaderSPtr _file_reader;
    uint8_t* _file_buf;
    std::unique_ptr<doris::vectorized::ArrowBatchReader> _arrow_batch_reader;
};
} // namespace vectorized
} // namespace doris
