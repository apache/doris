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

#ifdef BUILD_RUST_READERS

#include <cctz/time_zone.h>

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "core/data_type/data_type.h"
#include "format/generic_reader.h"

namespace arrow {
class DataType;
}

namespace doris {
namespace io {
struct IOContext;
}

class RuntimeProfile;
class RuntimeState;
class SlotDescriptor;
class Block;
class TFileRangeDesc;
class TFileScanRangeParams;

/// Reads Lance format datasets via Rust FFI (lance-rs).
///
/// Data exchange uses the Arrow C Data Interface: the Rust side exports
/// ArrowSchema + ArrowArray, which the C++ side imports as an
/// arrow::RecordBatch, then converts column-by-column to Doris Block.
///
/// Each reader instance owns an opaque Rust handle that holds a
/// single-threaded tokio runtime and a lance::Scanner stream.
class LanceRustReader : public GenericReader {
    ENABLE_FACTORY_CREATOR(LanceRustReader);

public:
    LanceRustReader(const std::vector<SlotDescriptor*>& file_slot_descs, RuntimeState* state,
                    RuntimeProfile* profile, const TFileRangeDesc& range,
                    const TFileScanRangeParams* range_params);

    ~LanceRustReader() override;

    /// Constructor for schema-only mode (used by fetch_table_schema RPC).
    /// Only needs params and range, no slot descs or runtime state.
    LanceRustReader(const TFileScanRangeParams& params, const TFileRangeDesc& range,
                    io::IOContext* io_ctx);

    Status init_reader();

    /// Initialize reader in schema-only mode (open dataset, read schema, no scan).
    Status init_schema_reader() override;

    Status get_parsed_schema(std::vector<std::string>* col_names,
                             std::vector<DataTypePtr>* col_types) override;

    Status close() override;

protected:
    Status _do_get_next_block(Block* block, size_t* read_rows, bool* eof) override;

    Status _get_columns_impl(std::unordered_map<std::string, DataTypePtr>* name_to_type) override;

private:
    /// Open via JSON config — shared by init_reader() and init_schema_reader().
    Status _open_with_json(bool schema_only);

    /// Build a Doris Status from the Rust FFI error code + thread-local error message.
    Status _get_ffi_error(int32_t status_code) const;

    /// Convert an Arrow DataType to a Doris DataTypePtr.
    static DataTypePtr _arrow_type_to_doris_type(
            const std::shared_ptr<arrow::DataType>& arrow_type);

    const std::vector<SlotDescriptor*>& _file_slot_descs;
    RuntimeState* _state;
    const TFileRangeDesc& _range;
    const TFileScanRangeParams* _params;

    void* _reader_handle = nullptr;
    std::unordered_map<std::string, uint32_t> _col_name_to_block_idx;
    cctz::time_zone _ctzz;
    bool _schema_only = false;
    static const std::vector<SlotDescriptor*> _empty_slot_descs;
};

} // namespace doris

#include "common/compile_check_avoid_begin.h"
#include "common/compile_check_avoid_end.h"

#endif // BUILD_RUST_READERS
