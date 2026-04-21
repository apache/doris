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
#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/factory_creator.h"
#include "common/status.h"
#include "format/orc/vorc_reader.h"
#include "format/table/table_schema_change_helper.h"
#include "format/table/transactional_hive_common.h"

namespace doris {
class RuntimeState;
class TFileRangeDesc;
class TFileScanRangeParams;

namespace io {
struct IOContext;
} // namespace io

class Block;
class ShardedKVCache;
class VExprContext;

// TransactionalHiveReader: directly inherits OrcReader (no composition wrapping).
// ACID column expansion/shrinking done via on_before_read_block/on_after_read_block hooks.
// Delete delta reading done via on_after_init_reader hook.
class TransactionalHiveReader final : public OrcReader, public TableSchemaChangeHelper {
    ENABLE_FACTORY_CREATOR(TransactionalHiveReader);

public:
    TransactionalHiveReader(RuntimeProfile* profile, RuntimeState* state,
                            const TFileScanRangeParams& params, const TFileRangeDesc& range,
                            size_t batch_size, const std::string& ctz, io::IOContext* io_ctx,
                            FileMetaCache* meta_cache = nullptr);
    ~TransactionalHiveReader() final = default;

protected:
    // Hook: ACID schema mapping (add transactional columns, map row.* fields)
    Status on_before_init_reader(ReaderInitContext* ctx) override;

    // Hook: read delete delta files
    Status on_after_init_reader(ReaderInitContext* /*ctx*/) override;

    // Hook: expand ACID columns into block before reading
    Status on_before_read_block(Block* block) override;

    // Hook: shrink ACID columns from block after reading
    Status on_after_read_block(Block* block, size_t* read_rows) override;

private:
    struct TransactionalHiveProfile {
        RuntimeProfile::Counter* num_delete_files = nullptr;
        RuntimeProfile::Counter* num_delete_rows = nullptr;
        RuntimeProfile::Counter* delete_files_read_time = nullptr;
    };

    TransactionalHiveProfile _transactional_orc_profile;
    AcidRowIDSet _acid_delete_rows;
    std::vector<std::string> _col_names;
};

} // namespace doris
