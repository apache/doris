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

#include "format/orc/vorc_reader.h"
#include "format/parquet/vparquet_reader.h"
#include "format/table/table_schema_change_helper.h"

namespace doris {
class ShardedKVCache;

// PaimonOrcReader: directly inherits OrcReader (no composition wrapping).
// Schema mapping in on_before_init_reader, deletion vector reading in on_after_init_reader.
class PaimonOrcReader final : public OrcReader, public TableSchemaChangeHelper {
public:
    ENABLE_FACTORY_CREATOR(PaimonOrcReader);
    PaimonOrcReader(RuntimeProfile* profile, RuntimeState* state,
                    const TFileScanRangeParams& params, const TFileRangeDesc& range,
                    size_t batch_size, const std::string& ctz, ShardedKVCache* kv_cache,
                    io::IOContext* io_ctx, FileMetaCache* meta_cache = nullptr,
                    bool enable_lazy_mat = true)
            : OrcReader(profile, state, params, range, batch_size, ctz, io_ctx, meta_cache,
                        enable_lazy_mat),
              _kv_cache(kv_cache) {
        _init_paimon_profile();
    }
    ~PaimonOrcReader() final = default;

protected:
    Status on_before_init_reader(ReaderInitContext* ctx) override;

    Status on_after_init_reader(ReaderInitContext* /*ctx*/) override;

private:
    void _init_paimon_profile();
    Status _init_deletion_vector();

    struct PaimonProfile {
        RuntimeProfile::Counter* num_delete_rows = nullptr;
        RuntimeProfile::Counter* delete_files_read_time = nullptr;
        RuntimeProfile::Counter* parse_deletion_vector_time = nullptr;
    };

    const std::vector<int64_t>* _delete_rows = nullptr;
    ShardedKVCache* _kv_cache;
    PaimonProfile _paimon_profile;
};

// PaimonParquetReader: directly inherits ParquetReader (no composition wrapping).
class PaimonParquetReader final : public ParquetReader, public TableSchemaChangeHelper {
public:
    ENABLE_FACTORY_CREATOR(PaimonParquetReader);
    PaimonParquetReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                        const TFileRangeDesc& range, size_t batch_size, const cctz::time_zone* ctz,
                        ShardedKVCache* kv_cache, io::IOContext* io_ctx, RuntimeState* state,
                        FileMetaCache* meta_cache = nullptr, bool enable_lazy_mat = true)
            : ParquetReader(profile, params, range, batch_size, ctz, io_ctx, state, meta_cache,
                            enable_lazy_mat),
              _kv_cache(kv_cache) {
        _init_paimon_profile();
    }
    ~PaimonParquetReader() final = default;

protected:
    Status on_before_init_reader(ReaderInitContext* ctx) override;

    Status on_after_init_reader(ReaderInitContext* /*ctx*/) override;

private:
    void _init_paimon_profile();
    Status _init_deletion_vector();

    struct PaimonProfile {
        RuntimeProfile::Counter* num_delete_rows = nullptr;
        RuntimeProfile::Counter* delete_files_read_time = nullptr;
        RuntimeProfile::Counter* parse_deletion_vector_time = nullptr;
    };

    const std::vector<int64_t>* _delete_rows = nullptr;
    ShardedKVCache* _kv_cache;
    PaimonProfile _paimon_profile;
};

} // namespace doris
