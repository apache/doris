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

#include <gen_cpp/DataSinks_types.h>

#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "runtime/runtime_profile.h"
#include "core/block/block.h"

namespace paimon {
class FileStoreWrite;
class MemoryPool;
} // namespace paimon

namespace arrow {
class MemoryPool;
}

namespace doris {

class ObjectPool;
class RuntimeProfile;
class RuntimeState;

namespace vectorized {


// Single write unit for a specific Paimon partition + bucket.
// It writes RecordBatch into a shared paimon-cpp FileStoreWrite instance.
class VPaimonPartitionWriter {
public:
    VPaimonPartitionWriter(const TDataSink& t_sink, std::vector<std::string> partition_values,
                           int32_t bucket_id
#ifdef WITH_PAIMON_CPP
                           ,
                           ::paimon::FileStoreWrite* file_store_write,
                           std::shared_ptr<::paimon::MemoryPool> pool
#endif
    );

    // Keep the same interface style with IcebergPartitionWriter.
    Status init_properties(ObjectPool* /*pool*/) { return Status::OK(); }

    Status open(RuntimeState* state, RuntimeProfile* profile);

    Status write(::doris::Block& block);

    Status close(const Status& status);

private:
    Status _write_block(::doris::Block& block);
    Status _append_to_buffer(const ::doris::Block& block);
    Status _flush_buffer();

    [[maybe_unused]] const TDataSink& _t_sink;
    std::vector<std::string> _partition_values;
    [[maybe_unused]] int32_t _bucket_id = -1;

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;

    RuntimeProfile::Counter* _arrow_convert_timer = nullptr;
    RuntimeProfile::Counter* _file_store_write_timer = nullptr;
    RuntimeProfile::Counter* _buffer_flush_count = nullptr;

#ifdef WITH_PAIMON_CPP
    size_t _batch_max_rows = 32768;
    size_t _batch_max_bytes = 4 * 1024 * 1024;
    size_t _buffered_rows = 0;
    size_t _buffered_bytes = 0;
    std::unique_ptr<::doris::Block> _buffer;
#endif

#ifdef WITH_PAIMON_CPP
    ::paimon::FileStoreWrite* _file_store_write = nullptr;
    std::shared_ptr<::paimon::MemoryPool> _pool;
    std::shared_ptr<arrow::MemoryPool> _arrow_pool;
#endif
};

} // namespace vectorized
} // namespace doris
