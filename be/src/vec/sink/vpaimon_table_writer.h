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
#include <unordered_map>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "exec/sink/writer/async_result_writer.h"
#include "exprs/vexpr_fwd.h"
#include "runtime/runtime_profile.h"

#ifdef WITH_PAIMON_CPP
namespace paimon {
class FileStoreWrite;
class MemoryPool;
} // namespace paimon
#endif

namespace doris {

class ObjectPool;
class RuntimeProfile;
class RuntimeState;

namespace vectorized {

class VPaimonPartitionWriter;

// Table-level writer responsible for routing rows to partition writers
// according to TPaimonWriteShuffleMode and FE-provided bucket metadata.
class VPaimonTableWriter : public AsyncResultWriter {
public:
    VPaimonTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);
    VPaimonTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                       std::shared_ptr<Dependency> dep, std::shared_ptr<Dependency> fin_dep);

    ~VPaimonTableWriter() override;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, ::doris::Block& block) override;

    Status close(Status status) override;

protected:
    // The sink descriptor is owned by the surrounding sink/operator state and
    // outlives the writer during fragment execution.
    const TDataSink& _t_sink;

private:
    Status _init_partition_column_indices(const ::doris::Block& block) const;
    std::string _default_partition_name() const;
    Status _collect_partition_value_columns(
            const ::doris::Block& block,
            std::vector<std::vector<std::string>>* partition_value_columns) const;

    struct WriteKey {
        std::vector<std::string> partition_values;
        int32_t bucket_id = -1;

        bool operator==(const WriteKey& rhs) const {
            return bucket_id == rhs.bucket_id && partition_values == rhs.partition_values;
        }
    };

    struct WriteKeyHash {
        size_t operator()(const WriteKey& key) const {
            std::hash<int32_t> int_hasher;
            std::hash<std::string> string_hasher;
            size_t hash = int_hasher(key.bucket_id);
            for (const auto& v : key.partition_values) {
                hash = hash * 31 + string_hasher(v);
            }
            return hash;
        }
    };

    Status _get_or_create_writer(const WriteKey& key,
                                 std::shared_ptr<VPaimonPartitionWriter>* writer);

    Status _filter_block(::doris::Block& block, const IColumn::Filter* filter,
                         ::doris::Block* output_block);

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;

    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _written_bytes_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _project_timer = nullptr;
    RuntimeProfile::Counter* _bucket_calc_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_dispatch_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_write_timer = nullptr;
    RuntimeProfile::Counter* _partition_writers_count = nullptr;
    RuntimeProfile::Counter* _partition_writer_created = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _prepare_commit_timer = nullptr;
    RuntimeProfile::Counter* _serialize_commit_messages_timer = nullptr;
    RuntimeProfile::Counter* _commit_payload_bytes_counter = nullptr;

    std::unordered_map<WriteKey, std::shared_ptr<VPaimonPartitionWriter>, WriteKeyHash> _writers;

    mutable bool _partition_indices_inited = false;
    mutable std::vector<int> _partition_column_indices;

#ifdef WITH_PAIMON_CPP
    std::shared_ptr<::paimon::MemoryPool> _pool;
    std::unique_ptr<::paimon::FileStoreWrite> _file_store_write;
    size_t _row_count = 0;
#endif
};

} // namespace vectorized
} // namespace doris
