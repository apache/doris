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

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "util/bitmap_value.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/sink/writer/async_result_writer.h"
#include "vec/sink/writer/iceberg/viceberg_delete_file_writer.h"

namespace doris {

class ObjectPool;
class RuntimeState;

namespace vectorized {

class VIcebergDeleteFileWriter;

struct IcebergFileDeletion {
    IcebergFileDeletion() = default;
    IcebergFileDeletion(int32_t spec_id, std::string data_json)
            : partition_spec_id(spec_id), partition_data_json(std::move(data_json)) {}

    int32_t partition_spec_id = 0;
    std::string partition_data_json;
    doris::detail::Roaring64Map rows_to_delete;
};

/**
 * Sink for writing Iceberg position delete files.
 *
 * This sink receives blocks containing a $row_id column with
 * (file_path, row_position, partition_spec_id, partition_data).
 * It groups delete records by file_path and writes position delete files.
 */
class VIcebergDeleteSink final : public AsyncResultWriter {
public:
    VIcebergDeleteSink(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs,
                       std::shared_ptr<pipeline::Dependency> dep,
                       std::shared_ptr<pipeline::Dependency> fin_dep);

    ~VIcebergDeleteSink() override = default;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(RuntimeState* state, vectorized::Block& block) override;

    Status close(Status) override;

private:
    /**
     * Extract $row_id column from block and group by file_path.
     */
    Status _collect_position_deletes(const Block& block,
                                     std::map<std::string, IcebergFileDeletion>& file_deletions);

    /**
     * Write grouped position deletes to delete files
     */
    Status _write_position_delete_files(
            const std::map<std::string, IcebergFileDeletion>& file_deletions);

    /**
     * Generate unique delete file path
     */
    std::string _generate_delete_file_path(const std::string& referenced_data_file = "");

    /**
     * Get $row_id column index from block
     */
    int _get_row_id_column_index(const Block& block);

    /**
     * Build a block for position delete (file_path, pos)
     */
    Status _build_position_delete_block(const std::string& file_path,
                                        const std::vector<int64_t>& positions, Block& output_block);
    Status _init_position_delete_output_exprs();
    std::string _get_file_extension() const;

    TDataSink _t_sink;
    RuntimeState* _state = nullptr;

    TFileContent::type _delete_type = TFileContent::POSITION_DELETES;

    // Writers for delete files
    std::vector<std::unique_ptr<VIcebergDeleteFileWriter>> _writers;

    // Collected commit data from all writers
    std::vector<TIcebergCommitData> _commit_data_list;
    std::map<std::string, IcebergFileDeletion> _file_deletions;

    // Hadoop configuration
    std::map<std::string, std::string> _hadoop_conf;

    // File format settings
    TFileFormatType::type _file_format_type = TFileFormatType::FORMAT_PARQUET;
    TFileCompressType::type _compress_type = TFileCompressType::SNAPPYBLOCK;

    // Output directory for delete files
    std::string _output_path;
    std::string _table_location;

    TFileType::type _file_type = TFileType::FILE_HDFS;
    std::vector<TNetworkAddress> _broker_addresses;

    // Partition information
    int32_t _partition_spec_id = 0;
    std::string _partition_data_json;

    // Counters
    size_t _row_count = 0;
    size_t _delete_file_count = 0;

    // Profile counters
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _write_delete_files_timer = nullptr;
    RuntimeProfile::Counter* _delete_file_count_counter = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;

    VExprContextSPtrs _position_delete_output_expr_ctxs;
};

} // namespace vectorized
} // namespace doris
