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
#include <string>
#include <vector>

#include "common/status.h"
#include "core/block/block.h"
#include "exprs/vexpr_fwd.h"
#include "gen_cpp/DataSinks_types.h"
#include "io/fs/file_writer.h"

namespace doris {

class RuntimeState;
class RuntimeProfile;
class ObjectPool;

namespace io {
class FileSystem;
}

class VFileFormatTransformer;

/**
 * Writer for Iceberg Position Delete files.
 *
 * Schema: (file_path: string, pos: long)
 * Each row contains the file path and row position to delete.
 */
class VIcebergDeleteFileWriter {
public:
    VIcebergDeleteFileWriter(TFileContent::type delete_type, const std::string& output_path,
                             TFileFormatType::type file_format,
                             TFileCompressType::type compress_type);

    ~VIcebergDeleteFileWriter();

    /**
     * Open the writer with runtime state
     */
    Status open(RuntimeState* state, RuntimeProfile* profile, const VExprContextSPtrs& output_exprs,
                const std::vector<std::string>& column_names,
                const std::map<std::string, std::string>& hadoop_config, TFileType::type file_type,
                const std::vector<TNetworkAddress>& broker_addresses = {});

    /**
     * Write a block of delete data.
     * For Position Delete: block contains (file_path, pos) columns.
     */
    Status write(const Block& block);

    /**
     * Close the writer and return commit data
     */
    Status close(TIcebergCommitData& commit_data);

    /**
     * Set partition information for the delete file.
     * Must be called before close() so that commit data includes correct partition info.
     */
    void set_partition_info(int32_t partition_spec_id, const std::string& partition_data_json) {
        _partition_spec_id = partition_spec_id;
        _partition_data_json = partition_data_json;
    }

    /**
     * Get the number of rows written
     */
    int64_t get_written_rows() const { return _written_rows; }

    /**
     * Get the file size in bytes
     */
    int64_t get_file_size() const { return _file_size; }

private:
    TFileContent::type _delete_type;
    std::string _output_path;
    TFileFormatType::type _file_format;
    TFileCompressType::type _compress_type = TFileCompressType::SNAPPYBLOCK;

    int64_t _written_rows = 0;
    int64_t _file_size = 0;

    RuntimeState* _state = nullptr;
    std::shared_ptr<io::FileSystem> _fs;
    io::FileWriterPtr _file_writer;
    std::unique_ptr<VFileFormatTransformer> _file_format_transformer;

    int32_t _partition_spec_id = 0;
    std::string _partition_data_json;
    std::vector<std::string> _partition_values;
};

/**
 * Factory class for creating delete file writers
 */
class VIcebergDeleteFileWriterFactory {
public:
    static std::unique_ptr<VIcebergDeleteFileWriter> create_writer(
            TFileContent::type delete_type, const std::string& output_path,
            TFileFormatType::type file_format, TFileCompressType::type compress_type);
};

} // namespace doris
