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

#include "exec/sink/viceberg_delete_rewrite_loader.h"

#include "common/status.h"
#include "format/table/iceberg_delete_file_reader_helper.h"
#include "io/hdfs_builder.h"
#include "runtime/runtime_state.h"

namespace doris {

namespace {
class RewriteBitmapVisitor final : public IcebergPositionDeleteVisitor {
public:
    RewriteBitmapVisitor(const std::string& referenced_data_file_path,
                         roaring::Roaring64Map* rows_to_delete)
            : _referenced_data_file_path(referenced_data_file_path),
              _rows_to_delete(rows_to_delete) {}

    Status visit(const std::string& file_path, int64_t pos) override {
        if (_rows_to_delete == nullptr) {
            return Status::InvalidArgument("rows_to_delete is null");
        }
        if (file_path == _referenced_data_file_path) {
            _rows_to_delete->add(static_cast<uint64_t>(pos));
        }
        return Status::OK();
    }

private:
    const std::string& _referenced_data_file_path;
    roaring::Roaring64Map* _rows_to_delete;
};
} // namespace

Status VIcebergDeleteRewriteLoader::load(RuntimeState* state, RuntimeProfile* profile,
                                         const std::string& referenced_data_file_path,
                                         const std::vector<TIcebergDeleteFileDesc>& delete_files,
                                         const std::map<std::string, std::string>& hadoop_conf,
                                         TFileType::type file_type,
                                         const std::vector<TNetworkAddress>& broker_addresses,
                                         roaring::Roaring64Map* rows_to_delete) {
    if (rows_to_delete == nullptr) {
        return Status::InvalidArgument("rows_to_delete is null");
    }
    if (state == nullptr || profile == nullptr || delete_files.empty()) {
        return Status::OK();
    }

    TFileScanRangeParams params =
            build_iceberg_delete_scan_range_params(hadoop_conf, file_type, broker_addresses);
    IcebergDeleteFileIOContext delete_file_io_ctx(state);
    IcebergDeleteFileReaderOptions options;
    options.state = state;
    options.profile = profile;
    options.scan_params = &params;
    options.io_ctx = &delete_file_io_ctx.io_ctx;
    options.batch_size = 102400;

    for (const auto& delete_file : delete_files) {
        if (is_iceberg_deletion_vector(delete_file)) {
            RETURN_IF_ERROR(read_iceberg_deletion_vector(delete_file, options, rows_to_delete));
            continue;
        }
        RewriteBitmapVisitor visitor(referenced_data_file_path, rows_to_delete);
        RETURN_IF_ERROR(read_iceberg_position_delete_file(delete_file, options, &visitor));
    }
    return Status::OK();
}

} // namespace doris
