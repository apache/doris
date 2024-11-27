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

#include "inverted_index_compaction.h"

#include "inverted_index_file_writer.h"
#include "inverted_index_fs_directory.h"
#include "io/fs/local_file_system.h"
#include "olap/tablet_schema.h"
#include "util/debug_points.h"

namespace doris::segment_v2 {
Status compact_column(int64_t index_id,
                      std::vector<std::unique_ptr<DorisCompoundReader>>& src_index_dirs,
                      std::vector<lucene::store::Directory*>& dest_index_dirs,
                      std::string_view tmp_path,
                      const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& trans_vec,
                      const std::vector<uint32_t>& dest_segment_num_rows) {
    DBUG_EXECUTE_IF("index_compaction_compact_column_throw_error", {
        if (index_id % 2 == 0) {
            _CLTHROWA(CL_ERR_IO, "debug point: test throw error in index compaction");
        }
    })
    DBUG_EXECUTE_IF("index_compaction_compact_column_status_not_ok", {
        if (index_id % 2 == 1) {
            return Status::Error<ErrorCode::INVERTED_INDEX_COMPACTION_ERROR>(
                    "debug point: index compaction error");
        }
    })
    bool can_use_ram_dir = true;
    lucene::store::Directory* dir = DorisFSDirectoryFactory::getDirectory(
            io::global_local_filesystem(), tmp_path.data(), can_use_ram_dir);
    DBUG_EXECUTE_IF("compact_column_getDirectory_error", {
        _CLTHROWA(CL_ERR_IO, "debug point: compact_column_getDirectory_error in index compaction");
    })
    lucene::analysis::SimpleAnalyzer<char> analyzer;
    auto* index_writer = _CLNEW lucene::index::IndexWriter(dir, &analyzer, true /* create */,
                                                           true /* closeDirOnShutdown */);
    DBUG_EXECUTE_IF("compact_column_create_index_writer_error", {
        _CLTHROWA(CL_ERR_IO,
                  "debug point: compact_column_create_index_writer_error in index compaction");
    })
    DCHECK_EQ(src_index_dirs.size(), trans_vec.size());
    std::vector<lucene::store::Directory*> tmp_src_index_dirs(src_index_dirs.size());
    for (size_t i = 0; i < tmp_src_index_dirs.size(); ++i) {
        tmp_src_index_dirs[i] = src_index_dirs[i].get();
    }
    index_writer->indexCompaction(tmp_src_index_dirs, dest_index_dirs, trans_vec,
                                  dest_segment_num_rows);
    DBUG_EXECUTE_IF("compact_column_indexCompaction_error", {
        _CLTHROWA(CL_ERR_IO,
                  "debug point: compact_column_indexCompaction_error in index compaction");
    })

    index_writer->close();
    DBUG_EXECUTE_IF("compact_column_index_writer_close_error", {
        _CLTHROWA(CL_ERR_IO,
                  "debug point: compact_column_index_writer_close_error in index compaction");
    })
    _CLDELETE(index_writer);
    // NOTE: need to ref_cnt-- for dir,
    // when index_writer is destroyed, if closeDir is set, dir will be close
    // _CLDECDELETE(dir) will try to ref_cnt--, when it decreases to 1, dir will be destroyed.
    _CLDECDELETE(dir)

    // delete temporary segment_path, only when inverted_index_ram_dir_enable is false
    if (!config::inverted_index_ram_dir_enable) {
        std::ignore = io::global_local_filesystem()->delete_directory(tmp_path.data());
    }
    return Status::OK();
}
} // namespace doris::segment_v2
