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

#ifndef DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
#define DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_

#include <stddef.h>

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/tablet_schema.h"

namespace doris {

class BetaRowset;

namespace io {
class RemoteFileSystem;
} // namespace io
struct RowsetId;

using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;

class BetaRowset final : public Rowset {
public:
    virtual ~BetaRowset();

    Status create_reader(RowsetReaderSharedPtr* result) override;

    std::string segment_file_path(int segment_id);

    std::string segment_cache_path(int segment_id);

    static bool is_segment_cache_dir(const std::string& cache_dir);

    static std::string segment_file_path(const std::string& rowset_dir, const RowsetId& rowset_id,
                                         int segment_id);

    static std::string local_segment_path_segcompacted(const std::string& tablet_path,
                                                       const RowsetId& rowset_id, int64_t begin,
                                                       int64_t end);

    static std::string remote_segment_path(int64_t tablet_id, const RowsetId& rowset_id,
                                           int segment_id);

    static std::string remote_segment_path(int64_t tablet_id, const std::string& rowset_id,
                                           int segment_id);

    Status remove() override;

    Status link_files_to(const std::string& dir, RowsetId new_rowset_id,
                         size_t new_rowset_start_seg_id = 0,
                         std::set<int64_t>* without_index_uids = nullptr) override;

    Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) override;

    Status upload_to(io::RemoteFileSystem* dest_fs, const RowsetId& new_rowset_id) override;

    // only applicable to alpha rowset, no op here
    Status remove_old_files(std::vector<std::string>* files_to_remove) override {
        return Status::OK();
    }

    bool check_path(const std::string& path) override;

    bool check_file_exist() override;

    Status load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments);

    Status load_segments(int64_t seg_id_begin, int64_t seg_id_end,
                         std::vector<segment_v2::SegmentSharedPtr>* segments);

    Status get_segments_size(std::vector<size_t>* segments_size);

    void clear_inverted_index_cache() override;

    [[nodiscard]] virtual Status add_to_binlog() override;

    void clear_cache() override;

protected:
    BetaRowset(const TabletSchemaSPtr& schema, const std::string& tablet_path,
               const RowsetMetaSharedPtr& rowset_meta);

    // init segment groups
    Status init() override;

    Status do_load(bool use_cache) override;

    void do_close() override;

    bool check_current_rowset_segment() override;

private:
    friend class RowsetFactory;
    friend class BetaRowsetReader;
};

} // namespace doris

#endif //DORIS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
