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

#include <cstdint>
#include <string>

#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader.h"
#include "olap/rowset/segment_v2/segment.h"

namespace doris {

class BetaRowsetReader;
class RowsetFactory;

class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;

class BetaRowset : public Rowset {
public:
    virtual ~BetaRowset();

    Status create_reader(RowsetReaderSharedPtr* result) override;

    std::string segment_file_path(int segment_id);

    std::string segment_cache_path(int segment_id);

    static std::string segment_cache_path(const std::string& rowset_dir, const RowsetId& rowset_id,
                                          int segment_id);

    static std::string segment_file_path(const std::string& rowset_dir, const RowsetId& rowset_id,
                                         int segment_id);

    static std::string local_segment_path_segcompacted(const std::string& tablet_path,
                                                       const RowsetId& rowset_id, int64_t begin,
                                                       int64_t end);

    static std::string remote_segment_path(int64_t tablet_id, const RowsetId& rowset_id,
                                           int segment_id);

    static std::string remote_segment_path(int64_t tablet_id, const std::string& rowset_id,
                                           int segment_id);

    static std::string remote_tablet_path(int64_t tablet_id);

    Status split_range(const RowCursor& start_key, const RowCursor& end_key,
                       uint64_t request_block_row_count, size_t key_num,
                       std::vector<OlapTuple>* ranges) override;

    Status remove() override;

    Status link_files_to(const std::string& dir, RowsetId new_rowset_id,
                         size_t new_rowset_start_seg_id = 0) override;

    Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) override;

    Status upload_to(io::RemoteFileSystem* dest_fs, const RowsetId& new_rowset_id) override;

    // only applicable to alpha rowset, no op here
    Status remove_old_files(std::vector<std::string>* files_to_remove) override {
        return Status::OK();
    };

    bool check_path(const std::string& path) override;

    bool check_file_exist() override;

    Status load_segments(std::vector<segment_v2::SegmentSharedPtr>* segments);

    Status load_segment(int64_t seg_id, segment_v2::SegmentSharedPtr* segment);

    Status get_segments_size(std::vector<size_t>* segments_size);

protected:
    BetaRowset(TabletSchemaSPtr schema, const std::string& tablet_path,
               RowsetMetaSharedPtr rowset_meta);

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
