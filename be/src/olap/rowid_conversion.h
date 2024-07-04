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

#include <map>
#include <vector>

#include "io/fs/file_reader.h"
#include "io/fs/file_reader_writer_fwd.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "io/fs/path.h"
#include "olap/olap_common.h"
#include "olap/tablet.h"
#include "olap/utils.h"

namespace doris {

// For unique key merge on write table, we should update delete bitmap
// of destination rowset when compaction finished.
// Through the row id correspondence between the source rowset and the
// destination rowset, we can quickly update the delete bitmap of the
// destination rowset.
class RowIdConversion {
public:
    RowIdConversion();
    ~RowIdConversion();
    void init_segment_map(const RowsetId& src_rowset_id, const std::vector<uint32_t>& num_rows);

    // set dst rowset id
    void set_dst_rowset_id(const RowsetId& dst_rowset_id) { _dst_rowst_id = dst_rowset_id; }
    const RowsetId get_dst_rowset_id() { return _dst_rowst_id; }

    void add(const std::vector<RowLocation>& rss_row_ids,
             const std::vector<uint32_t>& dst_segments_num_row);

    const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& get_rowid_conversion_map()
            const {
        return _segments_rowid_map;
    }

    const std::map<std::pair<RowsetId, uint32_t>, uint32_t>& get_src_segment_to_id_map() {
        return _segment_to_id_map;
    }

    std::pair<RowsetId, uint32_t> get_segment_by_id(uint32_t id) const {
        DCHECK_GT(_id_to_segment_map.size(), id);
        return _id_to_segment_map.at(id);
    }

    uint32_t get_id_by_segment(const std::pair<RowsetId, uint32_t>& segment) const {
        return _segment_to_id_map.at(segment);
    }

    void set_file_name(std::string file_name);

    Status create_file_name(BaseTabletSPtr tablet, ReaderType reader_type);

    Status save_to_file_if_necessary(BaseTabletSPtr tablet, ReaderType reader_type);

    Status save_to_file();

    Status open_file();

    void clear_segments_rowid_map();

    int get(const RowLocation& src, RowLocation* dst) const;

    uint64_t count();

private:
    // the first level vector: index indicates src segment.
    // the second level vector: index indicates row id of source segment,
    // value indicates row id of destination segment.
    // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
    std::vector<std::vector<std::pair<uint32_t, uint32_t>>> _segments_rowid_map;

    // Map source segment to 0 to n
    std::map<std::pair<RowsetId, uint32_t>, uint32_t> _segment_to_id_map;

    // Map 0 to n to source segment
    std::vector<std::pair<RowsetId, uint32_t>> _id_to_segment_map;

    // Map src segment index to file reading position
    std::map<uint32_t, uint32_t> _id_to_pos_map;

    // dst rowset id
    RowsetId _dst_rowst_id;

    // current dst segment id
    std::uint32_t _cur_dst_segment_id = 0;

    // current rowid of dst segment
    std::uint32_t _cur_dst_segment_rowid = 0;

    std::string _file_name;
    io::FileWriterPtr _file_writer;
    io::FileReaderSPtr _file_reader;

    uint64_t _count = 0;
    bool _read_from_file = false;
};

} // namespace doris
