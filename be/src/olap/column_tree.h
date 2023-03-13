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

#include <mutex>
#include <utility>

#include "common/status.h"
#include "olap/olap_common.h"
#include "olap/rowset/segment_v2/hierarchical_data_reader.h"
#include "vec/columns/subcolumn_tree.h"
#include "vec/json/path_in_data.h"

namespace doris {
struct SubcolumnReader {
    std::unique_ptr<ColumnReader> reader;
    std::shared_ptr<const vectorized::IDataType> file_column_type;
};

using RowsetSegmentId = std::pair<RowsetId, uint32_t>;

using SubstreamReaderTree = vectorized::SubcolumnsTree<StreamReader>;
using SegmentSubcolumnReaderMap = std::map<RowsetSegmentId, SubcolumnReader>;
using SubcolumnColumnReaders = vectorized::SubcolumnsTree<SegmentSubcolumnReaderMap>;

// Tree for variant subcolumns
// path -> rowsetid-segmentid-> SubcolumnReader
class GolobalColumnTree {
public:
    GolobalColumnTree() = default;
    void register_column(const vectorized::PathInData& path, const RowsetId& rowset_id,
                         uint32_t segment_id, SubcolumnReader&& reader) {
        std::lock_guard<std::mutex> lock(_lock);
        const auto* node = _readers_tree.find_exact(path);
        if (!node) {
        }
    }
    void remove_column(const vectorized::PathInData& path, const RowsetId& rowset_id,
                       uint32_t segment_id) {
        std::lock_guard<std::mutex> lock(_lock);
    }
    // return nullptr if not found
    const SubcolumnReader* get_reader(const vectorized::PathInData& path, RowsetId rowset_id,
                                      uint32_t segment_id) {
        std::lock_guard<std::mutex> lock(_lock);
        const auto* node = _readers_tree.find_exact(path);
        if (!node) {
            return nullptr;
        }
        auto it = node->data.find(RowsetSegmentId {rowset_id, segment_id});
        if (it == node->data.end()) {
            return nullptr;
        }
        return &it->second;
    }

private:
    std::mutex _lock;
    SubcolumnColumnReaders _readers_tree;
};

} // namespace doris