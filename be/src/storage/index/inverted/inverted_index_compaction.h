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

#include <CLucene.h>

#include <cstdint>
#include <string_view>
#include <vector>

#include "common/status.h"
#include "storage/index/inverted/inverted_index_common.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"

namespace doris {
class TabletIndex;
namespace segment_v2 {
class IndexFileWriter;
class IndexFileReader;

// `index_meta` and `field_name` are required to emit the optional token-exists Bloom Filter
// ("tbf") sub-file into each dest dir when `config::enable_inverted_index_term_bf` is on and
// the index is analyzed (fulltext). They MUST identify the same logical (column, index) the
// CLucene `indexCompaction` is merging -- `field_name` is the same identifier the writer
// stored in the term dictionary (column name for storage format V1, `to_string(unique_id)`
// for V2). Without them, segments produced by this compaction shortcut would not carry a
// tbf and the read-side fast path would always fall back to a normal lookup for them.
Status compact_column(
        int64_t index_id,
        std::vector<std::unique_ptr<DorisCompoundReader, DirectoryDeleter>>& src_index_dirs,
        std::vector<lucene::store::Directory*>& dest_index_dirs, std::string_view tmp_path,
        const std::vector<std::vector<std::pair<uint32_t, uint32_t>>>& trans_vec,
        const std::vector<uint32_t>& dest_segment_num_rows,
        const TabletIndex* index_meta = nullptr, std::string_view field_name = {});
} // namespace segment_v2
} // namespace doris
