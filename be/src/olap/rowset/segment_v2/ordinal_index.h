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

#include <memory>

#include "util/slice.h"
#include "gen_cpp/segment_v2.pb.h"

#ifndef DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_ORDINAL_INDEX_H
#define DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_ORDINAL_INDEX_H

namespace doris {

namespace segment_v2 {

class OrdinalIndexReader {
public:
    // parse the data
    bool init(const Slice& data);

    // return the entry number of the index
    size_t count();

    // compare the row_id in idx_in_block to the row_id
    int compare_key(int idx_in_block, const rowid_t row_id);

    // get the OrdinalIndex from the reader
    std::unique_ptr<OrdinalIndex> get_short_key_index();
};

class OrdinalIndexWriter {
public:
    bool init();

    // add a rowid -> page_pointer entry to the index
    bool add_entry(rowid_t rowid, const PagePointerPB& page_pointer);

    // return the index data
    dorsi::Slice finish();
};

class OrdinalIndex {
public:
    OrdinalIndex(OrdinalIndexReader* reader);

    // seek the the first entry when the rowid is equal to or greater than row_id
    // if equal, matched will be set to true, else false
    bool seek_at_or_after(const rowid_t row_id, bool* matched);

    // seek the the first entry when the rowid is equal to or less than row_id
    // if equal, matched will be set to true, else false
    bool seek_at_or_before(const rowid_t row_id, bool* matched);

    // return the current seeked index related page pointer
    void get_current_page_pointer(PagePointerPB* page_pointer);

private:
    bool _seeked;
    size_t _cur_idx;
};

} // namespace segment_v2

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_ORDINAL_INDEX_H
