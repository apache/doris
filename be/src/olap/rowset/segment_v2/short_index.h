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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_SHORT_INDEX_H
#define DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_SHORT_INDEX_H

#include <memory>

#include "util/slice.h"

namespace doris {

namespace segment_v2 {

class ShortKeyIndexReader {
public:
    // parse index data
    bool init(const Slice& data);

    // return the entry number of the index
    size_t count();

    // compare the short key in idx_in_block to the key
    int compare_key(int idx_in_block, const Slice& key);

    // get the ShortKeyIndex from the reader
    std::unique_ptr<ShortKeyIndex> get_short_key_index();
};

class ShortKeyIndexWriter {
public:
    bool init();

    // add a short key -> rowid entry to the index
    bool add_entry(doris::Slice* key, rowid_t rowid);

    // return the index data
    dorsi::Slice finish();
};

class ShortKeyIndex {
public:
    ShortKeyIndex(ShortKeyIndexReader* reader);

    // seek the the first entry when the short key is equal to or greater than key
    // if equal, matched will be set to true, else false
    bool seek_at_or_after(const doris::Slice& key, bool* matched);

    // seek the the first entry when the short key is equal to or less than key
    // if equal, matched will be set to true, else false
    bool seek_at_or_before(const doris::Slice& key, bool* matched);

    // return the current row id of current index entry
    rowid_t get_current_rowid();

    // Seek the index to previous one
    // If the current index is 0, return false
    bool prev();

    // Seek the index to next one
    // If the current index is tee last one, return false
    bool next();

private:
    bool _seeked;
    size_t _cur_idx;
}

} // namespace segment_v2

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_SEGMENT_V2_SHORT_INDEX_H
