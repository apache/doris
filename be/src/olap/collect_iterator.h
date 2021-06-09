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

#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"

namespace doris {

class Reader;
class RowCursor;

class CollectIterator {
public:
    ~CollectIterator();

    // Hold reader point to get reader params
    void init(Reader* reader);

    OLAPStatus add_child(RowsetReaderSharedPtr rs_reader);

    void build_heap();

    // Get top row of the heap, nullptr if reach end.
    const RowCursor* current_row(bool* delete_flag) const;

    // Read next row into *row.
    // Returns
    //      OLAP_SUCCESS when read successfully.
    //      OLAP_ERR_DATA_EOF and set *row to nullptr when EOF is reached.
    //      Others when error happens
    OLAPStatus next(const RowCursor** row, bool* delete_flag);

    // Clear the MergeSet element and reset state.
    void clear();

private:
    // This interface is the actual implementation of the new version of iterator.
    // It currently contains two implementations, one is Level0Iterator,
    // which only reads data from the rowset reader, and the other is Level1Iterator,
    // which can read merged data from multiple LevelIterators through MergeHeap.
    // By using Level1Iterator, some rowset readers can be merged in advance and 
    // then merged with other rowset readers.
    class LevelIterator {
    public:
        virtual OLAPStatus init() = 0;

        virtual const RowCursor* current_row(bool* delete_flag) const = 0;

        virtual const RowCursor* current_row() const = 0;

        virtual int64_t version() const = 0;

        virtual OLAPStatus next(const RowCursor** row, bool* delete_flag) = 0;
        virtual ~LevelIterator() = 0;
    };
    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class LevelIteratorComparator {
    public:
        LevelIteratorComparator(const bool reverse = false) : _reverse(reverse) {}
        bool operator()(const LevelIterator* a, const LevelIterator* b);

    private:
        bool _reverse;
        OlapReaderStatistics* _stats;
    };

    typedef std::priority_queue<LevelIterator*, std::vector<LevelIterator*>,
                                LevelIteratorComparator>
            MergeHeap;
    // Iterate from rowset reader. This Iterator usually like a leaf node
    class Level0Iterator : public LevelIterator {
    public:
        Level0Iterator(RowsetReaderSharedPtr rs_reader, Reader* reader);

        OLAPStatus init();

        const RowCursor* current_row(bool* delete_flag) const;

        const RowCursor* current_row() const;

        int64_t version() const;

        OLAPStatus next(const RowCursor** row, bool* delete_flag);

        ~Level0Iterator();

    private:
        OLAPStatus (Level0Iterator::*_refresh_current_row)() = nullptr;

        OLAPStatus _refresh_current_row_v1();
        OLAPStatus _refresh_current_row_v2();

        RowsetReaderSharedPtr _rs_reader;
        const RowCursor* _current_row = nullptr;
        bool _is_delete = false;
        Reader* _reader = nullptr;
        // point to rows inside `_row_block`
        RowCursor _row_cursor;
        RowBlock* _row_block = nullptr;
    };
    // Iterate from LevelIterators (maybe Level0Iterators or Level1Iterator or mixed)
    class Level1Iterator : public LevelIterator {
    public:
        Level1Iterator(const std::vector<LevelIterator*>& children, bool merge, bool reverse);

        OLAPStatus init();

        const RowCursor* current_row(bool* delete_flag) const;

        const RowCursor* current_row() const;

        int64_t version() const;

        OLAPStatus next(const RowCursor** row, bool* delete_flag);

        ~Level1Iterator();

    private:
        inline OLAPStatus _merge_next(const RowCursor** row, bool* delete_flag);
        inline OLAPStatus _normal_next(const RowCursor** row, bool* delete_flag);

        // each Level0Iterator corresponds to a rowset reader
        const std::vector<LevelIterator*> _children;
        // point to the Level0Iterator containing the next output row.
        // null when CollectIterator hasn't been initialized or reaches EOF.
        LevelIterator* _cur_child = nullptr;

        // when `_merge == true`, rowset reader returns ordered rows and CollectIterator uses a priority queue to merge
        // sort them. The output of CollectIterator is also ordered.
        // When `_merge == false`, rowset reader returns *partial* ordered rows. CollectIterator simply returns all rows
        // from the first rowset, the second rowset, .., the last rowset. The output of CollectorIterator is also
        // *partially* ordered.
        bool _merge = true;
        bool _reverse = false;
        // used when `_merge == true`
        std::unique_ptr<MergeHeap> _heap;
        // used when `_merge == false`
        int _child_idx = 0;
    };

    std::unique_ptr<LevelIterator> _inner_iter;

    // each LevelIterator corresponds to a rowset reader
    std::vector<LevelIterator*> _children;

    bool _merge = true;
    bool _reverse = false;

    // Hold reader point to access read params, such as fetch conditions.
    Reader* _reader = nullptr;
};

} // namespace doris
