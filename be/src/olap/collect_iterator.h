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

#include <queue>

#include "olap/olap_define.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_reader.h"
#include "util/tuple_row_zorder_compare.h"

namespace doris {

class TabletReader;
class RowCursor;

class CollectIterator {
public:
    ~CollectIterator();

    // Hold reader point to get reader params
    void init(TabletReader* reader);

    Status add_child(RowsetReaderSharedPtr rs_reader);

    void build_heap(const std::vector<RowsetReaderSharedPtr>& rs_readers);

    // Get top row of the heap, nullptr if reach end.
    const RowCursor* current_row(bool* delete_flag) const;

    // Read next row into *row.
    // Returns
    //      Status::OLAPInternalError(OLAP_ERR_DATA_EOF) and set *row to nullptr when EOF is reached.
    //      Others when error happens
    Status next(const RowCursor** row, bool* delete_flag);

private:
    // This interface is the actual implementation of the new version of iterator.
    // It currently contains two implementations, one is Level0Iterator,
    // which only reads data from the rowset reader, and the other is Level1Iterator,
    // which can read merged data from multiple LevelIterators through MergeHeap.
    // By using Level1Iterator, some rowset readers can be merged in advance and
    // then merged with other rowset readers.
    class LevelIterator {
    public:
        virtual Status init() = 0;

        virtual const RowCursor* current_row(bool* delete_flag) const = 0;

        virtual const RowCursor* current_row() const = 0;

        virtual int64_t version() const = 0;

        virtual Status next(const RowCursor** row, bool* delete_flag) = 0;
        virtual ~LevelIterator() = 0;

        bool need_skip() const { return _skip_row; }

        void set_need_skip(bool skip) const { _skip_row = skip; }

        // Only use in unique reader. Heap will set _skip_row = true.
        // when build heap find the row in LevelIterator have same key but lower version or sequence
        // the row of LevelIteratro should be skipped to prevent useless compare and function call
        mutable bool _skip_row = false;
    };

    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class LevelIteratorComparator {
    public:
        LevelIteratorComparator(const bool reverse = false, int sequence_id_idx = -1)
                : _reverse(reverse), _sequence_id_idx(sequence_id_idx) {}
        virtual bool operator()(const LevelIterator* a, const LevelIterator* b);
        virtual ~LevelIteratorComparator() {}

    private:
        bool _reverse;
        int _sequence_id_idx;
    };

    class LevelZorderIteratorComparator : public LevelIteratorComparator {
    public:
        LevelZorderIteratorComparator(const bool reverse = false, int sequence_id_idx = -1,
                                      const size_t sort_col_num = 0)
                : _reverse(reverse) {
            _comparator = TupleRowZOrderComparator(sort_col_num);
        }
        virtual bool operator()(const LevelIterator* a, const LevelIterator* b);
        virtual ~LevelZorderIteratorComparator() = default;

    private:
        bool _reverse = false;
        TupleRowZOrderComparator _comparator;
    };

    class BaseComparator {
    public:
        BaseComparator(std::shared_ptr<LevelIteratorComparator>& cmp);
        bool operator()(const LevelIterator* a, const LevelIterator* b);

    private:
        std::shared_ptr<LevelIteratorComparator> _cmp;
    };

    typedef std::priority_queue<LevelIterator*, std::vector<LevelIterator*>, BaseComparator>
            MergeHeap;
    // Iterate from rowset reader. This Iterator usually like a leaf node
    class Level0Iterator : public LevelIterator {
    public:
        Level0Iterator(RowsetReaderSharedPtr rs_reader, TabletReader* reader);

        Status init() override;

        const RowCursor* current_row(bool* delete_flag) const override;

        const RowCursor* current_row() const override;

        int64_t version() const override;

        Status next(const RowCursor** row, bool* delete_flag) override;

        ~Level0Iterator();

    private:
        Status (Level0Iterator::*_refresh_current_row)() = nullptr;

        Status _refresh_current_row_v2();

        RowsetReaderSharedPtr _rs_reader;
        const RowCursor* _current_row = nullptr; // It points to the returned row
        bool _is_delete = false;
        TabletReader* _reader = nullptr;
        RowCursor _row_cursor; // It points to rows inside `_row_block`, maybe not returned
        RowBlock* _row_block = nullptr;
    };

    // Iterate from LevelIterators (maybe Level0Iterators or Level1Iterator or mixed)
    class Level1Iterator : public LevelIterator {
    public:
        Level1Iterator(const std::list<LevelIterator*>& children, bool merge, bool reverse,
                       int sequence_id_idx, uint64_t* merge_count, SortType sort_type,
                       int sort_col_num);

        Status init() override;

        const RowCursor* current_row(bool* delete_flag) const override;

        const RowCursor* current_row() const override;

        int64_t version() const override;

        Status next(const RowCursor** row, bool* delete_flag) override;

        ~Level1Iterator();

    private:
        Status _merge_next(const RowCursor** row, bool* delete_flag);
        Status _normal_next(const RowCursor** row, bool* delete_flag);

        // Each LevelIterator corresponds to a rowset reader,
        // it will be cleared after '_heap' has been initialized when '_merge == true'.
        std::list<LevelIterator*> _children;
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
        // need to be cleared when deconstructing this Level1Iterator
        // The child LevelIterator should be either in _heap or in _children
        std::unique_ptr<MergeHeap> _heap;
        // used when `_merge == false`
        int _sequence_id_idx = -1;

        uint64_t* _merged_rows = nullptr;
        SortType _sort_type;
        int _sort_col_num;
    };

    std::unique_ptr<LevelIterator> _inner_iter;

    // Each LevelIterator corresponds to a rowset reader,
    // it will be cleared after '_inner_iter' has been initialized.
    std::list<LevelIterator*> _children;

    bool _merge = true;
    bool _reverse = false;

    // Hold reader point to access read params, such as fetch conditions.
    TabletReader* _reader = nullptr;
};

} // namespace doris
