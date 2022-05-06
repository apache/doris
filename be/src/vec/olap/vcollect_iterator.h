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

#ifdef USE_LIBCPP
#include <queue>
#else
#include <ext/pb_ds/priority_queue.hpp>
#endif

#include "olap/olap_define.h"
#include "olap/reader.h"
#include "olap/rowset/rowset_reader.h"
#include "vec/core/block.h"

namespace doris {

class TabletSchema;

namespace vectorized {

struct IteratorRowRef {
    std::shared_ptr<Block> block;
    int16_t row_pos;
    bool is_same;
};

class VCollectIterator {
public:
    // Hold reader point to get reader params
    ~VCollectIterator();

    void init(TabletReader* reader);

    Status add_child(RowsetReaderSharedPtr rs_reader);

    void build_heap(std::vector<RowsetReaderSharedPtr>& rs_readers);
    // Get top row of the heap, nullptr if reach end.
    Status current_row(IteratorRowRef* ref) const;

    // Read nest order row in Block.
    // Returns
    //      OLAP_SUCCESS when read successfully.
    //      Status::OLAPInternalError(OLAP_ERR_DATA_EOF) and set *row to nullptr when EOF is reached.
    //      Others when error happens
    Status next(IteratorRowRef* ref);

    Status next(Block* block);

    bool is_merge() const { return _merge; }

private:
    // This interface is the actual implementation of the new version of iterator.
    // It currently contains two implementations, one is Level0Iterator,
    // which only reads data from the rowset reader, and the other is Level1Iterator,
    // which can read merged data from multiple LevelIterators through MergeHeap.
    // By using Level1Iterator, some rowset readers can be merged in advance and
    // then merged with other rowset readers.
    class LevelIterator {
    public:
        LevelIterator(TabletReader* reader) : _schema(reader->tablet()->tablet_schema()) {};

        virtual Status init() = 0;

        virtual int64_t version() const = 0;

        const IteratorRowRef* current_row_ref() const { return &_ref; }

        virtual Status next(IteratorRowRef* ref) = 0;

        virtual Status next(Block* block) = 0;

        void set_same(bool same) { _ref.is_same = same; }

        bool is_same() { return _ref.is_same; }

        virtual ~LevelIterator() = default;

        const TabletSchema& tablet_schema() const { return _schema; };

    protected:
        const TabletSchema& _schema;
        IteratorRowRef _ref;
    };

    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class LevelIteratorComparator {
    public:
        LevelIteratorComparator(int sequence = -1) : _sequence(sequence) {}

        bool operator()(LevelIterator* lhs, LevelIterator* rhs);

    private:
        int _sequence;
    };

#ifdef USE_LIBCPP
    using MergeHeap = std::priority_queue<LevelIterator*, std::vector<LevelIterator*>,
                                          LevelIteratorComparator>;
#else
    using MergeHeap = __gnu_pbds::priority_queue<LevelIterator*, LevelIteratorComparator,
                                                 __gnu_pbds::pairing_heap_tag>;
#endif

    // Iterate from rowset reader. This Iterator usually like a leaf node
    class Level0Iterator : public LevelIterator {
    public:
        Level0Iterator(RowsetReaderSharedPtr rs_reader, TabletReader* reader);
        ~Level0Iterator() {}

        Status init() override;

        int64_t version() const override;

        Status next(IteratorRowRef* ref) override;

        Status next(Block* block) override;

    private:
        Status _refresh_current_row();

        RowsetReaderSharedPtr _rs_reader;
        TabletReader* _reader = nullptr;
        std::shared_ptr<Block> _block;
    };

    // Iterate from LevelIterators (maybe Level0Iterators or Level1Iterator or mixed)
    class Level1Iterator : public LevelIterator {
    public:
        Level1Iterator(const std::list<LevelIterator*>& children, TabletReader* reader, bool merge,
                       bool skip_same);

        Status init() override;

        int64_t version() const override;

        Status next(IteratorRowRef* ref) override;

        Status next(Block* block) override;

        ~Level1Iterator();

    private:
        Status _merge_next(IteratorRowRef* ref);

        Status _normal_next(IteratorRowRef* ref);

        Status _normal_next(Block* block);

        Status _merge_next(Block* block);

        // Each LevelIterator corresponds to a rowset reader,
        // it will be cleared after '_heap' has been initialized when '_merge == true'.
        std::list<LevelIterator*> _children;
        // point to the Level0Iterator containing the next output row.
        // null when VCollectIterator hasn't been initialized or reaches EOF.
        LevelIterator* _cur_child = nullptr;
        TabletReader* _reader = nullptr;

        // when `_merge == true`, rowset reader returns ordered rows and VCollectIterator uses a priority queue to merge
        // sort them. The output of VCollectIterator is also ordered.
        // When `_merge == false`, rowset reader returns *partial* ordered rows. VCollectIterator simply returns all rows
        // from the first rowset, the second rowset, .., the last rowset. The output of CollectorIterator is also
        // *partially* ordered.
        bool _merge = true;

        bool _skip_same;
        // used when `_merge == true`
        std::unique_ptr<MergeHeap> _heap;

        // batch size, get from TabletReader
        int _batch_size;
    };

    std::unique_ptr<LevelIterator> _inner_iter;

    // Each LevelIterator corresponds to a rowset reader,
    // it will be cleared after '_inner_iter' has been initialized.
    std::list<LevelIterator*> _children;

    bool _merge = true;
    // Hold reader point to access read params, such as fetch conditions.
    TabletReader* _reader = nullptr;

    bool _skip_same;
};

} // namespace vectorized
} // namespace doris
