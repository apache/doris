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

#include <stddef.h>
#include <stdint.h>

#include <list>
#include <memory>
#include <vector>

#include "common/status.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/utils.h"
#ifdef USE_LIBCPP
#include <queue>
#else
#include <ext/pb_ds/priority_queue.hpp>
#endif

#include "olap/reader.h"
#include "olap/rowset/rowset_reader.h"
#include "vec/core/block.h"

namespace __gnu_pbds {
struct pairing_heap_tag;
} // namespace __gnu_pbds

namespace doris {

class TabletSchema;
class RuntimeProfile;

namespace vectorized {

class VCollectIterator {
public:
    // Hold reader point to get reader params
    ~VCollectIterator();

    void init(TabletReader* reader, bool ori_data_overlapping, bool force_merge, bool is_reverse);

    Status add_child(const RowSetSplits& rs_splits);

    Status build_heap(std::vector<RowsetReaderSharedPtr>& rs_readers);
    // Get top row of the heap, nullptr if reach end.
    Status current_row(IteratorRowRef* ref) const;

    // Read nest order row in Block.
    // Returns
    //      OK when read successfully.
    //      Status::Error<END_OF_FILE>("") and set *row to nullptr when EOF is reached.
    //      Others when error happens
    Status next(IteratorRowRef* ref);

    Status next(Block* block);

    bool is_merge() const { return _merge; }

    RowLocation current_row_location() { return _inner_iter->current_row_location(); }

    Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) {
        return _inner_iter->current_block_row_locations(block_row_locations);
    }

    bool update_profile(RuntimeProfile* profile) {
        if (_inner_iter != nullptr) {
            return _inner_iter->update_profile(profile);
        }
        return false;
    }

    inline bool use_topn_next() const { return _topn_limit > 0; }

private:
    // next for topn query
    Status _topn_next(Block* block);

    class BlockRowPosComparator {
    public:
        BlockRowPosComparator(MutableBlock* mutable_block,
                              const std::vector<uint32_t>* compare_columns, bool is_reverse)
                : _mutable_block(mutable_block),
                  _compare_columns(compare_columns),
                  _is_reverse(is_reverse) {}

        bool operator()(const size_t& lpos, const size_t& rpos) const;

    private:
        const MutableBlock* _mutable_block = nullptr;
        const std::vector<uint32_t>* _compare_columns;
        // reverse the compare order
        const bool _is_reverse = false;
    };

    // This interface is the actual implementation of the new version of iterator.
    // It currently contains two implementations, one is Level0Iterator,
    // which only reads data from the rowset reader, and the other is Level1Iterator,
    // which can read merged data from multiple LevelIterators through MergeHeap.
    // By using Level1Iterator, some rowset readers can be merged in advance and
    // then merged with other rowset readers.
    class LevelIterator {
    public:
        LevelIterator(TabletReader* reader)
                : _schema(reader->tablet_schema()),
                  _compare_columns(reader->_reader_context.read_orderby_key_columns) {}

        virtual Status init(bool get_data_by_ref = false) = 0;

        virtual void init_for_union(bool get_data_by_ref) {}

        virtual int64_t version() const = 0;

        virtual const IteratorRowRef* current_row_ref() const { return &_ref; }

        virtual Status next(IteratorRowRef* ref) = 0;

        virtual Status next(Block* block) = 0;

        void set_same(bool same) { _ref.is_same = same; }

        bool is_same() const { return _ref.is_same; }

        virtual ~LevelIterator() = default;

        const TabletSchema& tablet_schema() const { return _schema; }

        const inline std::vector<uint32_t>* compare_columns() const { return _compare_columns; }

        virtual RowLocation current_row_location() = 0;

        virtual Status current_block_row_locations(std::vector<RowLocation>* row_location) = 0;

        [[nodiscard]] virtual Status ensure_first_row_ref() = 0;

        virtual bool update_profile(RuntimeProfile* profile) = 0;

    protected:
        const TabletSchema& _schema;
        IteratorRowRef _ref;
        std::vector<uint32_t>* _compare_columns;
    };

    // Compare row cursors between multiple merge elements,
    // if row cursors equal, compare data version.
    class LevelIteratorComparator {
    public:
        LevelIteratorComparator(int sequence, bool is_reverse)
                : _sequence(sequence), _is_reverse(is_reverse) {}

        bool operator()(LevelIterator* lhs, LevelIterator* rhs);

    private:
        int _sequence;
        // reverse the compare order
        bool _is_reverse = false;
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
        ~Level0Iterator() override = default;

        Status init(bool get_data_by_ref = false) override;

        virtual void init_for_union(bool get_data_by_ref) override;

        /* For unique and agg, rows is aggregated in block_reader, which access
         * first row so we need prepare the first row ref while duplicated
         * key does not need it.
         *
         * Here, we organize a lot state here, e.g. data model, order, data
         * overlapping, we should split the iterators in the furure to make the
         * logic simple and much more understandable.
         */
        [[nodiscard]] Status ensure_first_row_ref() override;

        int64_t version() const override;

        Status next(IteratorRowRef* ref) override;

        Status next(Block* block) override;

        RowLocation current_row_location() override;

        Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override;

        bool update_profile(RuntimeProfile* profile) override {
            if (_rs_reader != nullptr) {
                return _rs_reader->update_profile(profile);
            }
            return false;
        }

        Status refresh_current_row();

    private:
        Status _next_by_ref(IteratorRowRef* ref);

        bool _is_empty() {
            if (_get_data_by_ref) {
                return _block_view.empty();
            } else {
                return _block->rows() == 0;
            }
        }

        bool _current_valid() {
            if (_get_data_by_ref) {
                return _current < _block_view.size();
            } else {
                return _ref.row_pos < _block->rows();
            }
        }

        void _reset() {
            if (_get_data_by_ref) {
                _block_view.clear();
                _ref.reset();
                _current = 0;
            } else {
                _ref.is_same = false;
                _ref.row_pos = 0;
                _block->clear_column_data();
            }
        }

        Status _refresh() {
            if (_get_data_by_ref) {
                return _rs_reader->next_block_view(&_block_view);
            } else {
                return _rs_reader->next_block(_block.get());
            }
        }

        RowsetReaderSharedPtr _rs_reader;
        TabletReader* _reader = nullptr;
        std::shared_ptr<Block> _block;

        int _current;
        BlockView _block_view;
        std::vector<RowLocation> _block_row_locations;
        bool _get_data_by_ref = false;
    };

    // Iterate from LevelIterators (maybe Level0Iterators or Level1Iterator or mixed)
    class Level1Iterator : public LevelIterator {
    public:
        Level1Iterator(std::list<std::unique_ptr<LevelIterator>> children, TabletReader* reader,
                       bool merge, bool is_reverse, bool skip_same);

        Status init(bool get_data_by_ref = false) override;

        int64_t version() const override;

        Status next(IteratorRowRef* ref) override;

        Status next(Block* block) override;

        RowLocation current_row_location() override { return _cur_child->current_row_location(); }

        Status current_block_row_locations(std::vector<RowLocation>* block_row_locations) override;

        [[nodiscard]] Status ensure_first_row_ref() override;

        ~Level1Iterator() override;

        bool update_profile(RuntimeProfile* profile) override {
            if (_cur_child != nullptr) {
                return _cur_child->update_profile(profile);
            }
            return false;
        }

        void init_level0_iterators_for_union();

    private:
        Status _merge_next(IteratorRowRef* ref);

        Status _normal_next(IteratorRowRef* ref);

        Status _normal_next(Block* block);

        Status _merge_next(Block* block);

        // Each LevelIterator corresponds to a rowset reader,
        // it will be cleared after '_heap' has been initialized when '_merge == true'.
        std::list<std::unique_ptr<LevelIterator>> _children;
        // point to the Level0Iterator containing the next output row.
        // null when VCollectIterator hasn't been initialized or reaches EOF.
        std::unique_ptr<LevelIterator> _cur_child;
        TabletReader* _reader = nullptr;

        // when `_merge == true`, rowset reader returns ordered rows and VCollectIterator uses a priority queue to merge
        // sort them. The output of VCollectIterator is also ordered.
        // When `_merge == false`, rowset reader returns *partial* ordered rows. VCollectIterator simply returns all rows
        // from the first rowset, the second rowset, .., the last rowset. The output of CollectorIterator is also
        // *partially* ordered.
        bool _merge = true;
        // reverse the compare order
        bool _is_reverse = false;

        bool _skip_same;
        // used when `_merge == true`
        std::unique_ptr<MergeHeap> _heap;

        std::vector<RowLocation> _block_row_locations;
    };

    std::unique_ptr<LevelIterator> _inner_iter;

    // Each LevelIterator corresponds to a rowset reader,
    // it will be cleared after '_inner_iter' has been initialized.
    std::list<std::unique_ptr<LevelIterator>> _children;

    bool _merge = true;
    // reverse the compare order
    bool _is_reverse = false;
    // for topn next
    size_t _topn_limit = 0;
    bool _topn_eof = false;
    std::vector<RowSetSplits> _rs_splits;

    // Hold reader point to access read params, such as fetch conditions.
    TabletReader* _reader = nullptr;

    bool _skip_same;
};

} // namespace vectorized
} // namespace doris
