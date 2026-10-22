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

#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "core/field.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_reader.h"
#include "storage/index/inverted/analysis/inverted_index_parser.h"
#include "storage/index/inverted/common/inverted_index_cache.h"
#include "storage/index/inverted/common/inverted_index_desc.h"
#include "storage/index/inverted/common/inverted_index_query_type.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"

namespace doris::index_query {
class IndexSource;
using IndexSourcePtr = std::shared_ptr<IndexSource>;
} // namespace doris::index_query

namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
struct OlapReaderStatistics;
class RuntimeState;
namespace index_query::logical {
struct Node;
} // namespace index_query::logical

namespace segment_v2 {

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;
class IndexFileReader;
class IndexIterator;
namespace inverted_index::query_v2 {
class Query;
} // namespace inverted_index::query_v2

class InvertedIndexResultBitmap {
private:
    std::shared_ptr<roaring::Roaring> _data_bitmap = nullptr;
    std::shared_ptr<roaring::Roaring> _null_bitmap = nullptr;
    // true means _data_bitmap is only a superset of candidates (from a gram index push-down, for
    // instance), so the caller (SegmentIterator) must keep the original expression for row-level
    // re-verification and may not consume the expression as it would for an exact index result.
    //
    // Every operation that produces a new value from an existing one has to carry the flag: the
    // four special member functions copy it, and &=, |= and -= OR it in, because a combination
    // involving one approximate operand is itself only a superset and must not be mistaken for an
    // exact result. op_not is the exception -- see the comment there; negation cannot preserve
    // the property at all, so it asserts instead of propagating.
    bool _approximate = false;

public:
    // Default constructor
    InvertedIndexResultBitmap() = default;
    ~InvertedIndexResultBitmap() = default;

    // Constructor with arguments
    InvertedIndexResultBitmap(std::shared_ptr<roaring::Roaring> data_bitmap,
                              std::shared_ptr<roaring::Roaring> null_bitmap)
            : _data_bitmap(std::move(data_bitmap)), _null_bitmap(std::move(null_bitmap)) {}

    // Copy constructor
    InvertedIndexResultBitmap(const InvertedIndexResultBitmap& other)
            : _data_bitmap(other._data_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._data_bitmap)
                                   : nullptr),
              _null_bitmap(other._null_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._null_bitmap)
                                   : nullptr),
              _approximate(other._approximate) {}

    // Move constructor
    InvertedIndexResultBitmap(InvertedIndexResultBitmap&& other) noexcept
            : _data_bitmap(std::move(other._data_bitmap)),
              _null_bitmap(std::move(other._null_bitmap)),
              _approximate(other._approximate) {}

    // Copy assignment operator
    InvertedIndexResultBitmap& operator=(const InvertedIndexResultBitmap& other) {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = other._data_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._data_bitmap)
                                   : nullptr;
            _null_bitmap = other._null_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._null_bitmap)
                                   : nullptr;
            _approximate = other._approximate;
        }
        return *this;
    }

    // Move assignment operator
    InvertedIndexResultBitmap& operator=(InvertedIndexResultBitmap&& other) noexcept {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::move(other._data_bitmap);
            _null_bitmap = std::move(other._null_bitmap);
            _approximate = other._approximate;
        }
        return *this;
    }

    // Operator &=
    InvertedIndexResultBitmap& operator&=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && other._data_bitmap) {
            const auto& my_null = _null_bitmap ? *_null_bitmap : _empty_bitmap();
            const auto& ot_null = other._null_bitmap ? *other._null_bitmap : _empty_bitmap();
            auto new_null_bitmap = (*_data_bitmap & ot_null) | (my_null & *other._data_bitmap) |
                                   (my_null & ot_null);
            *_data_bitmap &= *other._data_bitmap;
            if (!_null_bitmap) {
                _null_bitmap = std::make_shared<roaring::Roaring>();
            }
            *_null_bitmap = std::move(new_null_bitmap);
        }
        _approximate = _approximate || other._approximate;
        return *this;
    }

    // Operator |=
    InvertedIndexResultBitmap& operator|=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && other._data_bitmap) {
            const auto& my_null = _null_bitmap ? *_null_bitmap : _empty_bitmap();
            const auto& ot_null = other._null_bitmap ? *other._null_bitmap : _empty_bitmap();
            // SQL three-valued logic for OR:
            // - TRUE OR anything = TRUE (not NULL)
            // - FALSE OR NULL = NULL
            // - NULL OR NULL = NULL
            // Result is NULL when the row is NULL on either side while the other side
            // is not TRUE. Rows that become TRUE must be removed from the NULL bitmap.
            *_data_bitmap |= *other._data_bitmap;
            auto new_null_bitmap = (my_null - *other._data_bitmap) | (ot_null - *_data_bitmap);
            new_null_bitmap -= *_data_bitmap;
            if (!_null_bitmap) {
                _null_bitmap = std::make_shared<roaring::Roaring>();
            }
            *_null_bitmap = std::move(new_null_bitmap);
        }
        _approximate = _approximate || other._approximate;
        return *this;
    }

    // NOT operation
    //
    // Negation is the one combination an approximate result cannot survive: complementing a
    // superset of the matching rows yields a subset of the non-matching ones, so rows that do
    // match would be dropped, and the approximate flag cannot repair that -- re-verifying the
    // survivors never brings back a row that was already excluded. A caller holding an
    // approximate result must keep the expression and let the scalar path evaluate the negation
    // instead of asking for it here.
    const InvertedIndexResultBitmap& op_not(const roaring::Roaring* universe) const {
        DCHECK(!_approximate) << "op_not on an approximate result would drop matching rows";
        if (_data_bitmap) {
            if (_null_bitmap) {
                *_data_bitmap = *universe - *_data_bitmap - *_null_bitmap;
            } else {
                *_data_bitmap = *universe - *_data_bitmap;
            }
            // The _null_bitmap remains unchanged.
        }
        return *this;
    }

    // Operator -=
    InvertedIndexResultBitmap& operator-=(const InvertedIndexResultBitmap& other) {
        if (_data_bitmap && other._data_bitmap) {
            *_data_bitmap -= *other._data_bitmap;
            if (other._null_bitmap) {
                *_data_bitmap -= *other._null_bitmap;
            }
            if (_null_bitmap && other._null_bitmap) {
                *_null_bitmap -= *other._null_bitmap;
            }
        }
        _approximate = _approximate || other._approximate;
        return *this;
    }

    void mask_out_null() {
        if (_data_bitmap && _null_bitmap) {
            *_data_bitmap -= *_null_bitmap;
        }
    }

    const std::shared_ptr<roaring::Roaring>& get_data_bitmap() const { return _data_bitmap; }

    const std::shared_ptr<roaring::Roaring>& get_null_bitmap() const { return _null_bitmap; }

    // Check if both bitmaps are empty
    bool is_empty() const { return (_data_bitmap == nullptr && _null_bitmap == nullptr); }

    // true = superset of candidates: rows outside the bitmap certainly do not match, but rows
    // inside it may not all match, so the expression must re-verify them.
    void set_approximate(bool v) { _approximate = v; }
    bool approximate() const { return _approximate; }

private:
    static const roaring::Roaring& _empty_bitmap() {
        static const roaring::Roaring empty;
        return empty;
    }
};

// Owns an open index until the query releases its source.
struct OpenedIndex {
    virtual ~OpenedIndex() = default;

    // Whether the segment was written as a gram index, whatever its policy names now.
    virtual bool written_as_gram_index() const { return false; }
};

class InvertedIndexReader : public IndexReader {
public:
    // `rows_of_segment` and `column_is_array` describe the segment and the column rather than the
    // index image: the count-only fast path fabricates row ids, so it needs a bound a corrupt
    // image cannot move and one fact about how the column was written. A reader created without
    // them (0, false) never takes that path.
    explicit InvertedIndexReader(const TabletIndex* index_meta,
                                 std::shared_ptr<IndexFileReader> index_file_reader,
                                 uint64_t rows_of_segment = 0, bool column_is_array = false)
            : _index_file_reader(std::move(index_file_reader)),
              _index_meta(*index_meta),
              _rows_of_segment(rows_of_segment),
              _column_is_array(column_is_array) {}
    virtual ~InvertedIndexReader() = default;

#ifdef BE_TEST
    using SingleFlightFollowerJoinedObserver = void (*)(void*) noexcept;
    using SingleFlightLeaderBeforeComputeObserver = void (*)(void*) noexcept;
    using SearcherOpenObserver = void (*)(void*) noexcept;

    void set_single_flight_follower_joined_observer_for_test(
            SingleFlightFollowerJoinedObserver observer, void* opaque) {
        _single_flight_follower_joined_observer = observer;
        _single_flight_follower_joined_opaque = opaque;
    }
    void set_single_flight_leader_before_compute_observer_for_test(
            SingleFlightLeaderBeforeComputeObserver observer, void* opaque) {
        _single_flight_leader_before_compute_observer = observer;
        _single_flight_leader_before_compute_opaque = opaque;
    }
    void set_searcher_open_observer_for_test(SearcherOpenObserver observer, void* opaque) {
        _searcher_open_observer = observer;
        _searcher_open_opaque = opaque;
    }
#endif

    IndexType index_type() override { return IndexType::INVERTED; }

    virtual Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                         const Field& query_value, InvertedIndexQueryType query_type,
                         std::shared_ptr<roaring::Roaring>& bit_map,
                         const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) = 0;
    virtual Status query_with_null_bitmap(const IndexQueryContextPtr& context,
                                          const std::string& column_name, const Field& query_value,
                                          InvertedIndexQueryType query_type,
                                          std::shared_ptr<roaring::Roaring>& bit_map,
                                          InvertedIndexQueryCacheHandle* null_bitmap_cache_handle,
                                          const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr);
    virtual Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                             const Field& query_value, InvertedIndexQueryType query_type,
                             size_t* count) = 0;

    // Runs a leaf SEARCH lowered, whose terms are taken as they are. Readers that only accept raw
    // values keep the default.
    virtual Status query_leaf(
            const IndexQueryContextPtr& /*context*/, const std::string& /*column_name*/,
            const index_query::logical::Node& /*leaf*/,
            std::shared_ptr<roaring::Roaring>& /*bit_map*/,
            InvertedIndexQueryCacheHandle* /*null_bitmap_cache_handle*/ = nullptr) {
        return Status::NotSupported("this index reader does not run lowered leaves");
    }

    // Opens the index for a query and binds `field` as the engine's source; `opened` keeps
    // the index alive for as long as the source is read. Readers the engine does not read
    // keep the default.
    virtual Status open_source(const IndexQueryContextPtr& /*context*/,
                               const std::wstring& /*field*/,
                               std::unique_ptr<OpenedIndex>* /*opened*/,
                               index_query::IndexSourcePtr* /*source*/) {
        return Status::NotSupported("this index reader has no source for the query engine");
    }

    virtual Status read_null_bitmap(const IndexQueryContextPtr& /*context*/,
                                    InvertedIndexQueryCacheHandle* /*cache_handle*/) {
        return Status::NotSupported("this index reader has no null bitmap");
    }

    virtual InvertedIndexReaderType type() = 0;

    // Whether the index names an analyzer that cuts sparse or dense grams under the current
    // policy -- the index LIKE / REGEXP gram queries are meant for. Decided from the index
    // properties and in-memory policies only, never by opening the index.
    virtual bool is_gram_family() const { return false; }

    [[nodiscard]] uint64_t get_index_id() const override { return _index_meta.index_id(); }

    [[nodiscard]] MOCK_FUNCTION const std::map<std::string, std::string>& get_index_properties()
            const {
        return _index_meta.properties();
    }

    [[nodiscard]] bool has_null() const { return _has_null; }
    void set_has_null(bool has_null) { _has_null = has_null; }

    bool handle_query_cache(const IndexQueryContextPtr& context, InvertedIndexQueryCache* cache,
                            const InvertedIndexQueryCache::CacheKey& cache_key,
                            InvertedIndexQueryCacheHandle* cache_handler,
                            std::shared_ptr<roaring::Roaring>& bit_map, bool enabled = true);
    void insert_query_cache(const IndexQueryContextPtr& context, InvertedIndexQueryCache* cache,
                            const InvertedIndexQueryCache::CacheKey& cache_key,
                            std::shared_ptr<roaring::Roaring> bit_map,
                            InvertedIndexQueryCacheHandle* cache_handler, bool enabled = true);

    std::string get_index_file_path();
    std::shared_ptr<IndexFileReader> get_index_file_reader() const { return _index_file_reader; }
    const TabletIndex& get_index_meta() const { return _index_meta; }

protected:
    friend class InvertedIndexIterator;
    std::shared_ptr<IndexFileReader> _index_file_reader;
    TabletIndex _index_meta;
    bool _has_null = true;
    uint64_t _rows_of_segment = 0;
    bool _column_is_array = false;
#ifdef BE_TEST
    SingleFlightFollowerJoinedObserver _single_flight_follower_joined_observer = nullptr;
    void* _single_flight_follower_joined_opaque = nullptr;
    SingleFlightLeaderBeforeComputeObserver _single_flight_leader_before_compute_observer = nullptr;
    void* _single_flight_leader_before_compute_opaque = nullptr;
    SearcherOpenObserver _searcher_open_observer = nullptr;
    void* _searcher_open_opaque = nullptr;
#endif
};
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;
} // namespace segment_v2
} // namespace doris
