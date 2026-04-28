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

#include <CLucene/util/bkd/bkd_reader.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/data_type/primitive_type.h"
#include "io/fs/file_system.h"
#include "io/fs/path.h"
#include "storage/index/index_query_context.h"
#include "storage/index/index_reader.h"
#include "storage/index/inverted/inverted_index_cache.h"
#include "storage/index/inverted/inverted_index_compound_reader.h"
#include "storage/index/inverted/inverted_index_desc.h"
#include "storage/index/inverted/inverted_index_parser.h"
#include "storage/index/inverted/inverted_index_query_type.h"
#include "storage/tablet/tablet_schema.h"
#include "util/once.h"

#define FINALIZE_INPUT(x) \
    if (x != nullptr) {   \
        x->close();       \
        _CLDELETE(x);     \
    }
#define FINALLY_FINALIZE_INPUT(x) \
    try {                         \
        FINALIZE_INPUT(x)         \
    } catch (...) {               \
    }

namespace lucene {
namespace store {
class Directory;
} // namespace store
namespace util {
namespace bkd {
class bkd_docid_set_iterator;
} // namespace bkd
} // namespace util
} // namespace lucene
namespace roaring {
class Roaring;
} // namespace roaring

namespace doris {
class KeyCoder;
class TypeInfo;
struct OlapReaderStatistics;
class RuntimeState;

namespace segment_v2 {

class InvertedIndexIterator;
class InvertedIndexQueryCacheHandle;
class IndexFileReader;
class InvertedIndexQueryInfo;
class IndexIterator;
class InvertedIndexQueryParam;
class StringQueryParam;
class NumericQueryParam;

class InvertedIndexResultBitmap {
private:
    std::shared_ptr<roaring::Roaring> _data_bitmap = nullptr;
    std::shared_ptr<roaring::Roaring> _null_bitmap = nullptr;

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
                                   : nullptr) {}

    // Move constructor
    InvertedIndexResultBitmap(InvertedIndexResultBitmap&& other) noexcept
            : _data_bitmap(std::move(other._data_bitmap)),
              _null_bitmap(std::move(other._null_bitmap)) {}

    // Copy assignment operator
    InvertedIndexResultBitmap& operator=(const InvertedIndexResultBitmap& other) {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = other._data_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._data_bitmap)
                                   : nullptr;
            _null_bitmap = other._null_bitmap
                                   ? std::make_shared<roaring::Roaring>(*other._null_bitmap)
                                   : nullptr;
        }
        return *this;
    }

    // Move assignment operator
    InvertedIndexResultBitmap& operator=(InvertedIndexResultBitmap&& other) noexcept {
        if (this != &other) { // Prevent self-assignment
            _data_bitmap = std::move(other._data_bitmap);
            _null_bitmap = std::move(other._null_bitmap);
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
        return *this;
    }

    // NOT operation
    const InvertedIndexResultBitmap& op_not(const roaring::Roaring* universe) const {
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

private:
    static const roaring::Roaring& _empty_bitmap() {
        static const roaring::Roaring empty;
        return empty;
    }
};

class InvertedIndexReader : public IndexReader {
public:
    explicit InvertedIndexReader(const TabletIndex* index_meta,
                                 std::shared_ptr<IndexFileReader> index_file_reader)
            : _index_file_reader(std::move(index_file_reader)), _index_meta(*index_meta) {}
    virtual ~InvertedIndexReader() = default;

    IndexType index_type() override { return IndexType::INVERTED; }

    // Callers pass a TypedInvertedIndexQueryParam<PT> produced by
    // InvertedIndexQueryParamFactory. Each reader static_casts to the
    // appropriate intermediate (StringQueryParam / NumericQueryParam) at entry.
    virtual Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                         const InvertedIndexQueryParam* query_value,
                         InvertedIndexQueryType query_type,
                         std::shared_ptr<roaring::Roaring>& bit_map,
                         const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) = 0;
    virtual Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                             const InvertedIndexQueryParam* query_value,
                             InvertedIndexQueryType query_type, size_t* count) = 0;

    Status read_null_bitmap(const IndexQueryContextPtr& context,
                            InvertedIndexQueryCacheHandle* cache_handle,
                            lucene::store::Directory* dir = nullptr);

    virtual InvertedIndexReaderType type() = 0;

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
                            std::shared_ptr<roaring::Roaring>& bit_map);

    virtual Status handle_searcher_cache(const IndexQueryContextPtr& context,
                                         InvertedIndexCacheHandle* inverted_index_cache_handle);
    std::string get_index_file_path();
    static Status create_index_searcher(IndexSearcherBuilder* index_searcher_builder,
                                        lucene::store::Directory* dir, IndexSearcherPtr* searcher,
                                        size_t& reader_size);
    std::shared_ptr<IndexFileReader> get_index_file_reader() const { return _index_file_reader; }
    const TabletIndex& get_index_meta() const { return _index_meta; }

protected:
    Status match_index_search(const IndexQueryContextPtr& context,
                              InvertedIndexQueryType query_type,
                              const InvertedIndexQueryInfo& query_info,
                              const FulltextIndexSearcherPtr& index_searcher,
                              const std::shared_ptr<roaring::Roaring>& term_match_bitmap);

    friend class InvertedIndexIterator;
    std::shared_ptr<IndexFileReader> _index_file_reader;
    TabletIndex _index_meta;
    bool _has_null = true;
};
using InvertedIndexReaderPtr = std::shared_ptr<InvertedIndexReader>;

class FullTextIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(FullTextIndexReader);

public:
    explicit FullTextIndexReader(const TabletIndex* index_meta,
                                 const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~FullTextIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const InvertedIndexQueryParam* query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const InvertedIndexQueryParam* query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "FullTextIndexReader not support try_query");
    }

    InvertedIndexReaderType type() override;
};

class StringTypeInvertedIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(StringTypeInvertedIndexReader);

public:
    explicit StringTypeInvertedIndexReader(
            const TabletIndex* index_meta,
            const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~StringTypeInvertedIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const InvertedIndexQueryParam* query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const InvertedIndexQueryParam* query_value, InvertedIndexQueryType query_type,
                     size_t* count) override {
        return Status::Error<ErrorCode::NOT_IMPLEMENTED_ERROR>(
                "StringTypeInvertedIndexReader not support try_query");
    }
    InvertedIndexReaderType type() override;
};

template <InvertedIndexQueryType QT>
class InvertedIndexVisitor : public lucene::util::bkd::bkd_reader::intersect_visitor {
private:
    const void* _io_ctx = nullptr;
    roaring::Roaring* _hits = nullptr;
    uint32_t _num_hits;
    bool _only_count;
    lucene::util::bkd::bkd_reader* _reader = nullptr;

public:
    std::string query_min;
    std::string query_max;

public:
    InvertedIndexVisitor(const void* io_ctx, lucene::util::bkd::bkd_reader* r,
                         roaring::Roaring* hits, bool only_count = false);
    ~InvertedIndexVisitor() override = default;

    void set_reader(lucene::util::bkd::bkd_reader* r) { _reader = r; }
    lucene::util::bkd::bkd_reader* get_reader() { return _reader; }

    void visit(int row_id) override;
    void visit(roaring::Roaring& r) override;
    void visit(roaring::Roaring&& r) override;
    void visit(roaring::Roaring* doc_id, std::vector<uint8_t>& packed_value) override;
    void visit(std::vector<char>& doc_id, std::vector<uint8_t>& packed_value) override;
    int visit(int row_id, std::vector<uint8_t>& packed_value) override;
    void visit(lucene::util::bkd::bkd_docid_set_iterator* iter,
               std::vector<uint8_t>& packed_value) override;
    int matches(uint8_t* packed_value);
    lucene::util::bkd::relation compare(std::vector<uint8_t>& min_packed,
                                        std::vector<uint8_t>& max_packed) override;
    lucene::util::bkd::relation compare_prefix(std::vector<uint8_t>& prefix) override;
    uint32_t get_num_hits() const { return _num_hits; }
    const void* get_io_context() override { return _io_ctx; }
};

class BkdIndexReader : public InvertedIndexReader {
    ENABLE_FACTORY_CREATOR(BkdIndexReader);

public:
    explicit BkdIndexReader(const TabletIndex* index_meta,
                            const std::shared_ptr<IndexFileReader>& index_file_reader)
            : InvertedIndexReader(index_meta, index_file_reader) {}
    ~BkdIndexReader() override = default;

    Status new_iterator(std::unique_ptr<IndexIterator>* iterator) override;
    Status query(const IndexQueryContextPtr& context, const std::string& column_name,
                 const InvertedIndexQueryParam* query_value, InvertedIndexQueryType query_type,
                 std::shared_ptr<roaring::Roaring>& bit_map,
                 const InvertedIndexAnalyzerCtx* analyzer_ctx = nullptr) override;
    Status try_query(const IndexQueryContextPtr& context, const std::string& column_name,
                     const InvertedIndexQueryParam* query_value, InvertedIndexQueryType query_type,
                     size_t* count) override;
    Status invoke_bkd_try_query(const IndexQueryContextPtr& context,
                                const NumericQueryParam* query_value,
                                InvertedIndexQueryType query_type,
                                std::shared_ptr<lucene::util::bkd::bkd_reader> r, size_t* count);
    Status invoke_bkd_query(const IndexQueryContextPtr& context,
                            const NumericQueryParam* query_value, InvertedIndexQueryType query_type,
                            std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                            std::shared_ptr<roaring::Roaring>& bit_map);
    template <InvertedIndexQueryType QT>
    Status construct_bkd_query_value(const NumericQueryParam* query_value,
                                     std::shared_ptr<lucene::util::bkd::bkd_reader> r,
                                     InvertedIndexVisitor<QT>* visitor);

    InvertedIndexReaderType type() override;
    Status get_bkd_reader(const IndexQueryContextPtr& context, BKDIndexSearcherPtr& reader);

private:
    const TypeInfo* _type_info {};
    const KeyCoder* _value_key_coder {};
};

} // namespace segment_v2
} // namespace doris
